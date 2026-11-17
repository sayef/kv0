//! Writing data files and committing them.
//!
//! - Appends commit `Add` actions under a `Write` operation marked as a blind append, so
//!   concurrent rewrites never conflict with them.
//! - Rewrites commit `Remove` and `Add` with `data_change = false` under `Optimize`: another
//!   writer's append then never sees a delete it would have to conflict with.
//! - A writer's high-water mark rides along as a `txn` action, so data and mark land together.
//!
//! delta-rs retries a commit that lost a race for its version number (without backoff), up to
//! `MAX_RETRIES` times; the default of 15 ran out with 8 concurrent writers.

use deltalake_core::arrow::array::RecordBatch;
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties};
use deltalake_core::kernel::{Action, Add, Transaction};
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::{DeltaTable, Path};

use std::collections::HashSet;

use deltalake_core::logstore::object_store::ObjectStoreExt;

use crate::error::{Error, Result};

const MAX_RETRIES: usize = 64;

/// The `txn` application id of a writer's WAL marks.
pub(crate) fn wal_app_id(writer: &str) -> String {
    format!("kv0-wal/{writer}")
}

/// A writer's mark: its highest committed sequence number, as a `txn` action.
pub(crate) fn mark(writer: &str, seq: i64) -> Transaction {
    Transaction::new_with_last_update(
        wal_app_id(writer),
        seq,
        Some(chrono::Utc::now().timestamp_millis()),
    )
}

/// Commit properties of every kv0 commit: retries, checkpoints, no delta-rs log cleanup (kv0
/// deletes old log files itself), and `kv0` application metadata naming the operation.
fn properties(op: &str, blind_append: bool, txn: Option<Transaction>) -> CommitProperties {
    let mut meta = vec![("kv0".to_string(), serde_json::json!({ "op": op }))];
    if blind_append {
        meta.push(("isBlindAppend".to_string(), serde_json::Value::Bool(true)));
    }
    let p = CommitProperties::default()
        .with_max_retries(MAX_RETRIES)
        .with_create_checkpoint(true)
        .with_cleanup_expired_logs(Some(false))
        .with_metadata(meta);
    match txn {
        Some(t) => p.with_application_transaction(t),
        None => p,
    }
}

/// Commits `actions` on top of `table`'s version and returns the table at the new version.
///
/// delta-rs checks a commit against every commit since its read version, so it fails when a
/// cleanup or purge deleted those commit files meanwhile. A blind append (`rebase`) does not
/// depend on what it read: it is committed again on the latest version. Any other commit
/// fails with [`Error::Conflict`], which makes rewrites plan again on the latest version.
async fn commit(
    table: &DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
    props: CommitProperties,
    rebase: bool,
) -> Result<DeltaTable> {
    // The commit file is written when the prepared commit completes; what follows (loading the
    // new version, writing a checkpoint every 100 commits) can fail although the commit
    // landed. Reporting that as a failed commit would make callers write it again, so the new
    // version is loaded here instead.
    let mut base = table.clone();
    let mut attempt = 0;
    let mut below = 0;
    let post = loop {
        let prepared = CommitBuilder::from(props.clone())
            .with_actions(actions.clone())
            .build(Some(base.snapshot()?), base.log_store(), operation.clone())
            .into_prepared_commit_future()
            .await;
        let e = match prepared {
            Ok(p) => match p.await {
                Ok(post) => {
                    // Checked before anything reads or checkpoints the new version.
                    if !landed_below_checkpoint(&base, post.version, &actions).await {
                        break post;
                    }
                    let version = post.version;
                    drop(post);
                    remove_commit_file(&base, version).await;
                    if !rebase || below >= 4 {
                        return Err(Error::Conflict(format!(
                            "the commit file of version {version} landed below a newer checkpoint (a cleanup removed the commit it raced)"
                        )));
                    }
                    below += 1;
                    tracing::debug!(version, "kv0: a commit landed below a newer checkpoint; committing on the latest version");
                    base = super::table::load_at(table.log_store(), None).await?;
                    continue;
                }
                Err(e) => e,
            },
            Err(e) => e,
        };
        if !super::table::log_cut(&e) {
            return Err(e.into());
        }
        if !rebase || attempt >= 4 {
            return Err(Error::Conflict(format!(
                "commits after version {} were cleaned up meanwhile: {e}",
                base.version().unwrap_or(0)
            )));
        }
        attempt += 1;
        tracing::debug!(error = %e, attempt, "kv0: the log moved past a cleanup; committing on the latest version");
        base = super::table::load_at(table.log_store(), None).await?;
    };
    let version = post.version;
    let mut t = base;
    match post.await {
        Ok(fin) => t.state = Some(fin.snapshot()),
        Err(e) => {
            tracing::debug!(version, error = %e, "kv0: after a commit landed, loading it or checkpointing failed");
        }
    }
    if t.version() != Some(version) {
        let mut attempt = 0;
        loop {
            match t.update_incremental(Some(version)).await {
                Ok(()) => break,
                Err(e) if attempt < 8 => {
                    attempt += 1;
                    tracing::debug!(version, attempt, error = %e, "kv0: loading a committed version failed; again");
                    tokio::time::sleep(crate::storage::backoff(attempt, 50, 2_000)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(t)
}

/// Whether the commit file just written for `version` lies below a newer checkpoint, so no
/// reader will ever see it.
///
/// delta-rs lists the log, then writes `latest + 1` if absent. If meanwhile two other commits
/// land and a cleanup deletes the first of them (keeping a checkpoint at the second), the write
/// recreates that commit file beneath the checkpoint: it succeeds and is invisible. Only then
/// is there a checkpoint above `version` right after the commit, so one listing settles the
/// common case; with such a checkpoint, the latest version is loaded and searched for the
/// commit: its writer mark, or its files (live or removed since). When the check itself fails,
/// the commit counts as visible, as it did before the check existed.
async fn landed_below_checkpoint(table: &DeltaTable, version: u64, actions: &[Action]) -> bool {
    match below_checkpoint(table, version, actions).await {
        Ok(below) => below,
        Err(e) => {
            tracing::debug!(version, error = %e, "kv0: could not check a commit against newer checkpoints");
            false
        }
    }
}

async fn below_checkpoint(table: &DeltaTable, version: u64, actions: &[Action]) -> Result<bool> {
    if !super::table::checkpoint_above(table.object_store().as_ref(), version).await? {
        return Ok(false);
    }
    let latest = super::table::load_at(table.log_store(), None).await?;
    let state = latest.snapshot()?;
    let log_store = latest.log_store();
    for a in actions {
        if let Action::Txn(t) = a {
            let seen = state
                .transaction_version(log_store.as_ref(), t.app_id.clone())
                .await?;
            return Ok(seen.is_none_or(|v| v < t.version));
        }
    }
    let live: HashSet<String> = state
        .log_data()
        .iter()
        .map(|f| f.object_store_path().to_string())
        .collect();
    let adds: Vec<String> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Add(a) => Some(decoded(&a.path)),
            _ => None,
        })
        .collect();
    if adds.is_empty() {
        // A rewrite that only removes: it landed when its files are gone.
        return Ok(actions.iter().any(|a| match a {
            Action::Remove(r) => live.contains(&decoded(&r.path)),
            _ => false,
        }));
    }
    if adds.iter().any(|p| live.contains(p)) {
        return Ok(false);
    }
    // Its files may have been rewritten since; then their removal is a tombstone.
    let mut removed = state.all_tombstones(log_store.as_ref());
    while let Some(t) = futures::TryStreamExt::try_next(&mut removed).await? {
        if adds.contains(&decoded(&t.path())) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Deletes the commit file of `version`, which no reader sees (see
/// [`landed_below_checkpoint`]), so listings of the log do not show it.
async fn remove_commit_file(table: &DeltaTable, version: u64) {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    if let Err(e) = table.object_store().delete(&path).await {
        tracing::debug!(version, error = %e, "kv0: an unreachable commit file stays until log cleanup");
    }
}

/// Commits new files as a blind append, with the writer's mark when there is one.
pub(crate) async fn append(
    table: &DeltaTable,
    adds: Vec<Add>,
    txn: Option<Transaction>,
) -> Result<DeltaTable> {
    commit(
        table,
        adds.into_iter().map(Action::Add).collect(),
        DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        },
        properties("flush", true, txn),
        true,
    )
    .await
}

/// Commits writer marks alone (`txn` actions, no data): a migration carries over the marks of
/// the writers of the source.
#[cfg(feature = "lance-migrate")]
pub(crate) async fn marks(table: &DeltaTable, txns: Vec<Transaction>) -> Result<DeltaTable> {
    let mut p = properties("marks", true, None);
    for t in txns {
        p = p.with_application_transaction(t);
    }
    commit(
        table,
        Vec::new(),
        DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        },
        p,
        true,
    )
    .await
}

/// Commits a rewrite: removes `removed` (files of `table`'s version) and adds `adds`, all with
/// `data_change = false`.
pub(crate) async fn rewrite(
    table: &DeltaTable,
    removed: &[String],
    adds: Vec<Add>,
    op: &str,
) -> Result<DeltaTable> {
    let snap = table.snapshot()?;
    let mut actions: Vec<Action> = snap
        .log_data()
        .iter()
        .filter(|f| removed.contains(&f.object_store_path().to_string()))
        .map(|f| Action::Remove(f.remove_action(false)))
        .collect();
    if actions.len() != removed.len() {
        discard(table, &adds).await;
        return Err(Error::Corruption(format!(
            "{} of the {} files to replace are not in version {}",
            removed.len() - actions.len(),
            removed.len(),
            snap.version()
        )));
    }
    let added: Vec<Add> = adds.clone();
    actions.extend(adds.into_iter().map(|mut a| {
        a.data_change = false;
        Action::Add(a)
    }));
    let r = commit(
        table,
        actions,
        DeltaOperation::Optimize {
            predicate: None,
            target_size: 0,
        },
        properties(op, false, None),
        false,
    )
    .await;
    let Err(Error::Conflict(m)) = r else {
        return r;
    };
    // A conflict can be this very commit, written once already by a request whose response
    // was lost: then the latest version holds its files. Otherwise nothing references the
    // files written for it, and they go now rather than after the orphan grace period (a
    // purge in between would miss the data they hold).
    let latest = super::table::load_at(table.log_store(), None).await?;
    let live: HashSet<String> = latest
        .snapshot()?
        .log_data()
        .iter()
        .map(|f| f.object_store_path().to_string())
        .collect();
    if added.iter().any(|a| live.contains(&decoded(&a.path))) {
        tracing::debug!(error = %m, "kv0: the rewrite was committed after all");
        return Ok(latest);
    }
    discard(table, &added).await;
    Err(Error::Conflict(m))
}

/// The object-store path of an `Add` path (which is URL-encoded).
fn decoded(path: &str) -> String {
    Path::from_url_path(path)
        .map(|p| p.to_string())
        .unwrap_or_else(|_| path.to_string())
}

/// Deletes data files written for a commit that is known not to have landed, so no data
/// outlives a purge in them. Failures are logged: cleanup takes the files after the orphan
/// grace period.
pub(crate) async fn discard(table: &DeltaTable, adds: &[Add]) {
    let store = table.object_store();
    for a in adds {
        let path = Path::from(decoded(&a.path));
        if let Err(e) = store.delete(&path).await {
            tracing::debug!(path = %path, error = %e, "kv0: an uncommitted file stays until cleanup");
        }
    }
}

/// A file written and not yet committed.
pub(crate) struct Written {
    pub add: Add,
    /// The file's location, as the table's file list names it.
    pub path: String,
}

/// Writes record batches into data files of at most `target_rows` rows (one file when `None`),
/// in order: row `i` of the rows written to a file is that file's row ordinal `i`.
pub(crate) struct FileWriter {
    w: RecordBatchWriter,
    target_rows: Option<u64>,
    rows: u64,
    done: Vec<Written>,
}

impl FileWriter {
    pub(crate) fn new(
        table: &DeltaTable,
        props: WriterProperties,
        target_rows: Option<u64>,
    ) -> Result<Self> {
        Ok(FileWriter {
            w: RecordBatchWriter::for_table(table)?.with_writer_properties(props),
            target_rows,
            rows: 0,
            done: Vec::new(),
        })
    }

    pub(crate) async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.rows += batch.num_rows() as u64;
        self.w.write(batch).await?;
        if self.target_rows.is_some_and(|t| self.rows >= t) {
            self.seal().await?;
        }
        Ok(())
    }

    /// Finishes the file being written, if it holds any rows.
    pub(crate) async fn seal(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let mut adds = self.w.flush().await?;
        if adds.len() != 1 {
            return Err(Error::Storage(
                format!(
                    "expected one data file per flush, the writer wrote {}",
                    adds.len()
                )
                .into(),
            ));
        }
        let add = adds.pop().expect("one");
        let path = Path::from_url_path(&add.path)
            .map_err(|e| Error::Corruption(format!("data file path {}: {e}", add.path)))?
            .to_string();
        self.done.push(Written { add, path });
        self.rows = 0;
        Ok(())
    }

    pub(crate) async fn finish(mut self) -> Result<Vec<Written>> {
        self.seal().await?;
        Ok(self.done)
    }
}
