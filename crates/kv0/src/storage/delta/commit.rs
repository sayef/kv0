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
async fn commit(
    table: &DeltaTable,
    actions: Vec<Action>,
    operation: DeltaOperation,
    props: CommitProperties,
) -> Result<DeltaTable> {
    let fin = CommitBuilder::from(props)
        .with_actions(actions)
        .build(Some(table.snapshot()?), table.log_store(), operation)
        .await?;
    let mut t = table.clone();
    t.state = Some(fin.snapshot());
    if t.version() != Some(fin.version()) {
        t.update_incremental(Some(fin.version())).await?;
    }
    Ok(t)
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
        return Err(Error::Corruption(format!(
            "{} of the {} files to replace are not in version {}",
            removed.len() - actions.len(),
            removed.len(),
            snap.version()
        )));
    }
    actions.extend(adds.into_iter().map(|mut a| {
        a.data_change = false;
        Action::Add(a)
    }));
    commit(
        table,
        actions,
        DeltaOperation::Optimize {
            predicate: None,
            target_size: 0,
        },
        properties(op, false, None),
    )
    .await
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
