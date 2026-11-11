//! Versioned tables of JSON rows for per-item data: evaluation datasets, per-run outputs and
//! scores, labels, application state.
//!
//! Each logical table is its own Lance dataset, `records/<name>.lance`, so a table's versions
//! and tags cover that table only: tagging a dataset's version keeps that dataset, not every
//! other table's rows at the time. A row is `(table_name, run_id?, item_id, data, seq,
//! created_at)`, keyed by `(run_id, item_id)`: an append with a key already present supersedes
//! the older row, and reads return the newest row per key. `optimize` folds superseded rows
//! away and keeps B-tree indices on `item_id` and `run_id`, so a lookup by either reads only
//! the matching rows. Nothing is kept in memory; a commit returns the table's version, which a
//! run can record and read back exactly later (by version, or by a tag that survives cleanup).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::TryStreamExt;
use lance::dataset::{Dataset, InsertBuilder, WriteMode};
use lance::index::DatasetIndexExt;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::IndexType;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::storage::{self, StorageOptions};
pub static RECORDS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("run_id", DataType::Utf8, true),
        Field::new("item_id", DataType::Utf8, false),
        // JSON.
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]))
});

/// A row to append.
#[derive(Clone, Debug, PartialEq)]
pub struct NewRecord {
    pub item_id: String,
    pub run_id: Option<String>,
    pub data: Value,
}

/// A stored row.
#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    pub table: String,
    pub item_id: String,
    pub run_id: Option<String>,
    pub data: Value,
    /// Order of appends across the repository; higher is newer.
    pub seq: u64,
    /// Microseconds since the Unix epoch (UTC).
    pub created_at: i64,
}

/// Which version of the records to read.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum At {
    /// The latest committed version.
    #[default]
    Latest,
    Version(u64),
    /// A tag made with `Repo::tag_records`.
    Tag(String),
}

/// A read of one logical table: the newest row of each `(run_id, item_id)` matching every
/// filter given, in append order.
#[derive(Clone, Debug, Default)]
pub struct RecordQuery {
    pub run_id: Option<String>,
    /// Rows of any of these runs (`run_id IN (...)`, using the run id index); combined with
    /// `run_id` and `item_id` when those are set too. An empty list matches nothing.
    pub run_ids: Option<Vec<String>>,
    pub item_id: Option<String>,
    pub at: At,
}

/// Run ids per `IN` list, so a long list does not make one huge filter.
const RUN_IDS_PER_QUERY: usize = 500;

pub(crate) fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The directory name of a table's dataset: letters, digits, `-` and `_` (and `.` after the
/// first byte) as they are, every other byte as `=XX`. Only characters object stores never
/// percent-encode are used: an earlier `~XX` scheme made Lance address the dataset's tag refs
/// at a percent-encoded path (`%7E`) next to the data, so cleanup did not see those tags.
pub(crate) fn encode(table: &str) -> String {
    let mut out = String::new();
    for b in table.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || (b == b'.' && !out.is_empty()) {
            out.push(b as char);
        } else {
            out.push_str(&format!("={b:02X}"));
        }
    }
    out
}

fn unescape(name: &str, esc: u8) -> Option<String> {
    let mut bytes = Vec::new();
    let mut it = name.bytes();
    while let Some(b) = it.next() {
        if b == esc {
            let h = [it.next()?, it.next()?];
            bytes.push(u8::from_str_radix(std::str::from_utf8(&h).ok()?, 16).ok()?);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).ok()
}

pub(crate) fn decode(name: &str) -> Option<String> {
    if name.contains('~') || name.contains('%') {
        return None; // written by the earlier scheme; see `migrate_paths`
    }
    unescape(name, b'=')
}

/// Moves records tables written with the earlier `~XX` directory names to the current ones,
/// together with any tags Lance filed under the percent-encoded twin (`%7E`) of such a name.
/// Local repositories are migrated in place; on an object store the stray names are reported.
/// Returns the tables moved.
pub(crate) async fn migrate_paths(root: &str, opts: &StorageOptions) -> Result<Vec<String>> {
    let dir = storage::join(root, DIR);
    let names = storage::list_dir(&dir, opts).await?;
    let old: Vec<&String> = names
        .iter()
        .filter(|n| n.contains('~') || n.contains("%7E") || n.contains("%7e"))
        .collect();
    if old.is_empty() {
        return Ok(Vec::new());
    }
    if !storage::is_local(&dir) {
        return Err(Error::InvalidArgument(format!(
            "records tables under {dir} use directory names of an earlier build ({}); copy each \
             <name>.lance to the name encode() gives now (with its _refs from the %7E twin) and \
             remove the old ones",
            old.iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    let base = std::path::PathBuf::from(dir.strip_prefix("file://").unwrap_or(&dir));
    // Another process may be migrating the same repository at the same time. A step that fails
    // while the other process completes it is judged by its outcome, not by the error: operating
    // systems report these races differently (macOS answers a rename racing a removal with
    // EINVAL, Linux with ENOENT or ENOTEMPTY).
    let moved_to = |r: std::io::Result<()>, from: &std::path::Path, to: &std::path::Path| match r {
        Ok(()) => Ok(()),
        Err(_) if !from.exists() && to.exists() => Ok(()),
        Err(e) => Err(Error::Io(e.to_string())),
    };
    let gone = |r: std::io::Result<()>, path: &std::path::Path| match r {
        Ok(()) => Ok(()),
        Err(_) if !path.exists() => Ok(()),
        Err(e) => Err(Error::Io(e.to_string())),
    };
    let created = |r: std::io::Result<()>, path: &std::path::Path| match r {
        Ok(()) => Ok(()),
        Err(_) if path.is_dir() => Ok(()),
        Err(e) => Err(Error::Io(e.to_string())),
    };
    let mut moved = Vec::new();
    // Data first: `x~3Ay.lance` becomes `x=3Ay.lance`.
    for n in old.iter().filter(|n| n.contains('~')) {
        let Some(table) = n.strip_suffix(".lance").and_then(|s| unescape(s, b'~')) else {
            continue;
        };
        let to = base.join(format!("{}.lance", encode(&table)));
        if !to.exists() {
            let from = base.join(n);
            moved_to(std::fs::rename(&from, &to), &from, &to)?;
        }
        moved.push(table);
    }
    // Then tags Lance filed under `x%7E3Ay.lance/_refs`: into the table's own `_refs`.
    for n in old.iter().filter(|n| n.contains('%')) {
        let tilde = n.replace("%7E", "~").replace("%7e", "~");
        let Some(table) = tilde.strip_suffix(".lance").and_then(|s| unescape(s, b'~')) else {
            continue;
        };
        let from = base.join(n).join("_refs").join("tags");
        let to = base
            .join(format!("{}.lance", encode(&table)))
            .join("_refs")
            .join("tags");
        if let Ok(rd) = std::fs::read_dir(&from) {
            created(std::fs::create_dir_all(&to), &to)?;
            for e in rd.flatten() {
                let dst = to.join(e.file_name());
                if !dst.exists() {
                    moved_to(std::fs::rename(e.path(), &dst), &e.path(), &dst)?;
                }
            }
        }
        let stray = base.join(n);
        gone(std::fs::remove_dir_all(&stray), &stray)?;
    }
    Ok(moved)
}

pub(crate) const DIR: &str = "records";

/// The dataset earlier builds kept every table in.
pub(crate) const LEGACY: &str = "records.lance";

/// Every row of the legacy dataset (of `table`, or all), in append order.
pub(crate) async fn read_legacy(
    root: &str,
    opts: &StorageOptions,
    table: Option<&str>,
) -> Result<Vec<Record>> {
    let ds = match storage::open(&storage::join(root, LEGACY), opts, None).await {
        Ok(ds) => ds,
        Err(Error::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut s = ds.scan();
    if let Some(t) = table {
        s.filter(&format!("table_name = {}", quote(t)))?;
    }
    let batches: Vec<RecordBatch> = s.try_into_stream().await?.try_collect().await?;
    let mut out = Vec::new();
    for b in &batches {
        let (seq, tbl, run, item, data, at) = arrays(b)?;
        for i in 0..b.num_rows() {
            out.push(Record {
                table: tbl.value(i).to_string(),
                item_id: item.value(i).to_string(),
                run_id: (!run.is_null(i)).then(|| run.value(i).to_string()),
                data: serde_json::from_str(data.value(i))
                    .map_err(|e| Error::Corruption(e.to_string()))?,
                seq: seq.value(i),
                created_at: at.value(i),
            });
        }
    }
    out.sort_by_key(|r| r.seq);
    Ok(out)
}

/// Deletes the legacy dataset. Returns whether there was one.
pub(crate) async fn drop_legacy(root: &str, opts: &StorageOptions) -> Result<bool> {
    let uri = storage::join(root, LEGACY);
    match storage::open(&uri, opts, None).await {
        Ok(_) => {}
        Err(Error::NotFound(_)) => return Ok(false),
        Err(e) => return Err(e),
    }
    storage::remove_dir(&uri, opts).await?;
    Ok(true)
}

pub(crate) async fn open(
    root: &str,
    table: &str,
    opts: &StorageOptions,
    create: bool,
    retention: Option<std::time::Duration>,
) -> Result<Dataset> {
    if table.is_empty() {
        return Err(Error::InvalidArgument(
            "a records table needs a name".into(),
        ));
    }
    storage::open_or_create(
        &storage::join(root, &format!("{DIR}/{}.lance", encode(table))),
        opts,
        RECORDS.clone(),
        create,
        retention,
    )
    .await
}

/// Names of the tables stored under `root`.
pub(crate) async fn tables(root: &str, opts: &StorageOptions) -> Result<Vec<String>> {
    let names = storage::list_dir(&storage::join(root, DIR), opts).await?;
    Ok(names
        .iter()
        .filter_map(|n| n.strip_suffix(".lance").and_then(decode))
        .collect())
}

pub(crate) fn batch(
    table: &str,
    rows: &[NewRecord],
    seqs: &[u64],
    now: i64,
) -> Result<RecordBatch> {
    let n = rows.len();
    Ok(RecordBatch::try_new(
        RECORDS.clone(),
        vec![
            Arc::new(UInt64Array::from_iter_values(seqs.iter().copied())),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(table, n))),
            Arc::new(
                rows.iter()
                    .map(|r| r.run_id.as_deref())
                    .collect::<StringArray>(),
            ),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| &r.item_id),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.data.to_string()),
            )),
            Arc::new(TimestampMicrosecondArray::from(vec![now; n]).with_timezone("UTC")),
        ],
    )?)
}

pub(crate) async fn append(ds: &mut Dataset, b: RecordBatch, opts: &StorageOptions) -> Result<u64> {
    let params = storage::write_params(opts, WriteMode::Append);
    let new = InsertBuilder::new(Arc::new(ds.clone()))
        .with_params(&params)
        .execute(vec![b])
        .await?;
    *ds = new;
    Ok(ds.version().version)
}

fn arrays(b: &RecordBatch) -> Result<Cols<'_>> {
    let col = |n: &str| {
        b.column_by_name(n)
            .ok_or_else(|| Error::Corruption(format!("records column {n} is missing")))
    };
    let seq = col("seq")?.as_any().downcast_ref::<UInt64Array>();
    let tbl = col("table_name")?.as_any().downcast_ref::<StringArray>();
    let run = col("run_id")?.as_any().downcast_ref::<StringArray>();
    let item = col("item_id")?.as_any().downcast_ref::<StringArray>();
    let data = col("data")?.as_any().downcast_ref::<StringArray>();
    let at = col("created_at")?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>();
    match (seq, tbl, run, item, data, at) {
        (Some(seq), Some(tbl), Some(run), Some(item), Some(data), Some(at)) => {
            Ok((seq, tbl, run, item, data, at))
        }
        _ => Err(Error::Corruption("records columns are mistyped".into())),
    }
}

type Cols<'a> = (
    &'a UInt64Array,
    &'a StringArray,
    &'a StringArray,
    &'a StringArray,
    &'a StringArray,
    &'a TimestampMicrosecondArray,
);

fn key(run: &StringArray, item: &StringArray, i: usize) -> (Option<String>, String) {
    (
        (!run.is_null(i)).then(|| run.value(i).to_string()),
        item.value(i).to_string(),
    )
}

/// Reads a table from `ds` (already at the version wanted): the newest row per key.
pub(crate) async fn read(ds: &Dataset, q: &RecordQuery) -> Result<Vec<Record>> {
    let mut filter = Vec::new();
    if let Some(r) = &q.run_id {
        filter.push(format!("run_id = {}", quote(r)));
    }
    if let Some(i) = &q.item_id {
        filter.push(format!("item_id = {}", quote(i)));
    }
    // One scan, or one per batch of run ids; keys include the run id, so batches never share
    // a key and newest-wins per key holds across them.
    let filters: Vec<Vec<String>> = match &q.run_ids {
        None => vec![filter],
        Some(ids) => ids
            .chunks(RUN_IDS_PER_QUERY)
            .map(|c| {
                let mut f = filter.clone();
                let list = c.iter().map(|r| quote(r)).collect::<Vec<_>>().join(", ");
                f.push(format!("run_id IN ({list})"));
                f
            })
            .collect(),
    };
    let mut batches: Vec<RecordBatch> = Vec::new();
    for f in filters {
        let mut s = ds.scan();
        if !f.is_empty() {
            s.filter(&f.join(" AND "))?;
        }
        batches.extend(s.try_into_stream().await?.try_collect::<Vec<_>>().await?);
    }
    let mut newest: HashMap<(Option<String>, String), Record> = HashMap::new();
    for b in &batches {
        let (seq, tbl, run, item, data, at) = arrays(b)?;
        for i in 0..b.num_rows() {
            let k = key(run, item, i);
            if newest.get(&k).is_some_and(|r| r.seq >= seq.value(i)) {
                continue;
            }
            newest.insert(
                k,
                Record {
                    table: tbl.value(i).to_string(),
                    item_id: item.value(i).to_string(),
                    run_id: (!run.is_null(i)).then(|| run.value(i).to_string()),
                    data: serde_json::from_str(data.value(i))
                        .map_err(|e| Error::Corruption(e.to_string()))?,
                    seq: seq.value(i),
                    created_at: at.value(i),
                },
            );
        }
    }
    let mut out: Vec<Record> = newest.into_values().collect();
    out.sort_by_key(|r| r.seq);
    Ok(out)
}

/// Rewrites `ds` without superseded rows, in one commit that keeps appends committed
/// meanwhile. Returns the rows dropped (0 without a commit when there are none).
pub(crate) async fn fold(ds: &mut Dataset, opts: &StorageOptions) -> Result<usize> {
    use lance::dataset::transaction::{Operation, TransactionBuilder, UpdateMode};
    use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;

    // The newest sequence number per key.
    let mut newest: HashMap<(Option<String>, String), u64> = HashMap::new();
    let mut rows = 0usize;
    {
        let mut s = ds.scan();
        s.project(&["seq", "run_id", "item_id"])?;
        let mut st = s.try_into_stream().await?;
        while let Some(b) = st.try_next().await? {
            let seq = b
                .column_by_name("seq")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
            let run = b
                .column_by_name("run_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let item = b
                .column_by_name("item_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let (Some(seq), Some(run), Some(item)) = (seq, run, item) else {
                return Err(Error::Corruption("records columns are mistyped".into()));
            };
            for i in 0..b.num_rows() {
                rows += 1;
                let e = newest.entry(key(run, item, i)).or_insert(0);
                *e = (*e).max(seq.value(i));
            }
        }
    }
    let dropped = rows - newest.len();
    if dropped == 0 {
        return Ok(0);
    }
    let removed: Vec<u64> = ds.fragments().iter().map(|f| f.id).collect();
    let newest = Arc::new(newest);
    let mut s = ds.scan();
    s.with_fragments(ds.fragments().to_vec());
    let stream = s
        .try_into_stream()
        .await?
        .map_ok(move |b| {
            let keep: arrow_array::BooleanArray = match arrays(&b) {
                Ok((seq, _, run, item, _, _)) => (0..b.num_rows())
                    .map(|i| Some(newest.get(&key(run, item, i)) == Some(&seq.value(i))))
                    .collect(),
                Err(_) => return b,
            };
            arrow_select::filter::filter_record_batch(&b, &keep).unwrap_or(b)
        })
        .map_err(|e| lance::deps::datafusion::error::DataFusionError::External(Box::new(e)));
    let source: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(RECORDS.clone(), stream));
    let params = storage::write_params(opts, WriteMode::Append);
    let base = Arc::new(ds.clone());
    let txn = InsertBuilder::new(base.clone())
        .with_params(&params)
        .execute_uncommitted_stream(source)
        .await?;
    let new_fragments = match txn.operation {
        Operation::Append { fragments } => fragments,
        other => {
            return Err(Error::Storage(
                format!("unexpected uncommitted operation {other:?}").into(),
            ))
        }
    };
    let op = Operation::Update {
        removed_fragment_ids: removed,
        updated_fragments: Vec::new(),
        new_fragments,
        fields_modified: Vec::new(),
        compacted_sstables: Vec::new(),
        fields_for_preserving_frag_bitmap: Vec::new(),
        update_mode: Some(UpdateMode::RewriteRows),
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    };
    let txn = TransactionBuilder::new(base.version().version, op).build();
    *ds = lance::dataset::CommitBuilder::new(base)
        .execute(txn)
        .await?;
    Ok(dropped)
}

/// Builds the B-tree indices on `item_id` and `run_id`, or adds new fragments to them.
pub(crate) async fn index(ds: &mut Dataset) -> Result<()> {
    if ds.count_rows(None).await? == 0 {
        return Ok(());
    }
    let have: Vec<String> = ds
        .load_indices()
        .await?
        .iter()
        .map(|i| i.name.clone())
        .collect();
    let mut missing = false;
    for c in ["item_id", "run_id"] {
        let name = format!("{c}_idx");
        if !have.contains(&name) {
            missing = true;
            ds.create_index(
                &[c],
                IndexType::BTree,
                Some(name),
                &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                true,
            )
            .await?;
        }
    }
    if !missing {
        ds.optimize_indices(&lance_index::optimize::OptimizeOptions::default())
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn table_names_round_trip_through_directory_names() {
        for t in [
            "evals",
            "dataset:qa v1",
            ".hidden",
            "ünï/c0de",
            "a~b",
            "x=3A",
            "50%",
        ] {
            let e = super::encode(t);
            assert!(
                e.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.=".contains(&b)),
                "{e}"
            );
            assert_eq!(super::decode(&e).as_deref(), Some(t));
        }
    }
}
