//! Versioned tables of JSON rows for per-item data: evaluation datasets, per-run outputs and
//! scores, labels, application state.
//!
//! Each logical table is its own moraine table (`records/<name>`, see `storage::moraine`), so a
//! table's versions and tags cover that table only: tagging a table's version keeps that table,
//! not every other table's rows at the time. A row is `(table_name, run_id?, item_id, data,
//! seq, created_at)`, keyed by `(run_id, item_id)`: an append with a key already present
//! supersedes the older row, and reads return the newest row per key. `optimize` folds
//! superseded rows away. Files are sorted by `(run_id, item_id)` and pruned by their statistics,
//! so a lookup by either reads few pages. Nothing is kept in memory; a commit returns the
//! table's version, which a run can record and read back exactly later (by a tag that survives
//! cleanup; moraine keeps only the latest and tagged versions readable).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::{StreamExt, TryStreamExt};
use serde_json::Value;

use crate::error::{Error, Result};
use crate::storage::oplog::FileId;
use crate::storage::pred::Pred;
use crate::storage::table::{Root, Scan, TableRef};
use crate::storage::types::{logical_schema, seq_from_i64, seq_to_i64};
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

/// [`RECORDS`] with a signed `seq`, as the tracking layer reads and writes it.
pub(crate) static RECORDS_LOGICAL: LazyLock<SchemaRef> = LazyLock::new(|| logical_schema(&RECORDS));

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

/// The directory name of a records table (`records/<name>`, which `storage::moraine` maps to a
/// moraine table name): letters, digits, `-` and `_` (and `.` after the first byte) as they
/// are, every other byte as `=XX`.
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
    unescape(name, b'=')
}

pub(crate) const DIR: &str = "records";

pub(crate) async fn open(root: &Root, table: &str, create: bool) -> Result<TableRef> {
    if table.is_empty() {
        return Err(Error::InvalidArgument(
            "a records table needs a name".into(),
        ));
    }
    root.table(&format!("{DIR}/{}", encode(table)), RECORDS.clone(), create)
        .await
}

/// Names of the tables stored under `root`.
pub(crate) async fn tables(root: &Root) -> Result<Vec<String>> {
    Ok(root
        .tables(DIR)
        .await?
        .iter()
        .filter_map(|n| decode(n))
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
        RECORDS_LOGICAL.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                seqs.iter().map(|s| seq_to_i64(*s)),
            )),
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

pub(crate) async fn append(ds: &mut TableRef, b: RecordBatch) -> Result<u64> {
    ds.append(vec![b], None).await?;
    Ok(ds.version())
}

fn arrays(b: &RecordBatch) -> Result<Cols<'_>> {
    let col = |n: &str| {
        b.column_by_name(n)
            .ok_or_else(|| Error::Corruption(format!("records column {n} is missing")))
    };
    let seq = col("seq")?.as_any().downcast_ref::<Int64Array>();
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
    &'a Int64Array,
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
pub(crate) async fn read(ds: &TableRef, q: &RecordQuery) -> Result<Vec<Record>> {
    let mut filter = Vec::new();
    if let Some(r) = &q.run_id {
        filter.push(Pred::eq("run_id", r));
    }
    if let Some(i) = &q.item_id {
        filter.push(Pred::eq("item_id", i));
    }
    // One scan, or one per batch of run ids; keys include the run id, so batches never share
    // a key and newest-wins per key holds across them.
    let filters: Vec<Vec<Pred>> = match &q.run_ids {
        None => vec![filter],
        Some(ids) => ids
            .chunks(RUN_IDS_PER_QUERY)
            .map(|c| {
                let mut f = filter.clone();
                f.push(Pred::is_in("run_id", c));
                f
            })
            .collect(),
    };
    let mut batches: Vec<RecordBatch> = Vec::new();
    for f in filters {
        let scan = Scan {
            filter: (!f.is_empty()).then_some(Pred::And(f)),
            ..Scan::all()
        };
        batches.extend(ds.scan(scan).await?);
    }
    let mut newest: HashMap<(Option<String>, String), Record> = HashMap::new();
    for b in &batches {
        let (seq, tbl, run, item, data, at) = arrays(b)?;
        for i in 0..b.num_rows() {
            let k = key(run, item, i);
            if newest
                .get(&k)
                .is_some_and(|r| r.seq >= seq_from_i64(seq.value(i)))
            {
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
                    seq: seq_from_i64(seq.value(i)),
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
pub(crate) async fn fold(ds: &mut TableRef) -> Result<usize> {
    // The newest sequence number per key.
    let mut newest: HashMap<(Option<String>, String), i64> = HashMap::new();
    let mut rows = 0usize;
    {
        let mut st = ds
            .scan_stream(Scan::all().columns(&["seq", "run_id", "item_id"]))
            .await?;
        while let Some(b) = st.try_next().await? {
            let seq = b
                .column_by_name("seq")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>());
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
                let e = newest.entry(key(run, item, i)).or_insert(i64::MIN);
                *e = (*e).max(seq.value(i));
            }
        }
    }
    let dropped = rows - newest.len();
    if dropped == 0 {
        return Ok(0);
    }
    let files: Vec<FileId> = ds.files().into_iter().map(|f| f.id).collect();
    let newest = Arc::new(newest);
    let stream = ds
        .scan_stream(Scan::all().files(files.clone()))
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
        .boxed();
    ds.replace(files, RECORDS_LOGICAL.clone(), stream, None)
        .await?;
    Ok(dropped)
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
