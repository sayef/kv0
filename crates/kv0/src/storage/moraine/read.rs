//! Reading chosen data files of a moraine table directly.
//!
//! moraine's scans cover a whole version. kv0 also reads single files: the files a rewrite
//! replaces (the statistics fold, the key-value compaction) and the files a reader has not seen
//! yet. moraine's data files are plain Parquet that never change once written, so kv0 reads
//! them itself, by the path the snapshot names.

use std::sync::Arc;

use arrow_59::array::{new_null_array, ArrayRef, RecordBatch};
use arrow_59::compute::filter_record_batch;
use arrow_59::datatypes::{Schema, SchemaRef};
use futures::{StreamExt, TryStreamExt};
#[allow(deprecated)] // see `files::scan`
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

use super::Repo;
use crate::error::{Error, Result};
use crate::storage::files::{filter, scan::parallelism};
use crate::storage::pred::Pred;

/// Rows per batch read.
const BATCH_ROWS: usize = 8192;

/// The location of data file `path` of moraine table `table`.
pub(crate) fn data_path(repo: &Repo, table: &str, path: &str) -> object_store::path::Path {
    repo.path(&format!("{table}/data/{path}"))
}

/// The rows of `files` (in this order) matching `pred`, with the columns of `schema` named in
/// `columns` (all of them when `None`). A column a file lacks (added to the table after the file
/// was written) reads as nulls.
pub(crate) async fn files(
    repo: &Repo,
    table: &str,
    schema: &SchemaRef,
    files: Vec<moraine::SnapshotFile>,
    pred: Option<&Pred>,
    columns: Option<&[&str]>,
) -> Result<Vec<RecordBatch>> {
    let out: SchemaRef = match columns {
        Some(c) => Arc::new(Schema::new(
            c.iter()
                .map(|n| {
                    schema
                        .field_with_name(n)
                        .cloned()
                        .map_err(|_| Error::InvalidArgument(format!("no column {n} in {table}")))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        None => schema.clone(),
    };
    let reads = files.into_iter().map(|f| {
        let (out, pred) = (out.clone(), pred.cloned());
        let store = repo.store().clone();
        let location = data_path(repo, table, &f.path);
        async move {
            #[allow(deprecated)]
            let reader = ParquetObjectReader::new(store, location).with_file_size(f.size);
            let mut b = ParquetRecordBatchStreamBuilder::new(reader)
                .await?
                .with_batch_size(BATCH_ROWS);
            // The output columns and those the filter reads, by top-level name.
            let mut want: Vec<String> = out.fields().iter().map(|f| f.name().clone()).collect();
            if let Some(p) = &pred {
                filter::columns(p, &mut want);
            }
            let descr = b.metadata().file_metadata().schema_descr_ptr();
            let idx: Vec<usize> = descr
                .root_schema()
                .get_fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| want.iter().any(|w| w == f.name()))
                .map(|(i, _)| i)
                .collect();
            b = b.with_projection(ProjectionMask::roots(&descr, idx));
            let batches: Vec<RecordBatch> = b.build()?.try_collect().await?;
            let mut kept = Vec::with_capacity(batches.len());
            for batch in batches {
                let batch = match &pred {
                    Some(p) => filter_record_batch(&batch, &filter::eval(p, &batch)?)?,
                    None => batch,
                };
                if batch.num_rows() > 0 {
                    kept.push(project(&batch, &out)?);
                }
            }
            Ok::<_, Error>(kept)
        }
    });
    let mut all = Vec::new();
    let mut s = futures::stream::iter(reads).buffered(parallelism());
    while let Some(batches) = s.next().await {
        all.extend(batches?);
    }
    Ok(all)
}

/// `b`'s columns as `out` names and types them: one the batch lacks is null.
fn project(b: &RecordBatch, out: &SchemaRef) -> Result<RecordBatch> {
    let cols: Vec<ArrayRef> = out
        .fields()
        .iter()
        .map(|f| match b.column_by_name(f.name()) {
            Some(c) if c.data_type() == f.data_type() => Ok(c.clone()),
            Some(c) => Ok(arrow_59::compute::cast(c, f.data_type())?),
            None => Ok(new_null_array(f.data_type(), b.num_rows())),
        })
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(out.clone(), cols)?)
}
