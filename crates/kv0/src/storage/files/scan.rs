//! Reading data files without a query engine: one Parquet reader per file, projected by column
//! name, files read in parallel.

use std::sync::Arc;

use arrow_59::array::RecordBatch;
// Parquet 59 deprecates its object-store reader in favour of implementing `AsyncFileReader`
// directly; it is the reader delta-rs 1.1 itself uses, so kv0 keeps it until both move on.
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
#[allow(deprecated)]
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

use super::footer_cache::FooterCache;
use crate::error::{Error, Result};

/// Rows per batch read.
const BATCH_ROWS: usize = 8192;

/// A data file to read: its row-address ordinal, location and size.
#[derive(Clone, Debug)]
pub(crate) struct ScanFile {
    pub ord: u32,
    pub path: Path,
    pub size: u64,
}

/// The batches of one file, projected to `columns` (every column when `None`), each with the
/// ordinal of its first row in the file.
pub(crate) fn file_stream(
    store: Arc<dyn ObjectStore>,
    footers: Arc<FooterCache>,
    file: ScanFile,
    columns: Option<Vec<String>>,
) -> BoxStream<'static, Result<(u64, RecordBatch)>> {
    let open = async move {
        let meta = footers.get(store.clone(), &file.path, file.size).await?;
        #[allow(deprecated)]
        let reader = ParquetObjectReader::new(store, file.path.clone()).with_file_size(file.size);
        let mut b = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta)
            .with_batch_size(BATCH_ROWS);
        if let Some(cols) = &columns {
            let schema = b.parquet_schema();
            let idx: Vec<usize> = (0..schema.num_columns())
                .filter(|i| cols.iter().any(|c| c == schema.column(*i).name()))
                .collect();
            let mask = ProjectionMask::leaves(schema, idx);
            b = b.with_projection(mask);
        }
        let stream = b.build()?.map_err(Error::from);
        let rows = stream.scan(0u64, |next, b| {
            let out = b.map(|b| {
                let first = *next;
                *next += b.num_rows() as u64;
                (first, b)
            });
            futures::future::ready(Some(out))
        });
        Ok::<_, Error>(rows)
    };
    futures::stream::once(open).try_flatten().boxed()
}

/// Calls `f` with every batch of `files` (file ordinal, first row ordinal, batch), reading up to
/// `parallel` files at once. Batches of different files arrive interleaved; batches of one
/// file arrive in order.
pub(crate) async fn scan(
    store: Arc<dyn ObjectStore>,
    footers: Arc<FooterCache>,
    files: Vec<ScanFile>,
    columns: Option<Vec<String>>,
    parallel: usize,
    mut f: impl FnMut(u32, u64, &RecordBatch) -> Result<()>,
) -> Result<()> {
    let streams = files.into_iter().map(|file| {
        let ord = file.ord;
        file_stream(store.clone(), footers.clone(), file, columns.clone())
            .map_ok(move |(first, b)| (ord, first, b))
    });
    let mut all = futures::stream::iter(streams).flatten_unordered(parallel.max(1));
    while let Some(item) = all.next().await {
        let (ord, first, b) = item?;
        f(ord, first, &b)?;
    }
    Ok(())
}

/// How many files to read at once.
pub(crate) fn parallelism() -> usize {
    std::thread::available_parallelism().map_or(4, |n| n.get().min(8))
}
