//! Reading single values by row ordinal: only the row groups and pages that hold them.
//!
//! The file's footer and offset index (from the [`FooterCache`]) say where each row group and
//! page starts, so a [`RowSelection`] over the wanted ordinals makes the reader fetch and decode
//! just those pages of the one projected column.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::arrow::array::{Array, AsArray, RecordBatch};
use deltalake_core::arrow::datatypes::DataType;
use deltalake_core::parquet::arrow::arrow_reader::{RowSelection, RowSelector};
#[allow(deprecated)] // see `scan`
use deltalake_core::parquet::arrow::async_reader::ParquetObjectReader;
use deltalake_core::parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use deltalake_core::parquet::arrow::ProjectionMask;
use deltalake_core::ObjectStore;
use futures::TryStreamExt;

use super::footer_cache::FooterCache;
use super::scan::ScanFile;
use crate::error::{Error, Result};

/// The binary values of `column` at row ordinals `rows` of `file`, in the order of `rows`
/// (which may repeat). A null reads as empty.
pub(crate) async fn take(
    store: Arc<dyn ObjectStore>,
    footers: &FooterCache,
    file: &ScanFile,
    rows: &[u64],
    column: &str,
) -> Result<Vec<Bytes>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let meta = footers.get(store.clone(), &file.path, file.size).await?;
    let mut wanted: Vec<u64> = rows.to_vec();
    wanted.sort_unstable();
    wanted.dedup();

    // The row groups holding wanted rows, and the selection within them.
    let groups = meta.metadata().row_groups();
    let mut picked = Vec::new();
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut start = 0u64;
    let mut i = 0;
    for (g, rg) in groups.iter().enumerate() {
        let n = rg.num_rows() as u64;
        let end = start + n;
        let mut pos = start;
        let mut any = false;
        while i < wanted.len() && wanted[i] < end {
            let r = wanted[i];
            if r > pos {
                selectors.push(RowSelector::skip((r - pos) as usize));
            }
            selectors.push(RowSelector::select(1));
            pos = r + 1;
            i += 1;
            any = true;
        }
        if any {
            if end > pos {
                selectors.push(RowSelector::skip((end - pos) as usize));
            }
            picked.push(g);
        }
        start = end;
    }
    if i != wanted.len() {
        return Err(Error::Corruption(format!(
            "row {} is beyond the {start} rows of {}",
            wanted[i], file.path
        )));
    }

    #[allow(deprecated)]
    let reader = ParquetObjectReader::new(store, file.path.clone()).with_file_size(file.size);
    let b = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, meta);
    let schema = b.parquet_schema();
    let idx: Vec<usize> = (0..schema.num_columns())
        .filter(|i| schema.column(*i).name() == column)
        .collect();
    if idx.len() != 1 {
        return Err(Error::Corruption(format!(
            "{} has no column {column}",
            file.path
        )));
    }
    let mask = ProjectionMask::leaves(schema, idx);
    let batches: Vec<RecordBatch> = b
        .with_projection(mask)
        .with_row_groups(picked)
        .with_row_selection(RowSelection::from(selectors))
        .with_batch_size(wanted.len().max(1))
        .build()?
        .try_collect()
        .await?;
    let mut got: Vec<Bytes> = Vec::with_capacity(wanted.len());
    for b in &batches {
        let c = b.column(0);
        match c.data_type() {
            DataType::Binary => {
                let a = c.as_binary::<i32>();
                got.extend((0..a.len()).map(|j| Bytes::copy_from_slice(a.value(j))));
            }
            DataType::LargeBinary => {
                let a = c.as_binary::<i64>();
                got.extend((0..a.len()).map(|j| Bytes::copy_from_slice(a.value(j))));
            }
            DataType::BinaryView => {
                let a = c.as_binary_view();
                got.extend((0..a.len()).map(|j| Bytes::copy_from_slice(a.value(j))));
            }
            other => {
                return Err(Error::Corruption(format!(
                    "column {column} of {} is {other}, not binary",
                    file.path
                )))
            }
        }
    }
    if got.len() != wanted.len() {
        return Err(Error::Corruption(format!(
            "read {} rows of {} for {} row ordinals",
            got.len(),
            file.path,
            wanted.len()
        )));
    }
    let at: HashMap<u64, Bytes> = wanted.into_iter().zip(got).collect();
    Ok(rows.iter().map(|r| at[r].clone()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake_core::arrow::array::{BinaryArray, Int64Array};
    use deltalake_core::arrow::datatypes::{Field, Schema};
    use deltalake_core::logstore::object_store::ObjectStoreExt;
    use deltalake_core::parquet::arrow::ArrowWriter;
    use deltalake_core::Path;

    #[test]
    fn takes_rows_across_row_groups_in_any_order() {
        crate::rt::block_on(async {
            let schema = Arc::new(Schema::new(vec![
                Field::new("seq", DataType::Int64, false),
                Field::new("value", DataType::Binary, true),
            ]));
            let n = 3000i64;
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..n)),
                    Arc::new(BinaryArray::from_iter_values(
                        (0..n).map(|i| format!("v{i}").into_bytes()),
                    )),
                ],
            )
            .unwrap();
            let props = deltalake_core::parquet::file::properties::WriterProperties::builder()
                .set_max_row_group_row_count(Some(1000))
                .set_statistics_enabled(
                    deltalake_core::parquet::file::properties::EnabledStatistics::Page,
                )
                .set_data_page_row_count_limit(64)
                .set_write_batch_size(64)
                .build();
            let mut buf = Vec::new();
            let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            let store: Arc<dyn ObjectStore> =
                Arc::new(deltalake_core::logstore::object_store::memory::InMemory::new());
            let size = buf.len() as u64;
            let path = Path::from("f.parquet");
            store.put(&path, buf.into()).await.unwrap();
            let file = ScanFile { ord: 0, path, size };
            let cache = FooterCache::default();
            let rows = [2999, 0, 1500, 1500, 999, 1000, 7];
            let got = take(store.clone(), &cache, &file, &rows, "value")
                .await
                .unwrap();
            let want: Vec<Bytes> = rows.iter().map(|r| Bytes::from(format!("v{r}"))).collect();
            assert_eq!(got, want);
            let e = take(store, &cache, &file, &[3000], "value")
                .await
                .unwrap_err();
            assert!(matches!(e, Error::Corruption(_)), "{e:?}");
        })
    }
}
