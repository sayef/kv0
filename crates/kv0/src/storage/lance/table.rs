//! Tracking and records tables as Lance datasets.
//!
//! A file is a fragment, named by its fragment id. Datasets store kv0's integer columns
//! unsigned; batches and filters are converted to and from the logical signed types at this
//! boundary (see [`crate::storage::types`]).
//!
//! A file is a fragment, named by its fragment id. Removing rows writes Lance deletion files;
//! [`Table::compact_removed`] rewrites the fragments that have them. File tags are kept in the
//! dataset metadata under `<key>_fragments`, as ranges of fragment ids (`a-b,c-d`).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::scanner::Scanner;
use lance::dataset::transaction::{Operation, TransactionBuilder, UpdateMode};
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, NewColumnTransform, WriteMode};
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance::deps::datafusion::physical_plan::SendableRecordBatchStream;
use lance::index::DatasetIndexExt;
use lance::table::format::Fragment;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::IndexType;

use crate::error::{Error, Result};
use crate::storage::oplog::{FileId, WriterMark};
use crate::storage::pred::Pred;
use crate::storage::table::{BatchStream, FileTag, Scan, Table, TableFile, TableRef, Tagged};
use crate::storage::types;
use crate::storage::StorageOptions;

/// Opens the table at `uri` (see [`crate::storage::table::Root::table`]).
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    schema: SchemaRef,
    create: bool,
    retention: Option<std::time::Duration>,
) -> Result<TableRef> {
    let ds = super::open_or_create(uri, options, schema, create, retention).await?;
    Ok(Box::new(LanceTable {
        ds,
        options: options.clone(),
    }))
}

/// A Lance dataset handle checked out at some version.
pub(crate) struct LanceTable {
    ds: Dataset,
    options: StorageOptions,
}

impl LanceTable {
    fn at(&self, ds: Dataset) -> TableRef {
        Box::new(LanceTable {
            ds,
            options: self.options.clone(),
        })
    }

    /// The dataset's schema, as stored.
    fn stored(&self) -> arrow_schema::Schema {
        arrow_schema::Schema::from(self.ds.schema())
    }

    fn sql(&self, pred: &Pred) -> String {
        types::stored_pred(pred, &self.stored()).to_sql()
    }

    fn fragments(&self, ids: &[FileId]) -> Vec<Fragment> {
        let ids: HashSet<&FileId> = ids.iter().collect();
        self.ds
            .fragments()
            .iter()
            .filter(|f| ids.contains(&file_id(f.id)))
            .cloned()
            .collect()
    }

    fn scanner(&self, scan: &Scan) -> Result<Scanner> {
        let mut s = self.ds.scan();
        if let Some(files) = &scan.files {
            s.with_fragments(self.fragments(files));
        }
        if let Some(cols) = &scan.columns {
            s.project(cols)?;
        }
        if let Some(f) = &scan.filter {
            s.filter(&self.sql(f))?;
        }
        if scan.bounded {
            // Lance may otherwise buffer up to 2 GiB of I/O ahead, which a process keeps
            // resident long after the scan.
            s.io_buffer_size(SCAN_IO_BUFFER);
            s.fragment_readahead(2);
            s.batch_readahead(2);
        }
        Ok(s)
    }

    /// Commits the fragments `source` writes in place of `removed`. Returns how many it wrote.
    async fn replace_fragments(
        &mut self,
        removed: Vec<u64>,
        schema: SchemaRef,
        source: BatchStream,
    ) -> Result<usize> {
        let stored = self.stored();
        let schema = types::to_stored(&RecordBatch::new_empty(schema), &stored)?.schema();
        let stream = source
            .and_then(move |b| futures::future::ready(types::to_stored(&b, &stored)))
            .map_err(|e| DataFusionError::External(Box::new(e)));
        let source: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        let params = super::write_params(&self.options, WriteMode::Append);
        let base = Arc::new(self.ds.clone());
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
        let added = new_fragments.len();
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
        self.ds = CommitBuilder::new(base).execute(txn).await?;
        Ok(added)
    }
}

/// Bytes of I/O a bounded scan buffers ahead of decoding.
const SCAN_IO_BUFFER: u64 = 64 << 20;

fn file_id(fragment: u64) -> FileId {
    FileId(fragment.to_string())
}

fn fragment_id(id: &FileId) -> Result<u64> {
    id.0.parse()
        .map_err(|_| Error::InvalidArgument(format!("not a Lance fragment id: {}", id.0)))
}

fn tag_metadata_key(key: &str) -> String {
    format!("{key}_fragments")
}

fn parse_ranges(s: &str) -> HashSet<u64> {
    let mut out = HashSet::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (a, b) = part.split_once('-').unwrap_or((part, part));
        if let (Ok(a), Ok(b)) = (a.parse::<u64>(), b.parse::<u64>()) {
            out.extend(a..=b.min(a + 10_000_000));
        }
    }
    out
}

fn format_ranges(ids: &HashSet<u64>) -> String {
    let mut v: Vec<u64> = ids.iter().copied().collect();
    v.sort_unstable();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let mut j = i;
        while j + 1 < v.len() && v[j + 1] == v[j] + 1 {
            j += 1;
        }
        out.push(format!("{}-{}", v[i], v[j]));
        i = j + 1;
    }
    out.join(",")
}

impl Table for LanceTable {
    fn boxed(&self) -> TableRef {
        self.at(self.ds.clone())
    }

    fn version(&self) -> u64 {
        self.ds.version().version
    }

    fn files(&self) -> Vec<TableFile> {
        self.ds
            .fragments()
            .iter()
            .map(|f| TableFile {
                id: file_id(f.id),
                rows: f.physical_rows.unwrap_or(0),
            })
            .collect()
    }

    fn has_column(&self, name: &str) -> bool {
        self.ds.schema().field(name).is_some()
    }

    fn latest_version(&self) -> BoxFuture<'_, Result<u64>> {
        async move { Ok(self.ds.latest_version_id().await?) }.boxed()
    }

    fn latest(&mut self) -> BoxFuture<'_, Result<()>> {
        async move {
            if self.ds.latest_version_id().await? > self.ds.version().version {
                self.ds.checkout_latest().await?;
            }
            Ok(())
        }
        .boxed()
    }

    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<TableRef>> {
        async move { Ok(self.at(self.ds.checkout_version(version).await?)) }.boxed()
    }

    fn checkout_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<TableRef>> {
        async move { Ok(self.at(self.ds.checkout_version(tag).await?)) }.boxed()
    }

    fn scan(&self, scan: Scan) -> BoxFuture<'_, Result<Vec<RecordBatch>>> {
        async move {
            let s = self.scanner(&scan)?;
            let stored = self.stored();
            let batches: Vec<RecordBatch> = s.try_into_stream().await?.try_collect().await?;
            batches
                .iter()
                .map(|b| types::to_logical(b, &stored))
                .collect()
        }
        .boxed()
    }

    fn scan_stream(&self, scan: Scan) -> BoxFuture<'_, Result<BatchStream>> {
        async move {
            let s = self.scanner(&scan)?;
            let stored = self.stored();
            let st = s.try_into_stream().await?;
            Ok(st.map(move |b| types::to_logical(&b?, &stored)).boxed())
        }
        .boxed()
    }

    fn count(&self, filter: Option<Pred>) -> BoxFuture<'_, Result<usize>> {
        async move { Ok(self.ds.count_rows(filter.map(|p| self.sql(&p))).await?) }.boxed()
    }

    /// Lance keeps no writer marks: callers write them as rows.
    fn append(
        &mut self,
        batches: Vec<RecordBatch>,
        _mark: Option<WriterMark>,
    ) -> BoxFuture<'_, Result<()>> {
        async move {
            let stored = self.stored();
            let batches = batches
                .iter()
                .map(|b| types::to_stored(b, &stored))
                .collect::<Result<Vec<_>>>()?;
            let params = super::write_params(&self.options, WriteMode::Append);
            self.ds = InsertBuilder::new(Arc::new(self.ds.clone()))
                .with_params(&params)
                .execute(batches)
                .await?;
            Ok(())
        }
        .boxed()
    }

    /// `added` is taken as stored: Lance tables gain the unsigned types they had from the start.
    fn add_columns(&mut self, added: SchemaRef) -> BoxFuture<'_, Result<()>> {
        async move {
            self.ds
                .add_columns(NewColumnTransform::AllNulls(added), None, None)
                .await?;
            Ok(())
        }
        .boxed()
    }

    fn rewrite_where(&mut self, pred: Pred) -> BoxFuture<'_, Result<()>> {
        async move {
            self.ds.delete(&self.sql(&pred)).await?;
            Ok(())
        }
        .boxed()
    }

    fn merge_small(&mut self, target_rows: Option<usize>) -> BoxFuture<'_, Result<()>> {
        async move {
            let mut options = CompactionOptions::default();
            if let Some(t) = target_rows {
                options.target_rows_per_fragment = t;
            }
            compact_files(&mut self.ds, options, None).await?;
            Ok(())
        }
        .boxed()
    }

    fn replace(
        &mut self,
        remove: Vec<FileId>,
        schema: SchemaRef,
        source: BatchStream,
        tag: Option<FileTag>,
    ) -> BoxFuture<'_, Result<()>> {
        async move {
            let removed = remove.iter().map(fragment_id).collect::<Result<Vec<_>>>()?;
            let added = self.replace_fragments(removed, schema, source).await?;
            if let Some(tag) = tag {
                // This commit's fragments got the highest ids of its version. A crash before
                // the metadata commit leaves them untagged.
                let mut ids: Vec<u64> = self.ds.fragments().iter().map(|f| f.id).collect();
                ids.sort_unstable();
                let mut tagged = tag
                    .keep
                    .iter()
                    .map(fragment_id)
                    .collect::<Result<HashSet<u64>>>()?;
                tagged.extend(ids.iter().rev().take(added));
                let key = tag_metadata_key(tag.key);
                self.ds
                    .update_metadata([(key.as_str(), format_ranges(&tagged).as_str())])
                    .await?;
            }
            Ok(())
        }
        .boxed()
    }

    fn tagged(&self, key: &'static str) -> Tagged {
        let present: HashSet<u64> = self.ds.fragments().iter().map(|f| f.id).collect();
        let recorded: HashSet<u64> = self
            .ds
            .metadata()
            .get(&tag_metadata_key(key))
            .map(|s| parse_ranges(s))
            .unwrap_or_default();
        let files: HashSet<u64> = recorded.intersection(&present).copied().collect();
        Tagged {
            lost: recorded.len() != files.len(),
            files: files.into_iter().map(file_id).collect(),
        }
    }

    fn compact_removed(&mut self) -> BoxFuture<'_, Result<usize>> {
        super::rewrite_deleted(&mut self.ds).boxed()
    }

    fn index<'a>(&'a mut self, columns: &'a [&'static str]) -> BoxFuture<'a, Result<()>> {
        async move {
            if self.ds.count_rows(None).await? == 0 {
                return Ok(());
            }
            let have: Vec<String> = self
                .ds
                .load_indices()
                .await?
                .iter()
                .map(|i| i.name.clone())
                .collect();
            let mut missing = false;
            for c in columns {
                let name = format!("{c}_idx");
                if !have.contains(&name) {
                    missing = true;
                    self.ds
                        .create_index(
                            &[*c],
                            IndexType::BTree,
                            Some(name),
                            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
                            true,
                        )
                        .await?;
                }
            }
            if !missing {
                // Indexes only the new fragments.
                self.ds
                    .optimize_indices(&lance_index::optimize::OptimizeOptions::default())
                    .await?;
            }
            Ok(())
        }
        .boxed()
    }

    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>> {
        super::cleanup(&self.ds, c.before).boxed()
    }

    #[cfg(feature = "lance-migrate")]
    fn last_commit_micros(&self) -> BoxFuture<'_, Result<Option<i64>>> {
        async move {
            let latest = self.ds.latest_version_id().await?;
            Ok(self
                .ds
                .versions()
                .await?
                .into_iter()
                .find(|v| v.version == latest)
                .map(|v| v.timestamp.timestamp_micros()))
        }
        .boxed()
    }

    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>> {
        async move {
            Ok(self
                .ds
                .tags()
                .list()
                .await?
                .into_iter()
                .map(|(name, c)| (name, c.version))
                .collect())
        }
        .boxed()
    }

    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>> {
        super::tag_version(&self.ds, tag, version).boxed()
    }

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>> {
        async move { Ok(self.ds.tags().delete(tag).await?) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{StringArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    fn rows(t: &TableRef, filter: Pred) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        let batches = crate::rt::block_on({
            let t = t.clone();
            async move { t.scan(Scan::filter(filter)).await }
        })
        .unwrap();
        for b in &batches {
            let c = b.column_by_name("run_id").unwrap();
            let c = c.as_any().downcast_ref::<StringArray>().unwrap();
            out.extend(c.iter().flatten().map(str::to_string));
        }
        out.sort();
        out
    }

    #[test]
    fn predicates_select_the_rows_they_describe() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("seq", DataType::UInt64, false),
        ]));
        let ids = [
            "__kv0_wal__/a",
            "xxkv0_wal__/b",
            "__kv0_wal__/c",
            "it's",
            "50%_off",
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(ids)),
                Arc::new(UInt64Array::from_iter_values(1..=5)),
            ],
        )
        .unwrap();
        let t = crate::rt::block_on(async move {
            let mut t = open("memory://pred", &StorageOptions::new(), schema, true, None).await?;
            t.append(vec![batch], None).await?;
            Ok::<_, Error>(t)
        })
        .unwrap();
        let prefix = |p: &str| Pred::StartsWith("run_id", p.to_string());
        assert_eq!(
            rows(&t, prefix("__kv0_wal__/")),
            ["__kv0_wal__/a", "__kv0_wal__/c"]
        );
        assert_eq!(rows(&t, prefix("50%")), ["50%_off"]);
        assert_eq!(rows(&t, prefix("50_")), Vec::<String>::new());
        assert_eq!(rows(&t, Pred::eq("run_id", "it's")), ["it's"]);
        assert_eq!(
            rows(&t, Pred::is_in("run_id", Vec::<String>::new())),
            Vec::<String>::new()
        );
        assert_eq!(
            rows(&t, Pred::ge("seq", 2i64).and(Pred::lt("seq", 4i64))),
            ["__kv0_wal__/c", "xxkv0_wal__/b"]
        );
        assert_eq!(
            rows(&t, Pred::eq("seq", 1i64).or(Pred::eq("seq", 5i64))),
            ["50%_off", "__kv0_wal__/a"]
        );
        assert_eq!(rows(&t, Pred::And(Vec::new())).len(), 5);
        assert_eq!(rows(&t, Pred::Or(Vec::new())).len(), 0);
    }

    #[test]
    fn signed_filters_on_unsigned_columns_follow_signed_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("run_id", DataType::Utf8, false),
            Field::new("seq", DataType::UInt64, false),
        ]));
        let vals = [i64::MIN, -5, -1, 0, 3, i64::MAX];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    vals.iter().map(|v| v.to_string()),
                )),
                Arc::new(UInt64Array::from_iter_values(
                    vals.iter().map(|v| *v as u64),
                )),
            ],
        )
        .unwrap();
        let t = crate::rt::block_on(async move {
            let mut t = open(
                "memory://signed",
                &StorageOptions::new(),
                schema,
                true,
                None,
            )
            .await?;
            t.append(vec![batch], None).await?;
            Ok::<_, Error>(t)
        })
        .unwrap();
        let check = |p: Pred, want: &dyn Fn(i64) -> bool| {
            let mut expect: Vec<String> = vals
                .iter()
                .filter(|v| want(**v))
                .map(|v| v.to_string())
                .collect();
            expect.sort();
            assert_eq!(rows(&t, p.clone()), expect, "{p:?}");
        };
        for v in [i64::MIN, -6, -5, -2, -1, 0, 1, 3, 4, i64::MAX] {
            check(Pred::ge("seq", v), &|x| x >= v);
            check(Pred::lt("seq", v), &|x| x < v);
            check(Pred::le("seq", v), &|x| x <= v);
            check(Pred::eq("seq", v), &|x| x == v);
            check(Pred::ne("seq", v), &|x| x != v);
        }
        check(Pred::is_in("seq", [-1i64, 3]), &|x| x == -1 || x == 3);
        // Scans return the logical type.
        let batches = crate::rt::block_on({
            let t = t.clone();
            async move { t.scan(Scan::all()).await }
        })
        .unwrap();
        assert_eq!(
            batches[0]
                .schema()
                .field_with_name("seq")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn fragment_ranges_round_trip() {
        let ids: HashSet<u64> = [1, 2, 3, 7, 9, 10].into_iter().collect();
        assert_eq!(format_ranges(&ids), "1-3,7-7,9-10");
        assert_eq!(parse_ranges(&format_ranges(&ids)), ids);
    }
}
