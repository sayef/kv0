//! Parquet footers (with their page indexes) of data files, kept per handle.
//!
//! Reading one value by row ordinal needs the file's footer and offset index; loading them costs
//! a request or two per file, so they are cached. Data files never change, so entries never go
//! stale: they are only evicted, least recently used first, once the cache holds more than its
//! byte budget.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake_core::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
#[allow(deprecated)] // see `scan`
use deltalake_core::parquet::arrow::async_reader::ParquetObjectReader;
use deltalake_core::parquet::file::metadata::PageIndexPolicy;
use deltalake_core::{ObjectStore, Path};
use parking_lot::Mutex;

use crate::error::Result;

/// Bytes of footers kept per handle.
const BUDGET: usize = 64 << 20;

#[derive(Default)]
pub(crate) struct FooterCache {
    inner: Mutex<Lru>,
}

#[derive(Default)]
struct Lru {
    map: HashMap<String, (ArrowReaderMetadata, usize, u64)>,
    bytes: usize,
    tick: u64,
}

impl FooterCache {
    /// The footer of the file at `path` (`size` bytes long), loaded on a miss.
    pub(crate) async fn get(
        &self,
        store: Arc<dyn ObjectStore>,
        path: &Path,
        size: u64,
    ) -> Result<ArrowReaderMetadata> {
        let key = path.to_string();
        {
            let mut l = self.inner.lock();
            l.tick += 1;
            let tick = l.tick;
            if let Some(e) = l.map.get_mut(&key) {
                e.2 = tick;
                return Ok(e.0.clone());
            }
        }
        #[allow(deprecated)]
        let mut reader = ParquetObjectReader::new(store, path.clone()).with_file_size(size);
        let meta = ArrowReaderMetadata::load_async(
            &mut reader,
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional),
        )
        .await?;
        let bytes = meta.metadata().memory_size();
        let mut l = self.inner.lock();
        l.tick += 1;
        let tick = l.tick;
        if l.map.insert(key, (meta.clone(), bytes, tick)).is_none() {
            l.bytes += bytes;
        }
        while l.bytes > BUDGET && l.map.len() > 1 {
            let oldest = l
                .map
                .iter()
                .min_by_key(|(_, e)| e.2)
                .map(|(k, _)| k.clone())
                .expect("not empty");
            if let Some(e) = l.map.remove(&oldest) {
                l.bytes -= e.1;
            }
        }
        Ok(meta)
    }
}
