//! Reading Parquet data files directly, without a query engine: moraine keeps its rows in plain
//! Parquet files whose paths never change, so the key-value layer scans a file on its own and
//! numbers its rows.

pub(crate) mod footer_cache;
pub(crate) mod scan;

/// Whether `e` (or one of its sources) is a missing object or file.
pub(crate) fn not_found(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(e) = cur {
        if let Some(object_store::Error::NotFound { .. }) = e.downcast_ref::<object_store::Error>()
        {
            return true;
        }
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::NotFound {
                return true;
            }
        }
        cur = e.source();
    }
    false
}
