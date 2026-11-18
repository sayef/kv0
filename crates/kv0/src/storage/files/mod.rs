//! Reading Parquet data files directly, without a query engine: the Delta Lake and moraine
//! engines both keep their rows in plain Parquet files whose paths never change, so a file can
//! be scanned on its own and a value fetched by its row ordinal.

pub(crate) mod filter;
pub(crate) mod footer_cache;
pub(crate) mod scan;
pub(crate) mod take;

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
