//! Parquet writer settings and table properties of kv0's Delta tables.

use std::collections::HashMap;
use std::time::Duration;

use deltalake_core::parquet::basic::{Compression, ZstdLevel};
use deltalake_core::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake_core::parquet::schema::types::ColumnPath;

/// Rows per row group: the batch size kv0 writes in.
pub(crate) const ROW_GROUP_ROWS: usize = 64 * 1024;

/// The format marker every kv0 table carries in its version-0 metadata.
pub(crate) const FORMAT_KEY: &str = "kv0.format";
pub(crate) const FORMAT: &str = "delta/1";

/// Compression of every kv0 data file: ZSTD level 1. Against Snappy on the key-value log
/// (1M puts of 40-byte keys and 16-byte values; 20k values of 8 KiB), it stored 44 % and 6 %
/// fewer bytes, opened 8 % faster, and wrote within the run-to-run noise for small values and
/// 7 % slower for large ones.
pub(crate) fn compression() -> Compression {
    Compression::ZSTD(ZstdLevel::default())
}

/// Writer settings for the key-value operation log.
///
/// Page statistics write the column and offset indexes, which reading one value by row ordinal
/// needs to skip to its page. Values get small pages (at most 64 KiB or 256 rows) and no
/// dictionary, so a lookup decodes little more than the value it wants; keys are mostly unique,
/// so a dictionary would only cost.
pub(crate) fn kv_writer() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(compression())
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_size_limit(64 << 10)
        .set_data_page_row_count_limit(256)
        .set_write_batch_size(256)
        .set_column_dictionary_enabled(ColumnPath::from("key"), false)
        .set_column_dictionary_enabled(ColumnPath::from("value"), false)
        .build()
}

/// Writer settings for tracking and records tables: page statistics (and with them the column
/// and offset indexes), and a Bloom filter on `bloom` (records' `item_id`), so a lookup of one
/// value skips the row groups that cannot hold it.
pub(crate) fn rows_writer(bloom: Option<&str>) -> WriterProperties {
    let b = WriterProperties::builder()
        .set_compression(compression())
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::Page);
    match bloom {
        Some(c) => b
            .set_column_bloom_filter_enabled(ColumnPath::from(c), true)
            .build(),
        None => b.build(),
    }
}

/// A duration as a Delta interval string.
fn interval(d: Option<Duration>) -> String {
    match d {
        // "Keep everything": a century.
        None => "interval 36500 days".into(),
        Some(d) => format!("interval {} milliseconds", d.as_millis().max(1)),
    }
}

/// Table properties, set once in the version-0 commit: a metadata change later would conflict
/// with every concurrent writer.
///
/// - kv0 deletes old log files itself (tagged versions must stay loadable), so delta-rs's own
///   log cleanup is off;
/// - only `stats_columns` get min/max statistics in the log, so keys and values never land in
///   commit files.
pub(crate) fn table_config(
    retention: Option<Duration>,
    mark_ttl: Duration,
    stats_columns: &str,
) -> HashMap<String, Option<String>> {
    let keep = interval(retention);
    [
        (FORMAT_KEY, FORMAT.to_string()),
        ("delta.enableExpiredLogCleanup", "false".into()),
        ("delta.logRetentionDuration", keep.clone()),
        ("delta.deletedFileRetentionDuration", keep),
        (
            "delta.setTransactionRetentionDuration",
            interval(Some(mark_ttl)),
        ),
        ("delta.checkpointInterval", "100".into()),
        ("delta.dataSkippingStatsColumns", stats_columns.into()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), Some(v)))
    .collect()
}
