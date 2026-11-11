//! Arrow schemas of the tracking tables.

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

/// `runs`: one row per change of a run; the row with the highest `seq` is current.
pub static RUNS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("experiment", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("status", DataType::Utf8, false),
        Field::new("archived", DataType::Boolean, false),
        Field::new("created_at", ts(), false),
        Field::new("updated_at", ts(), false),
        Field::new("ended_at", ts(), true),
        // Which fields this row sets (bits of `tracking::fields`); null means all of them
        // (rows written before per-field updates).
        Field::new("fields", DataType::UInt32, true),
        Field::new("kind", DataType::Utf8, true),
        // JSON.
        Field::new("target", DataType::Utf8, true),
    ]))
});

/// Columns added to `runs` after its first release, with how old tables gain them.
pub static RUNS_ADDED: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("fields", DataType::UInt32, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("target", DataType::Utf8, true),
    ]))
});

/// `params`: flattened parameters, one row per (run, dotted key) change; highest `seq` wins.
/// `value` is JSON; `num`, `text` and `flag` repeat scalars in typed columns for filtering.
pub static PARAMS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::UInt64, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("num", DataType::Float64, true),
        Field::new("text", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
    ]))
});

/// `series`: one row per metric series (run, name, context). `series_id` is a hash of the three.
pub static SERIES: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("run_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("context", DataType::Utf8, false),
        Field::new("created_at", ts(), false),
    ]))
});

/// `metrics`: append-only points. A BTree index on `series_id` is built by `optimize`.
pub static METRICS: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("series_id", DataType::UInt64, false),
        Field::new("step", DataType::Int64, false),
        Field::new("epoch", DataType::Int64, true),
        Field::new("timestamp", ts(), false),
        Field::new("value", DataType::Float64, false),
        // Reserved rows only (`series_id` 0): an encoded statistics delta for the series whose
        // id is in `step`. Null for points and WAL marks.
        Field::new("stats", DataType::Binary, true),
    ]))
});

/// Columns added to `metrics` after its first release.
pub static METRICS_ADDED: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "stats",
        DataType::Binary,
        true,
    )]))
});
