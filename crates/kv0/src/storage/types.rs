//! The logical column types kv0 reads and writes, and their storage at the edge of an engine.
//!
//! kv0's tables are stored with signed integers only (`seq: Int64`, `op: Int8`,
//! `series_id: Int64`, `fields: Int32`): Delta Lake has no unsigned types. The Rust and Python
//! APIs keep their unsigned values; a value crosses with a bit-cast (`as`), so every `u64`
//! round-trips, and ordering follows the signed value. The tracking schemas are declared with
//! the unsigned types of the values; [`logical_schema`] gives the stored, signed form.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// A sequence number as stored. Sequence numbers are hybrid-clock nanoseconds, below
/// `i64::MAX` until the year 2262.
pub(crate) fn seq_to_i64(seq: u64) -> i64 {
    debug_assert!(seq <= i64::MAX as u64, "sequence number {seq} out of range");
    seq as i64
}

pub(crate) fn seq_from_i64(seq: i64) -> u64 {
    seq as u64
}

/// A series id as stored: the xxh3 hash, bit-cast.
pub(crate) fn series_to_i64(id: u64) -> i64 {
    id as i64
}

pub(crate) fn series_from_i64(id: i64) -> u64 {
    id as u64
}

/// The signed type that stores `t`, or `None` when `t` is stored as it is.
fn signed(t: &DataType) -> Option<DataType> {
    Some(match t {
        DataType::UInt8 => DataType::Int8,
        DataType::UInt16 => DataType::Int16,
        DataType::UInt32 => DataType::Int32,
        DataType::UInt64 => DataType::Int64,
        _ => return None,
    })
}

/// `schema` with every unsigned integer column signed: the schema kv0 reads and stores for a
/// table declared as `schema`.
pub(crate) fn logical_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| match signed(f.data_type()) {
                Some(t) => Arc::new(Field::new(f.name(), t, f.is_nullable())),
                None => f.clone(),
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}
