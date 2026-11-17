//! Record batches between Arrow 58 (the version kv0 and Lance build with) and Arrow 59 (the
//! version of the Delta Lake engine), through the Arrow C data interface: the buffers are
//! handed over, not copied.
//!
//! Both versions implement the same C structs (`ArrowArray`, `ArrowSchema`), so a struct
//! exported by one is imported by the other.

// Used once a second engine joins; tested here so it is ready.
#![allow(dead_code)]

use arrow_array::ffi as ffi58;
use arrow_array::{Array as _, RecordBatch, StructArray};
use arrow_array_59::ffi as ffi59;
use arrow_array_59::{Array as _, RecordBatch as RecordBatch59, StructArray as StructArray59};

use crate::error::{Error, Result};

fn bridge(e: impl std::fmt::Display) -> Error {
    Error::Storage(format!("moving a record batch between Arrow versions: {e}").into())
}

/// `batch` as an Arrow 59 record batch.
pub(crate) fn to_59(batch: &RecordBatch) -> Result<RecordBatch59> {
    let data = StructArray::from(batch.clone()).into_data();
    let (array, schema) = ffi58::to_ffi(&data).map_err(bridge)?;
    // SAFETY: both are `#[repr(C)]` definitions of the C data interface's `ArrowArray` and
    // `ArrowSchema`; the exported release callbacks travel with them.
    let (array, schema) = unsafe {
        (
            std::mem::transmute::<ffi58::FFI_ArrowArray, ffi59::FFI_ArrowArray>(array),
            std::mem::transmute::<ffi58::FFI_ArrowSchema, ffi59::FFI_ArrowSchema>(schema),
        )
    };
    // SAFETY: `array` and `schema` were exported together just above.
    let data = unsafe { ffi59::from_ffi(array, &schema) }.map_err(bridge)?;
    let s = StructArray59::from(data);
    debug_assert_eq!(s.len(), batch.num_rows());
    Ok(RecordBatch59::from(s))
}

/// `batch` as an Arrow 58 record batch.
pub(crate) fn from_59(batch: &RecordBatch59) -> Result<RecordBatch> {
    let data = StructArray59::from(batch.clone()).into_data();
    let (array, schema) = ffi59::to_ffi(&data).map_err(bridge)?;
    // SAFETY: as in `to_59`, the other way round.
    let (array, schema) = unsafe {
        (
            std::mem::transmute::<ffi59::FFI_ArrowArray, ffi58::FFI_ArrowArray>(array),
            std::mem::transmute::<ffi59::FFI_ArrowSchema, ffi58::FFI_ArrowSchema>(schema),
        )
    };
    // SAFETY: `array` and `schema` were exported together just above.
    let data = unsafe { ffi58::from_ffi(array, &schema) }.map_err(bridge)?;
    let s = StructArray::from(data);
    debug_assert_eq!(s.len(), batch.num_rows());
    Ok(RecordBatch::from(s))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{BinaryArray, Int64Array, Int8Array, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn batches_round_trip_between_arrow_versions() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        for i in 0..3 {
            for t in 0..i {
                tags.values().append_value(format!("t{t}"));
            }
            tags.append(i != 1);
        }
        let tags = tags.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("op", DataType::Int8, false),
            Field::new("value", DataType::Binary, true),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
                Arc::new(Int8Array::from(vec![-1, 0, 3])),
                Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), None, Some(b"")])),
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone("UTC")),
                Arc::new(tags),
            ],
        )
        .unwrap();
        let there = to_59(&batch).unwrap();
        assert_eq!(there.num_rows(), 3);
        assert_eq!(there.schema().field(4).name(), "tags");
        let back = from_59(&there).unwrap();
        assert_eq!(back, batch);
        // A slice keeps its offset across the bridge.
        let sliced = batch.slice(1, 2);
        assert_eq!(from_59(&to_59(&sliced).unwrap()).unwrap(), sliced);
    }
}
