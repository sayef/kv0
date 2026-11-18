//! Hooks for kv0's own concurrency and crash tests. Not part of the supported API: anything
//! here may change in any release.
//!
//! [`set_faults`] makes the object stores of Delta Lake and moraine tables fail, delay or lose the
//! responses of operations at seeded random points, or end the process at a chosen write, as a
//! crash would. Tables opened before faults are first configured in a process are not affected.

/// An object-store operation, as faults see it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOp {
    /// An unconditional write (data files, checkpoints, `_last_checkpoint`).
    Put,
    /// A create-if-absent write (commit files, tags, markers).
    ConditionalPut,
    /// A read, whole or in ranges.
    Get,
    /// A listing.
    List,
    /// A delete.
    Delete,
}

/// What an injected fault does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    /// Fails without doing anything.
    Error,
    /// Waits 200 ms, then fails without doing anything.
    Timeout,
    /// Does the write, then fails: the response was lost (writes only).
    LostResponse,
    /// Does the write twice and returns the second result, as a client that retried after a
    /// lost response (writes only; a create-if-absent write then fails with "already exists").
    DuplicateSend,
}

/// Ends the process (SIGKILL, as `kill -9`) at the `nth` operation (from 1) of one of `ops`,
/// before it runs or, with `after`, after it succeeded.
#[derive(Clone, Debug)]
pub struct Crash {
    pub ops: Vec<StoreOp>,
    pub nth: u64,
    pub after: bool,
}

/// Holds the `nth` operation (from 1) of one of `ops` on a path containing `path` for `millis`
/// before it runs, so a test can act while that operation is in flight.
#[derive(Clone, Debug)]
pub struct Stall {
    pub ops: Vec<StoreOp>,
    pub path: String,
    pub nth: u64,
    pub millis: u64,
}

/// Faults injected into every Delta Lake table and moraine repository opened afterwards in this
/// process.
#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Seeds the random choice of operations and kinds; print it to reproduce a run.
    pub seed: u64,
    /// The share of matching writes and deletes that fail, from 0 to 1.
    pub rate: f64,
    /// The share of matching reads and listings that fail. Kept lower than `rate`: opening a
    /// table reads every commit since the last checkpoint, and one failed read fails the open.
    pub read_rate: f64,
    /// The operations that may fail.
    pub ops: Vec<StoreOp>,
    /// The kinds of failure, chosen at random per fault.
    pub kinds: Vec<FaultKind>,
    /// Only paths containing this may fail, when set.
    pub paths: Option<String>,
    /// Where to end the process, when set.
    pub crash: Option<Crash>,
    /// Which operation to hold, when set.
    pub stall: Option<Stall>,
}

/// Configures faults for this process (`None` stops them; tables opened since the first call
/// stay wrapped and pass operations through).
pub fn set_faults(faults: Option<Faults>) {
    crate::storage::faults::set(faults);
}

/// Operations held by [`Faults::stall`] so far in this process (counted when the hold starts).
pub fn stalls_entered() -> u64 {
    crate::storage::faults::stalls_entered()
}

/// Faults injected so far in this process.
pub fn faults_injected() -> u64 {
    crate::storage::faults::injected()
}

/// Whether a file under the local directory `dir` holds `needle`: in its raw bytes, or, for a
/// Parquet file (compressed, so a byte search misses), in any string or binary value once
/// decoded. Erasure tests use it to show that deleted data is gone.
#[cfg(any(feature = "delta", feature = "moraine"))]
pub fn stored_anywhere(dir: &std::path::Path, needle: &[u8]) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    rd.flatten().any(|e| {
        let p = e.path();
        if p.is_dir() {
            return stored_anywhere(&p, needle);
        }
        file_holds(&p, needle)
    })
}

/// [`stored_anywhere`] for one file.
#[cfg(any(feature = "delta", feature = "moraine"))]
pub fn file_holds(p: &std::path::Path, needle: &[u8]) -> bool {
    let Ok(bytes) = std::fs::read(p) else {
        return false;
    };
    if bytes.windows(needle.len()).any(|w| w == needle) {
        return true;
    }
    p.extension().is_some_and(|x| x == "parquet") && parquet_holds(bytes, needle)
}

#[cfg(any(feature = "delta", feature = "moraine"))]
fn parquet_holds(bytes: Vec<u8>, needle: &[u8]) -> bool {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let Ok(reader) =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).and_then(|b| b.build())
    else {
        return false;
    };
    reader.flatten().any(|batch| {
        batch
            .columns()
            .iter()
            .any(|c| array_holds(c.as_ref(), needle))
    })
}

#[cfg(any(feature = "delta", feature = "moraine"))]
fn array_holds(a: &dyn arrow_59::array::Array, needle: &[u8]) -> bool {
    use arrow_59::array::{
        BinaryArray, DictionaryArray, LargeBinaryArray, LargeListArray, LargeStringArray,
        ListArray, MapArray, StringArray, StructArray,
    };
    use arrow_59::datatypes::{DataType, Int32Type};
    let has = |v: &[u8]| v.windows(needle.len()).any(|w| w == needle);
    let any = a.as_any();
    match a.data_type() {
        DataType::Utf8 => any
            .downcast_ref::<StringArray>()
            .is_some_and(|s| s.iter().flatten().any(|v| has(v.as_bytes()))),
        DataType::LargeUtf8 => any
            .downcast_ref::<LargeStringArray>()
            .is_some_and(|s| s.iter().flatten().any(|v| has(v.as_bytes()))),
        DataType::Binary => any
            .downcast_ref::<BinaryArray>()
            .is_some_and(|s| s.iter().flatten().any(has)),
        DataType::LargeBinary => any
            .downcast_ref::<LargeBinaryArray>()
            .is_some_and(|s| s.iter().flatten().any(has)),
        DataType::Struct(_) => any
            .downcast_ref::<StructArray>()
            .is_some_and(|s| s.columns().iter().any(|c| array_holds(c.as_ref(), needle))),
        DataType::List(_) => any
            .downcast_ref::<ListArray>()
            .is_some_and(|l| array_holds(l.values().as_ref(), needle)),
        DataType::LargeList(_) => any
            .downcast_ref::<LargeListArray>()
            .is_some_and(|l| array_holds(l.values().as_ref(), needle)),
        DataType::Map(_, _) => any
            .downcast_ref::<MapArray>()
            .is_some_and(|m| array_holds(m.entries(), needle)),
        DataType::Dictionary(_, _) => any
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .is_some_and(|d| array_holds(d.values().as_ref(), needle)),
        _ => false,
    }
}
