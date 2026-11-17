//! The key-value layer's storage: an append-only log of operations, read whole into the view.
//!
//! A [`Log`] is one committed version of the log. It never changes: an append, a rewrite or a
//! merge commits a new version and returns it. Rows are identified by a row address (a `u64`
//! the view keeps for values it leaves in storage) and live in files, each with an id that
//! stays the same for as long as the file is part of the log.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;

use crate::error::Result;
use crate::kv::view::{Op, SnapChunk};
use crate::kv::VersionInfo;

/// A committed version of an operation log. Cheap to clone.
pub(crate) type Log = Arc<dyn OpLog>;

/// A data file of the log, stable for as long as the file is part of the log.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FileId(pub String);

/// A data file of a log version.
#[derive(Clone, Debug)]
pub(crate) struct LogFile {
    pub id: FileId,
    /// Rows stored in the file, superseded ones included.
    pub rows: usize,
    /// What the file holds, for checking that a view snapshot still describes it: equal
    /// identities mean the same rows at the same addresses.
    pub ident: String,
}

/// Which columns a scan reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Columns {
    /// Every column.
    All,
    /// `seq`, `op` and `key`; rows arrive without values.
    Keys,
}

/// One stored row, borrowed from the batch being scanned.
pub(crate) struct RawRow<'a> {
    pub seq: u64,
    pub op: u8,
    pub key: &'a [u8],
    /// `None` for a null value, and for every row of a [`Columns::Keys`] scan.
    pub value: Option<&'a [u8]>,
    pub addr: u64,
}

/// Receives the rows of a scan.
pub(crate) type Sink<'a> = &'a mut (dyn FnMut(RawRow<'_>) + Send);

/// The writer of an append and its highest sequence number in it, so a WAL replay can tell
/// which of its operations were committed.
#[derive(Clone, Debug)]
pub(crate) struct WriterMark {
    pub writer: String,
    pub seq: u64,
}

/// One committed version of an operation log in some storage engine.
pub(crate) trait OpLog: Send + Sync + 'static {
    /// The location of the log.
    #[cfg_attr(not(test), allow(dead_code))]
    fn uri(&self) -> &str;

    /// This version's number.
    fn version(&self) -> u64;

    /// The data files of this version.
    fn files(&self) -> Vec<LogFile>;

    /// Files small enough for [`OpLog::merge_small`] to merge.
    fn small_files(&self) -> usize;

    /// Calls `sink` for every row of `files` (every file when `None`), in storage order.
    fn scan<'a>(
        &'a self,
        files: Option<&'a HashSet<FileId>>,
        columns: Columns,
        sink: Sink<'a>,
    ) -> BoxFuture<'a, Result<()>>;

    /// The values at row addresses `addrs`, in the same order.
    fn take<'a>(&'a self, addrs: &'a [u64]) -> BoxFuture<'a, Result<Vec<Bytes>>>;

    /// Appends `ops` (and the writer's mark) in one commit.
    fn append(&self, ops: Vec<Op>, mark: Option<WriterMark>) -> BoxFuture<'static, Result<Log>>;

    /// Replaces every file of this version with files written from `chunks`, in one commit.
    /// Values the chunks leave in storage are read from this version while writing. Files
    /// other writers appended meanwhile are kept.
    fn rewrite(&self, chunks: Vec<SnapChunk>) -> BoxFuture<'static, Result<Log>>;

    /// Merges small files into larger ones, rows unchanged, in one commit.
    fn merge_small(&self) -> BoxFuture<'static, Result<Log>>;

    /// Version `version` of the same log.
    fn checkout(&self, version: u64) -> BoxFuture<'_, Result<Log>>;

    /// The latest version, when it is newer than this one.
    fn refresh(&self) -> BoxFuture<'_, Result<Option<Log>>>;

    /// Every version still stored, oldest first.
    fn versions(&self) -> BoxFuture<'_, Result<Vec<VersionInfo>>>;

    /// Removes versions committed before `c.before` and the files only they use (see
    /// [`crate::storage::Cleanup`]). The latest version and tagged versions are kept. Returns
    /// (versions, bytes) removed.
    fn cleanup(&self, c: crate::storage::Cleanup) -> BoxFuture<'_, Result<(u64, u64)>>;

    /// Whether kv0 must run version cleanup itself (every few commits); `false` when the
    /// engine cleans up on its own (Lance's automatic cleanup, configured at creation).
    fn needs_cleanup_cadence(&self) -> bool {
        false
    }

    /// Tags and the versions they name, in no particular order.
    fn tags(&self) -> BoxFuture<'_, Result<Vec<(String, u64)>>>;

    /// The version a tag names.
    fn tag_version<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<u64>>;

    /// Tags `version`, failing when a concurrent cleanup removed it first.
    fn create_tag<'a>(&'a self, tag: &'a str, version: u64) -> BoxFuture<'a, Result<()>>;

    fn delete_tag<'a>(&'a self, tag: &'a str) -> BoxFuture<'a, Result<()>>;

    /// The highest sequence number `writer` committed, for engines that keep writer marks
    /// outside the rows (Delta's `txn` actions). `None` when the engine keeps them as rows,
    /// which the view reads like any other.
    fn writer_mark<'a>(&'a self, _writer: &'a str) -> BoxFuture<'a, Result<Option<u64>>> {
        futures::future::ready(Ok(None)).boxed()
    }

    /// The log's identity: a log deleted and created again at the same location gets a new
    /// one. Empty when the engine has none.
    fn log_id(&self) -> String {
        String::new()
    }

    /// The engine's table behind row addresses, when it keeps one per handle (Delta numbers
    /// files as it meets them), so a view snapshot can restore it with its addresses.
    fn addr_table(&self) -> Vec<String> {
        Vec::new()
    }

    /// Restores a table [`OpLog::addr_table`] returned, before anything is read.
    fn restore_addr_table(&self, _table: Vec<String>) {}

    /// `(key, seq, row address)` of every put the commit that made this version wrote, when
    /// this handle made it and the engine knows them without reading (an append).
    fn written(&self) -> Option<Arc<Vec<(Bytes, u64, u64)>>> {
        None
    }
}

/// Opens the log at `uri`: at `version` when given, else the latest version, creating an empty
/// log when there is none and `create` is set. `retention` and `mark_ttl` (how long writer marks
/// are kept, where the engine keeps them itself) apply to a log created here.
///
/// An existing log opens with the engine it was written in; a new one with `engine`, else the
/// one `KV0_ENGINE` names, else Lance. Asking for another engine than an existing log's fails
/// (see [`super::format::choose`]).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open(
    uri: &str,
    options: &super::StorageOptions,
    version: Option<u64>,
    create: bool,
    retention: Option<std::time::Duration>,
    mark_ttl: std::time::Duration,
    engine: Option<super::Engine>,
) -> Result<Log> {
    use super::format::{choose, Kind};
    let (engine, format) = choose(uri, options, Kind::Database, engine).await?;
    match engine {
        #[cfg(feature = "delta")]
        super::Engine::Delta => {
            let new = matches!(format, super::Format::Empty | super::Format::Unknown);
            if new && create && version.is_none() {
                super::format::write_marker(uri, options, Kind::Database).await?;
            }
            super::delta::kv::open(uri, options, version, create, retention, mark_ttl).await
        }
        #[cfg(not(feature = "delta"))]
        super::Engine::Delta => unreachable!("choose refuses Delta without the feature"),
        super::Engine::Lance => {
            let _ = (mark_ttl, format);
            super::lance::kv::open(uri, options, version, create, retention).await
        }
    }
}
