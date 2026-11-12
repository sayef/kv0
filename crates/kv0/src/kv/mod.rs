//! Ordered key-value layer, stored in Lance.
//!
//! - Keys and values are byte strings; keys are ordered byte-wise (`memcmp`).
//! - Writes land in an in-memory view immediately (read-your-writes) and in a pending buffer that
//!   is appended to Lance as one fragment per flush: when the buffer exceeds
//!   [`Options::write_buffer_size`], every [`Options::flush_interval`], on [`Db::flush`] and on
//!   close. A [`WriteBatch`] is never split across flushes, so it is atomic in storage too.
//! - Writes that were not flushed are lost if the process crashes.
//! - Reads are served from the in-memory view, which holds every live key and value.
//! - Other writers' commits become visible on [`Db::refresh`], which reads run automatically at
//!   most every [`Options::refresh_interval`].
//!
//! See `DESIGN.md` for the consistency model.

mod cache;
mod snapshot;
mod store;
pub(crate) mod view;
pub(crate) mod wal;

pub use wal::{default_root as default_wal_root, WalSync};

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use lance::dataset::cleanup::CleanupPolicyBuilder;
use lance::dataset::optimize::{compact_files, CompactionOptions};
use lance::dataset::Dataset;
use parking_lot::{Mutex, RwLock};

use self::view::{Op, Val, View, OP_DELETE, OP_DELETE_RANGE, OP_PUT};
use crate::background::{self, Background};
use crate::clock::{now_nanos, Clock};
use crate::error::{Error, Result};
use crate::rt;
use crate::storage::{self, StorageOptions};

/// Options for [`Db::open`].
#[derive(Clone, Debug)]
pub struct Options {
    /// Create the database when it does not exist. Default `true`.
    pub create_if_missing: bool,
    /// Open without write access. Writes return [`Error::NotSupported`]. Default `false`.
    pub read_only: bool,
    /// Open a fixed version for time travel; implies `read_only`. Default `None` (latest).
    pub version: Option<u64>,
    /// Object-store options passed to Lance, such as `aws_endpoint` or `aws_region`.
    pub storage_options: StorageOptions,
    /// Flush once this many bytes of writes are buffered. Default 4 MiB.
    pub write_buffer_size: usize,
    /// Flush buffered writes in the background after this long. `None` flushes only on demand,
    /// when the buffer is full and on close. Default 5 s.
    pub flush_interval: Option<Duration>,
    /// How often reads check storage for other writers' commits. `None` checks only on
    /// [`Db::refresh`]. Default 1 s.
    pub refresh_interval: Option<Duration>,
    /// Compact once the dataset has more fragments than this. `0` disables automatic
    /// compaction. Default 32.
    pub max_fragments: usize,
    /// Keep delete markers this long when compacting, so a slower concurrent writer's older
    /// write to the same key stays deleted. Default 1 h.
    pub tombstone_grace: Duration,
    /// When creating the database, enable Lance's automatic cleanup of versions older than this
    /// (checked every 20 commits). `None` keeps every version until
    /// [`Db::cleanup_old_versions`]. Default 7 days.
    pub version_retention: Option<Duration>,
    /// Log every write to a local write-ahead log before acknowledging it, so buffered writes
    /// survive a process crash. Ignored for read-only handles and `memory://`. Default `true`.
    pub wal: bool,
    /// When WAL writes reach the disk. Default [`WalSync::Batch`].
    pub wal_sync: WalSync,
    /// WAL root directory. `None` uses `KV0_WAL_DIR`, else the platform cache directory
    /// (see [`default_wal_root`]).
    pub wal_dir: Option<std::path::PathBuf>,
    /// How long compaction keeps another writer's WAL mark (its committed high-water mark).
    /// A WAL left behind longer than this cannot be replayed safely; see `replay_stale_wal`.
    /// Default 30 days.
    pub mark_ttl: Duration,
    /// Replay a WAL older than `mark_ttl` whose writer has no mark left. Its writes may then be
    /// stored twice (they read back correctly; storage holds duplicate rows). Default `false`:
    /// such a WAL fails the open with an error that names the directory.
    pub replay_stale_wal: bool,
    /// Values longer than this many bytes are not kept in memory once committed: the view holds
    /// their row address and reads them from Lance when needed. `0` keeps every value in
    /// memory. Default 4 KiB.
    pub lazy_value_threshold: usize,
    /// Bytes of values read from Lance kept in an LRU cache per handle. Default 64 MiB.
    pub value_cache_size: usize,
    /// On close, save a snapshot of the view in the WAL directory when it holds at least this
    /// many entries, so the next open loads it and reads only newer fragments instead of the
    /// whole dataset. `usize::MAX` disables snapshots. Default 100 000.
    pub snapshot_min_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            read_only: false,
            version: None,
            storage_options: StorageOptions::new(),
            write_buffer_size: 4 << 20,
            flush_interval: Some(Duration::from_secs(5)),
            refresh_interval: Some(Duration::from_secs(1)),
            max_fragments: 32,
            tombstone_grace: Duration::from_secs(3600),
            version_retention: Some(Duration::from_secs(7 * 24 * 3600)),
            wal: true,
            wal_sync: WalSync::Batch,
            wal_dir: None,
            mark_ttl: Duration::from_secs(30 * 24 * 3600),
            replay_stale_wal: false,
            lazy_value_threshold: 4096,
            value_cache_size: 64 << 20,
            snapshot_min_entries: 100_000,
        }
    }
}

/// Per-write options for [`Db::write_opt`]: `sync` and `disable_wal`.
#[derive(Clone, Copy, Debug, Default)]
pub struct WriteOptions {
    /// Commit to Lance before returning.
    pub sync: bool,
    /// Skip the write-ahead log: the write is lost if the process crashes before the next flush.
    pub disable_wal: bool,
}

/// A group of writes applied atomically by [`Db::write`].
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    ops: Vec<(u8, Bytes, Option<Bytes>)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> &mut Self {
        self.ops.push((
            OP_PUT,
            Bytes::copy_from_slice(key.as_ref()),
            Some(Bytes::copy_from_slice(value.as_ref())),
        ));
        self
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> &mut Self {
        self.ops
            .push((OP_DELETE, Bytes::copy_from_slice(key.as_ref()), None));
        self
    }

    /// Deletes keys in `[begin, end)`.
    pub fn delete_range(&mut self, begin: impl AsRef<[u8]>, end: impl AsRef<[u8]>) -> &mut Self {
        self.ops.push((
            OP_DELETE_RANGE,
            Bytes::copy_from_slice(begin.as_ref()),
            Some(Bytes::copy_from_slice(end.as_ref())),
        ));
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn clear(&mut self) {
        self.ops.clear();
    }
}

/// A committed version of the database.
#[derive(Clone, Debug)]
pub struct VersionInfo {
    pub version: u64,
    /// Commit time in microseconds since the Unix epoch (UTC).
    pub timestamp_micros: i64,
}

/// Counters describing a handle's state.
#[derive(Clone, Debug)]
pub struct Stats {
    pub version: u64,
    pub live_keys: usize,
    pub pending_ops: usize,
    pub pending_bytes: usize,
    pub fragments: usize,
    pub stored_rows: usize,
    /// Entries in the in-memory view, tombstones included.
    pub view_entries: usize,
}

struct State {
    view: View,
    /// The dataset version the view's row addresses refer to.
    reader: Dataset,
    pending: Vec<Op>,
    pending_bytes: usize,
    clock: Clock,
    last_flush: Instant,
    last_refresh: Instant,
}

struct Persist {
    dataset: Dataset,
    /// Fragments whose rows are in the view.
    applied: HashSet<u64>,
}

struct Inner {
    uri: String,
    opts: Options,
    read_only: bool,
    closed: AtomicBool,
    /// The version, fragment count and stored rows of `persist.dataset`, readable without
    /// taking `persist` (which a flush holds across a commit).
    version: AtomicU64,
    fragments: AtomicU64,
    stored_rows: AtomicU64,
    state: RwLock<State>,
    persist: Mutex<Persist>,
    /// Lock order: `persist`, then `state`, then `wal`.
    wal: Mutex<Option<wal::Wal>>,
    /// Set by [`Db::abandon`]: drop without flushing, as a crash would.
    abandoned: AtomicBool,
    /// A background flush is running.
    flushing: AtomicBool,
    /// Values read from Lance, by row address.
    cache: Mutex<cache::ValueCache>,
    /// Reads of stored values in progress, by row address, so concurrent misses share one.
    flights: Mutex<std::collections::HashMap<u64, Arc<Flight>>>,
    /// Where this handle saves its view snapshot; `None` when snapshots are off.
    snapshot_path: Option<std::path::PathBuf>,
    /// The dataset version the snapshot file holds, as far as this handle knows.
    snapshot_version: AtomicU64,
    /// This handle started from a snapshot.
    from_snapshot: bool,
}

/// A read of a stored value in progress.
#[derive(Default)]
struct Flight {
    done: Mutex<bool>,
    cv: parking_lot::Condvar,
}

/// Whether `e` says a data file is gone (as after another writer's cleanup of old versions).
/// Lance reports a missing object as `NotFound` (see `Error`'s conversion); the message match
/// below is only a fallback for errors that arrive without their source.
fn missing_file(e: &Error) -> bool {
    match e {
        Error::NotFound(_) => true,
        Error::Io(m) => m.contains("No such file") || m.contains("not found"),
        Error::Storage(m) => {
            let m = m.to_string();
            m.contains("Not found") || m.contains("NotFound") || m.contains("No such file")
        }
        _ => false,
    }
}

/// An ordered key-value database. Cheap to clone; clones share one handle.
#[derive(Clone)]
pub struct Db {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").field("uri", &self.inner.uri).finish()
    }
}

impl Db {
    /// Opens (or creates) the database at `location`: a local path or any URI Lance supports.
    pub fn open(location: &str, opts: Options) -> Result<Db> {
        let uri = storage::normalize_uri(location)?;
        let read_only = opts.read_only || opts.version.is_some();
        let snapshot_path = (opts.version.is_none()
            && opts.snapshot_min_entries != usize::MAX
            && !uri.starts_with("memory://"))
        .then(|| {
            let root = opts.wal_dir.clone().unwrap_or_else(wal::default_root);
            snapshot::path(&root, &uri)
        });
        let loaded = snapshot_path.as_ref().and_then(|p| match snapshot::read(p) {
            Ok(l) => l,
            Err(e) => {
                tracing::debug!(path = %p.display(), error = %e, "kv0: ignoring an unreadable view snapshot");
                None
            }
        });
        let (dataset, mut view, snapshot_version) = {
            let uri = uri.clone();
            let o = opts.clone();
            rt::block_on(async move {
                let ds = match o.version {
                    Some(v) => storage::open(&uri, &o.storage_options, Some(v)).await?,
                    None => {
                        storage::open_or_create(
                            &uri,
                            &o.storage_options,
                            store::SCHEMA.clone(),
                            o.create_if_missing && !o.read_only,
                            o.version_retention,
                        )
                        .await?
                    }
                };
                match loaded {
                    Some(l)
                        if l.view.lazy_threshold == o.lazy_value_threshold
                            && snapshot::still_valid(&l, &ds) =>
                    {
                        // Start from the snapshot; read only fragments committed since.
                        let known: HashSet<u64> = l.frags.iter().map(|f| f.0).collect();
                        let newer: HashSet<u64> = store::fragment_ids(&ds)
                            .difference(&known)
                            .copied()
                            .collect();
                        let mut view = l.view;
                        if !newer.is_empty() {
                            for row in
                                store::read_fragments(&ds, Some(&newer), o.lazy_value_threshold)
                                    .await?
                            {
                                view.apply_row(row);
                            }
                        }
                        Ok::<_, Error>((ds, view, Some(l.version)))
                    }
                    _ => {
                        let mut view = View::default();
                        view.lazy_threshold = o.lazy_value_threshold;
                        store::load_into(&ds, &mut view).await?;
                        Ok::<_, Error>((ds, view, None))
                    }
                }
            })?
        };
        let mut clock = Clock::default();
        clock.observe(view.max_seq);
        let applied = store::fragment_ids(&dataset);

        // Claim WALs of writers that are no longer running and replay what they did not commit.
        let use_wal = !read_only && opts.wal && !uri.starts_with("memory://");
        let mut pending = Vec::new();
        let mut log = None;
        let mut orphans = Vec::new();
        if use_wal {
            let root = opts.wal_dir.clone().unwrap_or_else(wal::default_root);
            let (mut w, found) = wal::Wal::open(&root, &uri, opts.wal_sync)?;
            for o in &found {
                if o.ops.is_empty() {
                    continue;
                }
                let committed = view.marks.get(o.writer_id.as_bytes()).copied().unwrap_or(0);
                // Ambiguous only if it holds an operation old enough that its mark (which is at
                // least that operation's sequence number, had it been committed) may have been
                // pruned. Newer operations are certainly not committed.
                let cutoff = now_nanos().saturating_sub(opts.mark_ttl.as_nanos() as u64);
                let stale = committed == 0 && o.ops.iter().any(|op| op.seq <= cutoff);
                if stale && !opts.replay_stale_wal {
                    return Err(Error::InvalidArgument(format!(
                        "the write-ahead log in {} is older than mark_ttl and its writer's mark has expired, so kv0 cannot tell which of its writes were committed. Open with replay_stale_wal=True to replay it (writes may be stored twice), or delete the directory to discard its uncommitted writes",
                        o.dir.display()
                    )));
                }
                let mut ops: Vec<Op> = o
                    .ops
                    .iter()
                    .filter(|op| op.seq > committed)
                    .cloned()
                    .collect();
                if ops.is_empty() {
                    continue;
                }
                ops.sort_by_key(|op| op.seq);
                w.append(&ops)?;
                for op in &ops {
                    clock.observe(op.seq);
                    view.apply(op.clone());
                }
                pending.extend(ops);
            }
            if !pending.is_empty() {
                // Make the adopted writes durable in this writer's WAL before deleting theirs.
                w.rotate()?;
            }
            orphans = found;
            log = Some(w);
        }
        let pending_bytes = pending.iter().map(Op::size).sum();
        let now = Instant::now();
        let inner = Arc::new(Inner {
            uri,
            read_only,
            closed: AtomicBool::new(false),
            state: RwLock::new(State {
                view,
                reader: dataset.clone(),
                pending,
                pending_bytes,
                clock,
                last_flush: now,
                last_refresh: now,
            }),
            version: AtomicU64::new(dataset.version().version),
            fragments: AtomicU64::new(dataset.fragments().len() as u64),
            stored_rows: AtomicU64::new(store::physical_rows(&dataset) as u64),
            persist: Mutex::new(Persist { dataset, applied }),
            wal: Mutex::new(log),
            abandoned: AtomicBool::new(false),
            flushing: AtomicBool::new(false),
            cache: Mutex::new(cache::ValueCache::new(opts.value_cache_size)),
            flights: Mutex::new(std::collections::HashMap::new()),
            snapshot_path,
            snapshot_version: AtomicU64::new(snapshot_version.unwrap_or(0)),
            from_snapshot: snapshot_version.is_some(),
            opts,
        });
        for o in orphans {
            o.remove();
        }
        if inner.state.read().pending.is_empty() {
            // Nothing replayed.
        } else if let Err(e) = inner.flush() {
            // The replayed writes stay in this writer's WAL and buffer.
            tracing::warn!(uri = %inner.uri, error = %e, "kv0: committing replayed WAL writes failed");
        }
        let needs_tick = inner.opts.flush_interval.is_some()
            || (inner.wal.lock().is_some() && inner.opts.wal_sync == WalSync::Batch);
        if !read_only && needs_tick {
            let weak: Weak<dyn Background> = Arc::downgrade(&inner) as Weak<dyn Background>;
            background::register(weak);
        }
        Ok(Db { inner })
    }

    /// The normalised location of the database.
    pub fn uri(&self) -> &str {
        &self.inner.uri
    }

    pub fn is_read_only(&self) -> bool {
        self.inner.read_only
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.check_open()?;
        self.inner.maybe_refresh();
        let (v, reader) = {
            let st = self.inner.state.read();
            match st.view.get(key) {
                Some(Val::Here(v)) => return Ok(Some(v)),
                None => return Ok(None),
                Some(v) => (v, st.reader.clone()),
            }
        };
        let key = Bytes::copy_from_slice(key);
        Ok(self
            .inner
            .values(&[key], vec![Some(v)], reader)?
            .pop()
            .flatten())
    }

    pub fn multi_get<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Vec<Option<Bytes>>> {
        self.inner.check_open()?;
        self.inner.maybe_refresh();
        let (vals, reader) = {
            let st = self.inner.state.read();
            let vals: Vec<Option<Val>> = keys.iter().map(|k| st.view.get(k.as_ref())).collect();
            (vals, st.reader.clone())
        };
        if vals.iter().all(|v| !matches!(v, Some(Val::Stored { .. }))) {
            return Ok(vals
                .into_iter()
                .map(|v| match v {
                    Some(Val::Here(b)) => Some(b),
                    _ => None,
                })
                .collect());
        }
        let keys: Vec<Bytes> = keys
            .iter()
            .map(|k| Bytes::copy_from_slice(k.as_ref()))
            .collect();
        self.inner.values(&keys, vals, reader)
    }

    /// Applies a batch with per-write options (see [`WriteOptions`]).
    pub fn write_opt(&self, batch: WriteBatch, opts: &WriteOptions) -> Result<()> {
        if !batch.is_empty() {
            self.apply_with(batch.ops, !opts.disable_wal)?;
        }
        if opts.sync {
            self.flush()?;
        }
        Ok(())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.apply(vec![(
            OP_PUT,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
        )])
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.apply(vec![(OP_DELETE, Bytes::copy_from_slice(key), None)])
    }

    /// Deletes keys in `[begin, end)`.
    pub fn delete_range(&self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.apply(vec![(
            OP_DELETE_RANGE,
            Bytes::copy_from_slice(begin),
            Some(Bytes::copy_from_slice(end)),
        )])
    }

    /// Applies a batch atomically: readers see all of it or none of it, and it is stored in a
    /// single commit.
    pub fn write(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        self.apply(batch.ops)
    }

    fn apply(&self, ops: Vec<(u8, Bytes, Option<Bytes>)>) -> Result<()> {
        self.apply_with(ops, true)
    }

    fn apply_with(&self, ops: Vec<(u8, Bytes, Option<Bytes>)>, log: bool) -> Result<()> {
        self.inner.check_open()?;
        if self.inner.read_only {
            return Err(Error::NotSupported("the database is open read-only".into()));
        }
        let full = {
            let mut st = self.inner.state.write();
            let ops: Vec<Op> = ops
                .into_iter()
                .map(|(kind, key, value)| Op {
                    seq: st.clock.next(),
                    kind,
                    key,
                    value,
                })
                .collect();
            // Logged before it is visible or acknowledged; a failed WAL write is not applied.
            if log {
                if let Some(w) = self.inner.wal.lock().as_mut() {
                    w.append(&ops)?;
                }
            }
            for op in ops {
                st.pending_bytes += op.size();
                st.pending.push(op.clone());
                st.view.apply(op);
            }
            st.pending_bytes >= self.inner.opts.write_buffer_size
        };
        if full {
            self.inner.flush()?;
        }
        Ok(())
    }

    /// Returns an unpositioned iterator; call one of the `seek` methods first.
    pub fn iter(&self) -> DbIterator {
        DbIterator {
            db: self.inner.clone(),
            current: None,
            buffer: Default::default(),
            resume_after: None,
            exhausted: true,
        }
    }

    /// Commits buffered writes to storage.
    pub fn flush(&self) -> Result<()> {
        self.inner.check_open()?;
        self.inner.flush()
    }

    /// Whether this handle started from a view snapshot (diagnostics and tests).
    #[doc(hidden)]
    pub fn opened_from_snapshot(&self) -> bool {
        self.inner.from_snapshot
    }

    /// Live values kept in Lance rather than in memory (diagnostics and tests).
    #[doc(hidden)]
    pub fn stored_values(&self) -> usize {
        self.inner.state.read().view.stored_count()
    }

    /// Picks up other writers' commits now. Returns whether anything changed.
    pub fn refresh(&self) -> Result<bool> {
        self.inner.check_open()?;
        self.inner.refresh(true)
    }

    /// Flushes, then rewrites the dataset as the current live view in one commit.
    pub fn compact(&self) -> Result<()> {
        self.inner.check_open()?;
        if self.inner.read_only {
            return Ok(());
        }
        // Compaction must not write unflushed operations (they would be committed twice), so
        // it runs right after a flush that left nothing pending.
        for _ in 0..8 {
            self.inner.flush()?;
            if self.inner.compact()? {
                return Ok(());
            }
        }
        Err(Error::Conflict(
            "writes kept arriving; compaction did not find a quiet moment".into(),
        ))
    }

    /// Drops the handle without flushing, as a crash would; buffered writes stay in the WAL.
    #[doc(hidden)]
    pub fn abandon(self) {
        self.inner.abandoned.store(true, Ordering::Release);
        self.inner.closed.store(true, Ordering::Release);
        self.inner.wal.lock().take();
    }

    /// The version this handle has read.
    pub fn version(&self) -> u64 {
        self.inner.version.load(Ordering::Acquire)
    }

    /// Every version still stored, oldest first.
    pub fn versions(&self) -> Result<Vec<VersionInfo>> {
        let ds = self.inner.persist.lock().dataset.clone();
        let versions = rt::block_on(async move { ds.versions().await })?;
        Ok(versions
            .into_iter()
            .map(|v| VersionInfo {
                version: v.version,
                timestamp_micros: v.timestamp.timestamp_micros(),
            })
            .collect())
    }

    /// Deletes versions (and the files only they use) older than `older_than`. The latest
    /// version and tagged versions are always kept. Returns the number of bytes removed.
    ///
    /// Other handles read large values from the version they last loaded. A read whose files
    /// are gone refreshes and reads the value again, so this is safe, but each such read costs
    /// a refresh: keep `older_than` above the longest a reader goes without refreshing
    /// (`refresh_interval`, or the life of an iterator).
    pub fn cleanup_old_versions(&self, older_than: Duration) -> Result<u64> {
        self.inner.check_open()?;
        if self.inner.read_only {
            return Err(Error::NotSupported("the database is open read-only".into()));
        }
        let ds = self.inner.persist.lock().dataset.clone();
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| Error::InvalidArgument(e.to_string()))?;
        let stats = rt::block_on(async move {
            // Tagged versions are kept, and do not stop the cleanup of others.
            let policy = CleanupPolicyBuilder::default()
                .before_timestamp(cutoff)
                .error_if_tagged_old_versions(false)
                .build();
            lance::dataset::cleanup::cleanup_old_versions(&ds, policy).await
        })?;
        Ok(stats.bytes_removed)
    }

    /// Physically removes deleted and overwritten data: rewrites the dataset as the live view
    /// (as [`Db::compact`]), removes every version but the latest and tagged ones, and removes
    /// this machine's view snapshot of the database. Delete markers younger than
    /// `tombstone_grace` stay (they hold keys, not values). Fails, naming the tag, when a tag
    /// keeps a version holding a value that is not live now (deleted or overwritten since);
    /// with `force` such tags are deleted.
    /// Other handles holding older versions refresh when a read finds their files gone.
    pub fn purge(&self, force: bool) -> Result<crate::PurgeReport> {
        use crate::purge;
        self.inner.check_open()?;
        if self.inner.read_only {
            return Err(Error::NotSupported("the database is open read-only".into()));
        }
        let before: std::collections::HashSet<u64> = self
            .inner
            .persist
            .lock()
            .dataset
            .fragments()
            .iter()
            .map(|f| f.id)
            .collect();
        // Flushed first, so the tag check compares against everything written.
        self.inner.flush()?;
        let ds = self.inner.persist.lock().dataset.clone();
        let mut report = crate::PurgeReport::default();
        let pinned = self.pinned_tags(&ds)?;
        if !pinned.is_empty() {
            if !force {
                let names = pinned
                    .iter()
                    .map(|(n, v)| format!("{n:?} (version {v})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Error::InvalidArgument(format!(
                    "purge: tag {names} keeps a version holding keys that are no longer live (deleted or overwritten); delete the tag or purge with force"
                )));
            }
            for (tag, _) in &pinned {
                let (ds, tag2) = (ds.clone(), tag.clone());
                rt::block_on(async move { ds.tags().delete(&tag2).await })?;
                report.tags_deleted.push(tag.clone());
            }
        }
        // Only now change anything: a refused purge leaves the database as it was.
        self.compact()?;
        let ds = self.inner.persist.lock().dataset.clone();
        report.fragments_rewritten = before
            .iter()
            .filter(|id| !ds.fragments().iter().any(|f| f.id == **id))
            .count();
        let (v, b) = {
            let ds = ds.clone();
            rt::block_on(async move { purge::remove_old_versions(&ds).await })?
        };
        report.versions_removed = v;
        report.bytes_removed = b;
        // A write that overwrote a tagged key while this ran leaves a tag holding data purge
        // would remove; say so rather than report a complete erasure.
        self.inner.refresh(true)?;
        let ds = self.inner.persist.lock().dataset.clone();
        report.tags_left = self.pinned_tags(&ds)?.into_iter().map(|(t, _)| t).collect();
        // This machine's view snapshot may hold values of keys deleted since it was saved.
        let root = self
            .inner
            .opts
            .wal_dir
            .clone()
            .unwrap_or_else(wal::default_root);
        let path = snapshot::path(&root, &self.inner.uri);
        if std::fs::remove_file(&path).is_ok() {
            report.snapshots_removed += 1;
        }
        self.inner.snapshot_version.store(0, Ordering::Release);
        Ok(report)
    }

    /// Tags of `ds` on older versions holding a put that is not the live entry now (a deleted
    /// key, or a value overwritten since), with their versions.
    fn pinned_tags(&self, ds: &Dataset) -> Result<Vec<(String, u64)>> {
        let tags = {
            let ds = ds.clone();
            rt::block_on(async move { crate::purge::old_tags(&ds).await })?
        };
        let mut pinned = Vec::new();
        for (tag, v) in tags {
            let ds = ds.clone();
            let puts: Vec<(Bytes, u64)> = rt::block_on(async move {
                let old = ds.checkout_version(v).await?;
                let mut sc = old.scan();
                sc.project(&["seq", "key", "op"])?;
                sc.filter(&format!("op = {}", view::OP_PUT))?;
                let mut out = Vec::new();
                let mut st = sc.try_into_stream().await?;
                while let Some(b) = futures::TryStreamExt::try_next(&mut st).await? {
                    let k = b
                        .column_by_name("key")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::BinaryArray>())
                        .ok_or_else(|| Error::Corruption("key column".into()))?;
                    let s = b
                        .column_by_name("seq")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
                        .ok_or_else(|| Error::Corruption("seq column".into()))?;
                    for i in 0..b.num_rows() {
                        out.push((Bytes::copy_from_slice(k.value(i)), s.value(i)));
                    }
                }
                Ok::<_, Error>(out)
            })?;
            let st = self.inner.state.read();
            if puts.iter().any(|(k, s)| st.view.live_seq(k) != Some(*s)) {
                pinned.push((tag, v));
            }
        }
        Ok(pinned)
    }

    /// Names a version (the one this handle has read when `None`), so it can be opened later
    /// with `Options::version` (see [`Db::tag_version`]) and is kept by version cleanup.
    pub fn create_tag(&self, tag: &str, version: Option<u64>) -> Result<()> {
        self.inner.check_open()?;
        let ds = self.inner.persist.lock().dataset.clone();
        let v = version.unwrap_or(ds.version().version);
        let tag = tag.to_string();
        rt::block_on(async move { crate::storage::tag_version(&ds, &tag, v).await })
    }

    /// Tags and the versions they name, sorted by name.
    pub fn tags(&self) -> Result<Vec<(String, u64)>> {
        let ds = self.inner.persist.lock().dataset.clone();
        let mut out: Vec<(String, u64)> = rt::block_on(async move { ds.tags().list().await })?
            .into_iter()
            .map(|(name, c)| (name, c.version))
            .collect();
        out.sort();
        Ok(out)
    }

    /// The version a tag names.
    pub fn tag_version(&self, tag: &str) -> Result<u64> {
        let ds = self.inner.persist.lock().dataset.clone();
        let tag = tag.to_string();
        Ok(rt::block_on(
            async move { ds.tags().get_version(&tag).await },
        )?)
    }

    /// Removes a tag.
    pub fn delete_tag(&self, tag: &str) -> Result<()> {
        self.inner.check_open()?;
        let ds = self.inner.persist.lock().dataset.clone();
        let tag = tag.to_string();
        rt::block_on(async move { ds.tags().delete(&tag).await })?;
        Ok(())
    }

    /// Counters describing the handle. Never waits for a commit in progress.
    pub fn stats(&self) -> Stats {
        let i = &self.inner;
        let st = i.state.read();
        Stats {
            version: i.version.load(Ordering::Acquire),
            live_keys: st.view.live,
            pending_ops: st.pending.len(),
            pending_bytes: st.pending_bytes,
            fragments: i.fragments.load(Ordering::Acquire) as usize,
            stored_rows: i.stored_rows.load(Ordering::Acquire) as usize,
            view_entries: st.view.entries(),
        }
    }

    /// Flushes and closes the handle. Later calls on any clone fail.
    pub fn close(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let r = if self.inner.read_only {
            Ok(())
        } else {
            self.inner.flush()
        };
        self.inner.closed.store(true, Ordering::Release);
        if r.is_ok() {
            if let Some(w) = self.inner.wal.lock().take() {
                w.remove();
            }
            self.inner.save_snapshot();
        }
        r
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
}

impl Inner {
    /// Makes `ds`'s version and sizes readable without the `persist` lock.
    fn publish(&self, ds: &Dataset) {
        self.fragments
            .store(ds.fragments().len() as u64, Ordering::Release);
        self.stored_rows
            .store(store::physical_rows(ds) as u64, Ordering::Release);
        self.version.store(ds.version().version, Ordering::Release);
    }

    fn check_open(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            Err(Error::InvalidArgument("the database is closed".into()))
        } else {
            Ok(())
        }
    }

    fn flush(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let mut p = self.persist.lock();
        let (ops, writer) = {
            let mut st = self.state.write();
            st.last_flush = Instant::now();
            if st.pending.is_empty() {
                return Ok(());
            }
            // Seal the WAL segment with the operations taken, so the sealed segments hold
            // exactly what this commit covers.
            let mut w = self.wal.lock();
            if let Some(w) = w.as_mut() {
                w.rotate()?;
            }
            st.pending_bytes = 0;
            (
                std::mem::take(&mut st.pending),
                w.as_ref().map(|w| w.writer_id().to_string()),
            )
        };
        let before = p.dataset.version().version;
        let ds = p.dataset.clone();
        let applied = p.applied.clone();
        let o = self.opts.storage_options.clone();
        let own_mark = writer.as_ref().map(|w| {
            let max = ops.iter().map(|o| o.seq).max().unwrap_or(0);
            view::mark(Bytes::from(w.clone().into_bytes()), max)
        });
        let attempt = {
            let ops = ops.clone();
            let lazy = self.opts.lazy_value_threshold;
            // Only a commit holding large values moves anything out of memory.
            let relocate = lazy > 0
                && ops
                    .iter()
                    .any(|o| o.kind == OP_PUT && o.value.as_ref().is_some_and(|v| v.len() > lazy));
            rt::block_on(async move {
                let after = store::append(ds, ops, o, writer).await?;
                let u = store::unseen(before, &after, &applied, true, lazy, relocate).await?;
                Ok::<_, Error>((after, u))
            })
        };
        match attempt {
            Ok((after, u)) => {
                if let Some(m) = own_mark {
                    // This handle does not re-read its own fragment; record its mark directly
                    // so compaction carries it forward.
                    self.state.write().view.apply(m);
                }
                if let Some(w) = self.wal.lock().as_mut() {
                    w.committed();
                }
                self.publish(&after);
                self.take_in(&mut self.state.write(), &after, u.rows, u.moved);
                p.dataset = after;
                p.applied = u.ids;
                // The writes are committed; a failed automatic compaction (for example one that
                // lost a race with another writer's compaction) is retried after a later flush.
                if let Err(e) = self.maybe_compact(&mut p) {
                    tracing::debug!(uri = %self.uri, error = %e, "kv0: compaction skipped");
                }
                Ok(())
            }
            Err(e) => {
                // Keep the writes for the next attempt.
                let mut st = self.state.write();
                let newer = std::mem::take(&mut st.pending);
                st.pending_bytes = ops.iter().chain(newer.iter()).map(Op::size).sum();
                st.pending = ops;
                st.pending.extend(newer);
                Err(e)
            }
        }
    }

    fn maybe_compact(&self, p: &mut Persist) -> Result<()> {
        let max = self.opts.max_fragments;
        // Only fragments below Lance's compaction target can be merged, so only they count;
        // otherwise a large settled dataset would trigger a compaction on every flush.
        let target = CompactionOptions::default().target_rows_per_fragment;
        let small = p
            .dataset
            .fragments()
            .iter()
            .filter(|f| f.physical_rows.unwrap_or(0) < target)
            .count();
        if max == 0 || small <= max {
            return Ok(());
        }
        let stored = store::physical_rows(&p.dataset);
        let live = self.state.read().view.live;
        if stored > 2 * live + 1024 {
            // Mostly overwritten or deleted rows: materialise the view (skipped while writes are
            // pending; a later flush tries again).
            self.compact_locked(p).map(|_| ())
        } else {
            // Mostly live rows in many small fragments: merge files only.
            let before = p.dataset.version().version;
            let mut ds = p.dataset.clone();
            let applied = p.applied.clone();
            let lazy = self.opts.lazy_value_threshold;
            let relocate = self.state.read().view.stored_count() > 0;
            let (ds, u) = rt::block_on(async move {
                compact_files(&mut ds, CompactionOptions::default(), None).await?;
                let u = store::unseen(before, &ds, &applied, true, lazy, relocate).await?;
                Ok::<_, Error>((ds, u))
            })?;
            self.absorb(p, ds, u);
            Ok(())
        }
    }

    /// Returns `false` when writes were pending and nothing was compacted.
    fn compact(&self) -> Result<bool> {
        let mut p = self.persist.lock();
        self.compact_locked(&mut p)
    }

    fn compact_locked(&self, p: &mut Persist) -> Result<bool> {
        let now = now_nanos();
        let keep_after = now.saturating_sub(self.opts.tombstone_grace.as_nanos() as u64);
        let marks_after = now.saturating_sub(self.opts.mark_ttl.as_nanos() as u64);
        let snapshot = {
            let st = self.state.read();
            if !st.pending.is_empty() {
                return Ok(false);
            }
            st.view.snapshot(
                keep_after,
                marks_after,
                store::REWRITE_BATCH_ROWS,
                store::REWRITE_BATCH_BYTES,
            )
        };
        let before = p.dataset.version().version;
        let ds = p.dataset.clone();
        let applied = p.applied.clone();
        let o = self.opts.storage_options.clone();
        let lazy = self.opts.lazy_value_threshold;
        let relocate = snapshot.iter().any(|c| !c.stored.is_empty());
        let (ds, u) = rt::block_on(async move {
            let after = store::replace(ds, snapshot, o).await?;
            let u = store::unseen(before, &after, &applied, true, lazy, relocate).await?;
            Ok::<_, Error>((after, u))
        })?;
        self.absorb(p, ds, u);
        self.state.write().view.prune(keep_after, marks_after);
        Ok(true)
    }

    fn absorb(&self, p: &mut Persist, ds: Dataset, u: store::Unseen) {
        self.publish(&ds);
        {
            let mut st = self.state.write();
            st.last_refresh = Instant::now();
            self.take_in(&mut st, &ds, u.rows, u.moved);
        }
        p.dataset = ds;
        p.applied = u.ids;
    }

    /// Saves the view for the next open, when it is large enough, fully committed and newer
    /// than the snapshot on disk. Failures are logged; the next open scans instead.
    fn save_snapshot(&self) {
        let Some(path) = &self.snapshot_path else {
            return;
        };
        let st = self.state.read();
        if !st.pending.is_empty() || st.view.entries() < self.opts.snapshot_min_entries {
            return;
        }
        let version = st.reader.version().version;
        if version == self.snapshot_version.load(Ordering::Acquire) && path.exists() {
            return;
        }
        let frags = snapshot::fragments(&st.reader);
        match snapshot::write(path, version, &frags, &st.view) {
            Ok(()) => self.snapshot_version.store(version, Ordering::Release),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "kv0: saving the view snapshot failed")
            }
        }
    }

    /// Brings the view to `ds`: applies unseen rows, records where committed rows now live, and
    /// makes `ds` the version its row addresses refer to.
    fn take_in(
        &self,
        st: &mut State,
        ds: &Dataset,
        rows: Vec<view::Row>,
        moved: Vec<(Bytes, u64, u64)>,
    ) {
        let max = rows.iter().map(|r| r.op.seq).max().unwrap_or(0);
        for row in rows {
            st.view.apply_row(row);
        }
        for (key, seq, addr) in moved {
            st.view.relocate(&key, seq, addr);
        }
        st.clock.observe(max);
        st.reader = ds.clone();
    }

    /// Values of `keys` (whose view entries were `vals` at `reader`'s version). Values kept in
    /// Lance are read from `reader`; if their files are gone (another writer compacted and
    /// cleaned up old versions), the handle refreshes and reads them once more at the new
    /// version. `None` for a key that no longer exists by then.
    fn values(
        &self,
        keys: &[Bytes],
        vals: Vec<Option<Val>>,
        reader: Dataset,
    ) -> Result<Vec<Option<Bytes>>> {
        match self.resolve(&reader, &vals) {
            Ok(v) => return Ok(v),
            Err(e) if missing_file(&e) => {
                tracing::debug!(uri = %self.uri, error = %e, "kv0: stored values moved; refreshing");
            }
            Err(e) => return Err(e),
        }
        self.refresh(true)?;
        let (vals, reader) = {
            let st = self.state.read();
            let vals: Vec<Option<Val>> = keys.iter().map(|k| st.view.get(k)).collect();
            (vals, st.reader.clone())
        };
        self.resolve(&reader, &vals).map_err(|e| {
            Error::Storage(
                format!(
                    "a stored value could not be read ({e}); versions this handle still used were \
                     probably deleted by cleanup_old_versions with an age shorter than the \
                     handle's staleness"
                )
                .into(),
            )
        })
    }

    /// Values for `vals`, reading those kept in Lance from `reader` (one read for all misses).
    /// Misses another thread is already reading are waited for, not read again.
    fn resolve(&self, reader: &Dataset, vals: &[Option<Val>]) -> Result<Vec<Option<Bytes>>> {
        let mut out: Vec<Option<Bytes>> = Vec::with_capacity(vals.len());
        let mut mine: Vec<(usize, u64)> = Vec::new();
        let mut theirs: Vec<(usize, u64, Arc<Flight>)> = Vec::new();
        {
            let mut c = self.cache.lock();
            let mut flights = self.flights.lock();
            for (i, v) in vals.iter().enumerate() {
                match v {
                    None => out.push(None),
                    Some(Val::Here(b)) => out.push(Some(b.clone())),
                    Some(Val::Stored { addr, .. }) => match c.get(*addr) {
                        Some(b) => out.push(Some(b)),
                        None => {
                            out.push(None);
                            match flights.get(addr) {
                                Some(f) => theirs.push((i, *addr, f.clone())),
                                None => {
                                    flights.insert(*addr, Arc::new(Flight::default()));
                                    mine.push((i, *addr));
                                }
                            }
                        }
                    },
                }
            }
        }
        if !mine.is_empty() {
            // Completes this reader's flights however the read ends (an error, a short result or
            // a panic), so readers waiting on the same addresses never block for good.
            struct Land<'a>(&'a Inner, Vec<u64>);
            impl Drop for Land<'_> {
                fn drop(&mut self) {
                    let mut flights = self.0.flights.lock();
                    for addr in &self.1 {
                        if let Some(f) = flights.remove(addr) {
                            *f.done.lock() = true;
                            f.cv.notify_all();
                        }
                    }
                }
            }
            let addrs: Vec<u64> = mine.iter().map(|m| m.1).collect();
            let _land = Land(self, addrs.clone());
            let ds = reader.clone();
            let got = rt::block_on(async move { store::take_values(&ds, &addrs).await })?;
            if got.len() != mine.len() {
                return Err(Error::Corruption(format!(
                    "read {} values for {} row addresses",
                    got.len(),
                    mine.len()
                )));
            }
            let mut c = self.cache.lock();
            for ((i, addr), b) in mine.iter().zip(got) {
                c.put(*addr, b.clone());
                out[*i] = Some(b);
            }
        }
        for (i, addr, f) in theirs {
            {
                let mut d = f.done.lock();
                while !*d {
                    f.cv.wait(&mut d);
                }
            }
            // Usually cached now; read it here when the other reader failed or it was evicted.
            let hit = self.cache.lock().get(addr);
            out[i] = Some(match hit {
                Some(b) => b,
                None => {
                    let ds = reader.clone();
                    rt::block_on(async move { store::take_values(&ds, &[addr]).await })?
                        .pop()
                        .expect("one value")
                }
            });
        }
        Ok(out)
    }

    fn maybe_refresh(&self) {
        let Some(every) = self.opts.refresh_interval else {
            return;
        };
        if self.opts.version.is_some() || self.state.read().last_refresh.elapsed() < every {
            return;
        }
        if let Err(e) = self.refresh(false) {
            tracing::warn!(uri = %self.uri, error = %e, "kv0: refresh failed");
        }
    }

    fn refresh(&self, wait: bool) -> Result<bool> {
        if self.opts.version.is_some() {
            return Ok(false);
        }
        let mut p = if wait {
            self.persist.lock()
        } else {
            match self.persist.try_lock() {
                Some(p) => p,
                // A flush or compaction is running; it brings the view up to date anyway.
                None => return Ok(false),
            }
        };
        self.state.write().last_refresh = Instant::now();
        let before = p.dataset.version().version;
        let ds = p.dataset.clone();
        let applied = p.applied.clone();
        let threshold = self.opts.lazy_value_threshold;
        let res = rt::block_on(async move {
            if ds.latest_version_id().await? <= before {
                return Ok::<_, Error>(None);
            }
            let mut ds = ds;
            ds.checkout_latest().await?;
            let u = store::unseen(before, &ds, &applied, false, threshold, false).await?;
            Ok(Some((ds, u)))
        })?;
        match res {
            None => Ok(false),
            Some((ds, u)) => {
                self.absorb(&mut p, ds, u);
                Ok(true)
            }
        }
    }
}

impl Background for Inner {
    fn tick(&self, _now: Instant) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        if let Some(w) = self.wal.lock().as_mut() {
            w.sync_if_due();
        }
        let Some(every) = self.opts.flush_interval else {
            return false;
        };
        let st = self.state.read();
        !st.pending.is_empty() && st.last_flush.elapsed() >= every
    }

    fn flush_now(self: Arc<Self>) {
        if let Err(e) = self.flush() {
            tracing::warn!(uri = %self.uri, error = %e, "kv0: background flush failed");
        }
    }

    fn flushing(&self) -> &AtomicBool {
        &self.flushing
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) || self.abandoned.load(Ordering::Acquire) {
            return;
        }
        if self.read_only {
            self.save_snapshot();
            return;
        }
        match self.flush() {
            Ok(()) => {
                if let Some(w) = self.wal.lock().take() {
                    w.remove();
                }
                self.save_snapshot();
            }
            // The writes stay in the WAL and are replayed by the next writer on this machine.
            Err(e) => tracing::warn!(uri = %self.uri, error = %e, "kv0: flush on drop failed"),
        }
    }
}

/// Live entries fetched per refill of an iterator's buffer.
const ITER_CHUNK: usize = 256;

/// An ordered cursor over the database.
///
/// The cursor reads the view in chunks, so it is not a snapshot: writes after its position that
/// happen before the next chunk is fetched are seen.
pub struct DbIterator {
    db: Arc<Inner>,
    current: Option<(Bytes, Bytes)>,
    buffer: std::collections::VecDeque<(Bytes, Bytes)>,
    resume_after: Option<Bytes>,
    exhausted: bool,
}

impl DbIterator {
    fn fill(&mut self, from: Bound<&[u8]>) {
        if self.db.closed.load(Ordering::Acquire) {
            self.exhausted = true;
            return;
        }
        let (items, last, exhausted, reader) = {
            let st = self.db.state.read();
            let (items, last, exhausted) = st.view.scan_forward(from, ITER_CHUNK);
            let lazy = items.iter().any(|(_, v)| matches!(v, Val::Stored { .. }));
            (items, last, exhausted, lazy.then(|| st.reader.clone()))
        };
        match reader {
            None => self.buffer.extend(items.into_iter().map(|(k, v)| match v {
                Val::Here(v) => (k, v),
                Val::Stored { .. } => unreachable!("no stored values in this chunk"),
            })),
            Some(reader) => {
                let (keys, vals): (Vec<Bytes>, Vec<Val>) = items.into_iter().unzip();
                let vals = vals.into_iter().map(Some).collect();
                match self.db.values(&keys, vals, reader) {
                    // A key deleted while its value was re-read after a cleanup is skipped.
                    Ok(vals) => self.buffer.extend(
                        keys.into_iter()
                            .zip(vals)
                            .filter_map(|(k, v)| v.map(|v| (k, v))),
                    ),
                    Err(e) => {
                        // The iterator interface has no error path; end here and say why.
                        tracing::error!(uri = %self.db.uri, error = %e, "kv0: reading values for an iterator failed");
                        self.exhausted = true;
                        return;
                    }
                }
            }
        }
        if last.is_some() {
            self.resume_after = last;
        }
        self.exhausted = exhausted;
    }

    fn back(&self, upto: Bound<&[u8]>) -> Option<(Bytes, Bytes)> {
        let (k, v, reader) = {
            let st = self.db.state.read();
            let (k, v) = st.view.scan_back(upto)?;
            match v {
                Val::Here(v) => return Some((k, v)),
                v => (k, v, st.reader.clone()),
            }
        };
        match self
            .db
            .values(std::slice::from_ref(&k), vec![Some(v)], reader)
        {
            Ok(mut v) => v.pop().flatten().map(|v| (k, v)),
            Err(e) => {
                tracing::error!(uri = %self.db.uri, error = %e, "kv0: reading a value for an iterator failed");
                None
            }
        }
    }

    fn reset(&mut self) {
        self.db.maybe_refresh();
        self.buffer.clear();
        self.current = None;
        self.resume_after = None;
        self.exhausted = false;
    }

    fn advance(&mut self) {
        if self.buffer.is_empty() && !self.exhausted {
            match self.resume_after.clone() {
                Some(k) => self.fill(Bound::Excluded(&k[..])),
                None => self.exhausted = true,
            }
        }
        self.current = self.buffer.pop_front();
    }

    /// Positions at the first key.
    pub fn seek_to_first(&mut self) {
        self.reset();
        self.fill(Bound::Unbounded);
        self.advance();
    }

    /// Positions at the first key `>= key`.
    pub fn seek(&mut self, key: &[u8]) {
        self.reset();
        self.fill(Bound::Included(key));
        self.advance();
    }

    /// Positions at the last key `<= key`.
    pub fn seek_for_prev(&mut self, key: &[u8]) {
        self.reset();
        if self.db.closed.load(Ordering::Acquire) {
            return;
        }
        self.current = self.back(Bound::Included(key));
        self.resume_after = self.current.as_ref().map(|(k, _)| k.clone());
    }

    /// Positions at the last key.
    pub fn seek_to_last(&mut self) {
        self.reset();
        if self.db.closed.load(Ordering::Acquire) {
            return;
        }
        self.current = self.back(Bound::Unbounded);
        self.resume_after = self.current.as_ref().map(|(k, _)| k.clone());
    }

    /// The entry at the cursor, if it is valid.
    pub fn current(&self) -> Option<(&Bytes, &Bytes)> {
        self.current.as_ref().map(|(k, v)| (k, v))
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    /// Moves to the next entry.
    pub fn step(&mut self) {
        if self.current.is_some() {
            self.advance();
        }
    }
}

impl Iterator for DbIterator {
    type Item = (Bytes, Bytes);

    /// Returns the entry at the cursor, then advances.
    fn next(&mut self) -> Option<Self::Item> {
        let cur = self.current.take()?;
        self.advance();
        Some(cur)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_read_fails_and_releases_waiting_readers() {
        // Review: a single-flight leader whose read returns too few values must fail and still
        // complete its flights, or every later reader of those addresses blocks for good.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db").to_string_lossy().into_owned();
        let db = Db::open(
            &path,
            Options {
                flush_interval: None,
                lazy_value_threshold: 16,
                value_cache_size: 0,
                wal_dir: Some(dir.path().join("wal")),
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"a", &[1u8; 100]).unwrap();
        db.flush().unwrap();
        let uri = db.inner.state.read().reader.uri().to_owned();
        store::short_take_once(&uri);
        let e = db.get(b"a").unwrap_err();
        assert!(matches!(e, Error::Corruption(_)), "{e:?}");
        assert!(db.inner.flights.lock().is_empty());
        assert_eq!(db.get(b"a").unwrap().unwrap().len(), 100);
    }

    #[test]
    fn a_deleted_data_file_is_reported_as_not_found() {
        // The refresh-and-retry of moved values keys off `Error::NotFound`; this pins the
        // conversion from Lance's error to that variant, independent of its wording.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Db::open(
            path.to_str().unwrap(),
            Options {
                flush_interval: None,
                lazy_value_threshold: 16,
                value_cache_size: 0,
                wal_dir: Some(dir.path().join("wal")),
                ..Options::default()
            },
        )
        .unwrap();
        db.put(b"a", &[1u8; 100]).unwrap();
        db.flush().unwrap();
        for f in std::fs::read_dir(path.join("data")).unwrap() {
            std::fs::remove_file(f.unwrap().path()).unwrap();
        }
        let reader = db.inner.state.read().reader.clone();
        let addr = match db.inner.state.read().view.get(b"a") {
            Some(Val::Stored { addr, .. }) => addr,
            other => panic!("not stored: {other:?}"),
        };
        let e =
            rt::block_on(async move { store::take_values(&reader, &[addr]).await }).unwrap_err();
        assert!(matches!(e, Error::NotFound(_)), "{e:?}");
        assert!(missing_file(&e));
    }
}
