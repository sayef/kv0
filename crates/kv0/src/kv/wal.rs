//! Local write-ahead log for the key-value layer.
//!
//! Every write is appended to a local file before it is acknowledged, so buffered writes survive
//! a crash of the process; Lance commits stay batched. Layout:
//!
//! ```text
//! <root>/<xxh3 of the dataset URI>/uri                 the dataset URI, for people
//! <root>/<xxh3 of the dataset URI>/<writer id>/lock    held (flock) while the writer is open
//! <root>/<xxh3 of the dataset URI>/<writer id>/<n>-<created ms>.wal
//! ```
//!
//! A segment starts with an 8-byte magic. Each record is one `write` call (a single operation or
//! a whole batch): `len: u32 | xxh3-64 of payload: u64 | payload`, little-endian. On replay a
//! record cut off by the end of the file is a torn tail from a crash and is dropped; a checksum
//! mismatch anywhere else is corruption and fails the open.
//!
//! A flush seals the current segment and opens the next, so the sealed segments hold exactly the
//! operations being committed. After the Lance commit succeeds they are deleted. The committed
//! fragment also holds a mark row with the writer id and its highest committed sequence number;
//! compaction carries marks forward, so a replay after a crash between the commit and the delete
//! skips what was already committed, however long ago that was.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::view::Op;
use crate::error::{Error, Result};

const MAGIC: &[u8; 8] = b"LWWAL001";
const HEADER: usize = 12;

/// When WAL writes reach the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WalSync {
    /// Write to the OS on every call, never fsync: survives a process crash, not a power loss.
    None,
    /// Write on every call, fsync in the background about every 100 ms (default).
    #[default]
    Batch,
    /// fsync before every write call returns.
    Always,
}

impl WalSync {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(WalSync::None),
            "batch" => Ok(WalSync::Batch),
            "always" => Ok(WalSync::Always),
            other => Err(Error::InvalidArgument(format!(
                "wal_sync must be \"none\", \"batch\" or \"always\", not {other:?}"
            ))),
        }
    }
}

/// The WAL root: `KV0_WAL_DIR`, else `<platform cache dir>/kv0/wal`.
pub fn default_root() -> PathBuf {
    if let Some(d) = std::env::var_os("KV0_WAL_DIR").filter(|d| !d.is_empty()) {
        return PathBuf::from(d);
    }
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let cache = if cfg!(target_os = "macos") {
        home.map(|h| h.join("Library/Caches"))
    } else if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .filter(|d| !d.is_empty())
            .map(PathBuf::from)
            .or_else(|| home.map(|h| h.join(".cache")))
    };
    cache
        .unwrap_or_else(std::env::temp_dir)
        .join("kv0")
        .join("wal")
}

/// The per-dataset WAL directory under `root`.
pub fn dataset_dir(root: &Path, uri: &str) -> PathBuf {
    root.join(format!(
        "{:016x}",
        xxhash_rust::xxh3::xxh3_64(uri.as_bytes())
    ))
}

fn io(path: &Path, e: std::io::Error) -> Error {
    Error::Io(format!("WAL {}: {e}", path.display()))
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn encode(ops: &[Op]) -> Vec<u8> {
    let size: usize = ops
        .iter()
        .map(|o| 18 + o.key.len() + o.value.as_ref().map_or(0, |v| 4 + v.len()))
        .sum();
    let mut p = Vec::with_capacity(4 + size);
    p.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for o in ops {
        p.extend_from_slice(&o.seq.to_le_bytes());
        p.push(o.kind);
        p.extend_from_slice(&(o.key.len() as u32).to_le_bytes());
        p.extend_from_slice(&o.key);
        match &o.value {
            Some(v) => {
                p.push(1);
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                p.extend_from_slice(v);
            }
            None => p.push(0),
        }
    }
    let mut rec = Vec::with_capacity(HEADER + p.len());
    rec.extend_from_slice(&(p.len() as u32).to_le_bytes());
    rec.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(&p).to_le_bytes());
    rec.extend_from_slice(&p);
    rec
}

fn decode(p: &[u8]) -> Option<Vec<Op>> {
    fn take<'a>(p: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if p.len() < n {
            return None;
        }
        let (a, b) = p.split_at(n);
        *p = b;
        Some(a)
    }
    let mut p = p;
    let n = u32::from_le_bytes(take(&mut p, 4)?.try_into().ok()?);
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let seq = u64::from_le_bytes(take(&mut p, 8)?.try_into().ok()?);
        let kind = take(&mut p, 1)?[0];
        let klen = u32::from_le_bytes(take(&mut p, 4)?.try_into().ok()?) as usize;
        let key = Bytes::copy_from_slice(take(&mut p, klen)?);
        let value = match take(&mut p, 1)?[0] {
            0 => None,
            _ => {
                let vlen = u32::from_le_bytes(take(&mut p, 4)?.try_into().ok()?) as usize;
                Some(Bytes::copy_from_slice(take(&mut p, vlen)?))
            }
        };
        out.push(Op {
            seq,
            kind,
            key,
            value,
        });
    }
    p.is_empty().then_some(out)
}

/// Reads a segment. A torn tail is dropped (and cut from the file); corruption before the last
/// record is an error.
pub(crate) fn read_segment(path: &Path) -> Result<Vec<Op>> {
    let mut buf = Vec::new();
    File::open(path)
        .and_then(|mut f| f.read_to_end(&mut buf))
        .map_err(|e| io(path, e))?;
    if buf.len() < MAGIC.len() {
        // Crashed while creating the file.
        return Ok(Vec::new());
    }
    if &buf[..MAGIC.len()] != MAGIC {
        return Err(Error::Corruption(format!(
            "WAL segment {} has no kv0 header",
            path.display()
        )));
    }
    let mut ops = Vec::new();
    let mut pos = MAGIC.len();
    while pos < buf.len() {
        let rest = buf.len() - pos;
        if rest < HEADER {
            break; // torn header
        }
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        let sum = u64::from_le_bytes(buf[pos + 4..pos + 12].try_into().unwrap());
        let end = pos + HEADER + len;
        if end > buf.len() {
            break; // torn payload
        }
        let payload = &buf[pos + HEADER..end];
        let decoded = (xxhash_rust::xxh3::xxh3_64(payload) == sum)
            .then(|| decode(payload))
            .flatten();
        match decoded {
            Some(rec) => ops.extend(rec),
            None if end == buf.len() => break, // the last record is torn
            None => {
                return Err(Error::Corruption(format!(
                    "WAL segment {} is corrupt at byte {pos}; move the file away to open the database without it",
                    path.display()
                )))
            }
        }
        pos = end;
    }
    if pos < buf.len() {
        tracing::warn!(path = %path.display(), dropped = buf.len() - pos, "kv0: dropped a torn WAL tail");
        if let Ok(f) = OpenOptions::new().write(true).open(path) {
            let _ = f.set_len(pos as u64);
        }
    }
    Ok(ops)
}

/// Segments of a writer directory, oldest first, with their creation time in ms.
fn segments(dir: &Path) -> Result<Vec<(u64, u128, PathBuf)>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir).map_err(|e| io(dir, e))? {
        let p = e.map_err(|e| io(dir, e))?.path();
        let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".wal") else {
            continue;
        };
        let mut it = stem.splitn(2, '-');
        if let (Some(Ok(n)), Some(Ok(t))) = (
            it.next().map(str::parse::<u64>),
            it.next().map(str::parse::<u128>),
        ) {
            out.push((n, t, p));
        }
    }
    out.sort_by_key(|s| s.0);
    Ok(out)
}

/// A WAL left behind by a writer that is no longer running.
pub(crate) struct Orphan {
    pub dir: PathBuf,
    pub writer_id: String,
    pub ops: Vec<Op>,
    /// Its segment files, deleted once its operations are safe elsewhere.
    segments: Vec<PathBuf>,
    /// This handle took the directory over (and its writer id); only the old segments go.
    adopted: bool,
    _lock: Option<File>,
}

impl Orphan {
    /// Call after the operations that were not committed are in this handle's WAL.
    pub fn remove(self) {
        if self.adopted {
            for p in &self.segments {
                let _ = fs::remove_file(p);
            }
        } else {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

pub(crate) struct Wal {
    dir: PathBuf,
    writer_id: String,
    mode: WalSync,
    _lock: File,
    current: File,
    current_path: PathBuf,
    next_no: u64,
    /// Sealed segments whose operations are not committed yet.
    sealed: Vec<PathBuf>,
    dirty: bool,
    last_sync: Instant,
    /// Length of the current segment up to its last complete record.
    offset: u64,
    /// Set when a failed append could not be undone; appends fail until a rotation succeeds.
    poisoned: bool,
    #[cfg(test)]
    inject: Inject,
}

/// Failures to inject in unit tests.
#[cfg(test)]
#[derive(Default)]
struct Inject {
    /// The next append writes this many bytes of its record, then fails.
    short_write: Option<usize>,
    /// Truncating after a failed append fails.
    set_len_fails: bool,
    /// Starting a new segment fails.
    rotate_fails: bool,
}

/// fsync interval for [`WalSync::Batch`].
pub(crate) const BATCH_SYNC: Duration = Duration::from_millis(100);

impl Wal {
    /// Creates this writer's WAL and claims WALs of writers that are no longer running.
    pub fn open(root: &Path, uri: &str, mode: WalSync) -> Result<(Wal, Vec<Orphan>)> {
        let base = dataset_dir(root, uri);
        fs::create_dir_all(&base).map_err(|e| io(&base, e))?;
        let _ = fs::write(base.join("uri"), uri);
        let mut orphans = Vec::new();
        for e in fs::read_dir(&base).map_err(|e| io(&base, e))? {
            let dir = e.map_err(|e| io(&base, e))?.path();
            if !dir.is_dir() {
                continue;
            }
            let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with('.') && name.ends_with(".tmp") {
                // A crashed open's half-made directory. Its lock file only ever appears already
                // locked (see below), and a young directory may still be mid-rename: remove only
                // an old one whose lock this opener can take.
                let old = fs::metadata(&dir)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| t.elapsed().ok())
                    .is_some_and(|age| age > Duration::from_secs(60));
                if old {
                    // A crash between creating `lock.new` and renaming it can leave either file,
                    // or neither; remove the directory when no live opener holds one.
                    let held = ["lock", "lock.new"].iter().any(|f| {
                        OpenOptions::new()
                            .write(true)
                            .open(dir.join(f))
                            .is_ok_and(|l| l.try_lock().is_err())
                    });
                    if !held {
                        let _ = fs::remove_dir_all(&dir);
                    }
                }
                continue;
            }
            // Never create the lock file: a directory another opener is deleting must not get
            // a new one.
            let lock_path = dir.join("lock");
            let Ok(lock) = OpenOptions::new().write(true).open(&lock_path) else {
                continue;
            };
            if lock.try_lock().is_err() {
                continue; // a live writer
            }
            if !dir.exists() {
                continue; // claimed and removed by another opener
            }
            let segs = match segments(&dir) {
                Ok(s) => s,
                Err(_) if !dir.exists() => continue,
                Err(e) => return Err(e),
            };
            let mut ops = Vec::new();
            for (_, _, p) in &segs {
                ops.extend(read_segment(p)?);
            }
            orphans.push(Orphan {
                writer_id: dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default()
                    .to_string(),
                segments: segs.iter().map(|s| s.2.clone()).collect(),
                adopted: false,
                dir,
                ops,
                _lock: Some(lock),
            });
        }
        // Take over an idle writer directory, and its writer id, when there is one: each id
        // leaves one WAL mark row in the dataset, so reusing ids keeps their number at the
        // number of writers that ran at the same time instead of one per open.
        let mut next_no = 0;
        let adopted = orphans.first_mut().map(|o| {
            o.adopted = true;
            next_no = o
                .segments
                .iter()
                .filter_map(|p| {
                    p.file_name()?
                        .to_str()?
                        .split('-')
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .max()
                .map_or(0, |n| n + 1);
            (
                o.dir.clone(),
                o.writer_id.clone(),
                o._lock.take().expect("claimed"),
            )
        });
        let (dir, writer_id, lock) = match adopted {
            Some(a) => a,
            None => {
                // This writer's directory becomes visible only once it is locked, so a concurrent open
                // never mistakes it for an abandoned one.
                let writer_id = uuid::Uuid::now_v7().simple().to_string();
                let tmp = base.join(format!(".{writer_id}.tmp"));
                fs::create_dir_all(&tmp).map_err(|e| io(&tmp, e))?;
                // The lock file appears under its name already locked, so no scanner can lock
                // it first.
                let staging = tmp.join("lock.new");
                let lock = File::create(&staging).map_err(|e| io(&staging, e))?;
                lock.try_lock()
                    .map_err(|e| Error::Io(format!("WAL lock {}: {e}", staging.display())))?;
                let lock_path = tmp.join("lock");
                fs::rename(&staging, &lock_path).map_err(|e| io(&lock_path, e))?;
                let dir = base.join(&writer_id);
                fs::rename(&tmp, &dir).map_err(|e| io(&dir, e))?;

                (dir, writer_id, lock)
            }
        };
        let (current, current_path) = Self::create_segment(&dir, next_no)?;
        Ok((
            Wal {
                dir,
                writer_id,
                mode,
                _lock: lock,
                current,
                current_path,
                next_no: next_no + 1,
                sealed: Vec::new(),
                dirty: false,
                last_sync: Instant::now(),
                offset: MAGIC.len() as u64,
                poisoned: false,
                #[cfg(test)]
                inject: Inject::default(),
            },
            orphans,
        ))
    }

    fn create_segment(dir: &Path, n: u64) -> Result<(File, PathBuf)> {
        let path = dir.join(format!("{n:020}-{}.wal", now_ms()));
        let mut f = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        f.write_all(MAGIC).map_err(|e| io(&path, e))?;
        Ok((f, path))
    }

    pub fn writer_id(&self) -> &str {
        &self.writer_id
    }

    /// Appends one record. Returns once it is written (and synced, in [`WalSync::Always`]).
    ///
    /// On failure the segment is cut back to its last complete record, so a partial record can
    /// never sit in front of later acknowledged ones (replay would take it for a torn tail and
    /// drop them). If that cut fails the WAL moves to a new segment, and if that fails too it
    /// refuses writes until a flush rotates it successfully.
    pub fn append(&mut self, ops: &[Op]) -> Result<()> {
        if self.poisoned {
            return Err(Error::Io(format!(
                "WAL {} is unusable after a failed write; flush or reopen",
                self.current_path.display()
            )));
        }
        let rec = encode(ops);
        #[cfg(test)]
        if let Some(n) = self.inject.short_write.take() {
            let _ = self.current.write_all(&rec[..n.min(rec.len())]);
            let e = std::io::Error::other("injected short write");
            if (self.inject.set_len_fails || self.current.set_len(self.offset).is_err())
                && self.rotate().is_err()
            {
                self.poisoned = true;
            }
            return Err(io(&self.current_path, e));
        }
        let written = self.current.write_all(&rec).and_then(|()| {
            if self.mode == WalSync::Always {
                self.current.sync_data()
            } else {
                Ok(())
            }
        });
        match written {
            Ok(()) => {
                self.offset += rec.len() as u64;
                if self.mode != WalSync::Always {
                    self.dirty = true;
                }
                Ok(())
            }
            Err(e) => {
                // The caller reports the write as failed, so it must not be replayed either.
                if self.current.set_len(self.offset).is_err() && self.rotate().is_err() {
                    self.poisoned = true;
                }
                Err(io(&self.current_path, e))
            }
        }
    }

    /// fsyncs the current segment if [`WalSync::Batch`] and the interval has passed.
    pub fn sync_if_due(&mut self) {
        if self.mode == WalSync::Batch && self.dirty && self.last_sync.elapsed() >= BATCH_SYNC {
            if let Err(e) = self.current.sync_data() {
                tracing::warn!(path = %self.current_path.display(), error = %e, "kv0: WAL fsync failed");
            }
            self.dirty = false;
            self.last_sync = Instant::now();
        }
    }

    /// Seals the current segment and starts a new one. The sealed segments hold exactly the
    /// operations taken for the next commit.
    pub fn rotate(&mut self) -> Result<()> {
        #[cfg(test)]
        if self.inject.rotate_fails {
            return Err(Error::Io("injected rotate failure".into()));
        }
        if self.mode == WalSync::Batch && self.dirty {
            let _ = self.current.sync_data();
        }
        let (f, p) = Self::create_segment(&self.dir, self.next_no)?;
        self.next_no += 1;
        let old = std::mem::replace(&mut self.current_path, p);
        self.current = f;
        self.dirty = false;
        self.offset = MAGIC.len() as u64;
        self.poisoned = false;
        self.sealed.push(old);
        Ok(())
    }

    /// Deletes the sealed segments after their operations were committed.
    pub fn committed(&mut self) {
        for p in self.sealed.drain(..) {
            if let Err(e) = fs::remove_file(&p) {
                tracing::warn!(path = %p.display(), error = %e, "kv0: could not delete a committed WAL segment");
            }
        }
    }

    /// After a clean close (everything committed): deletes the segments and keeps the
    /// directory, so the next open on this machine reuses the writer id.
    pub fn remove(self) {
        for p in self
            .sealed
            .iter()
            .chain(std::iter::once(&self.current_path))
        {
            let _ = fs::remove_file(p);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn op(seq: u64, k: &str) -> Op {
        Op {
            seq,
            kind: crate::kv::view::OP_PUT,
            key: Bytes::copy_from_slice(k.as_bytes()),
            value: Some(Bytes::from_static(b"v")),
        }
    }

    fn replay(root: &Path) -> Vec<String> {
        let mut keys = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        let mut files = Vec::new();
        while let Some(d) = stack.pop() {
            for e in fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "wal") {
                    files.push(p);
                }
            }
        }
        files.sort();
        for f in files {
            for o in read_segment(&f).unwrap() {
                keys.push(String::from_utf8(o.key.to_vec()).unwrap());
            }
        }
        keys
    }

    #[test]
    fn failed_append_truncate_fails_then_rotate_fails_then_recovers() {
        let root = tempfile::tempdir().unwrap();
        let (mut w, _) = Wal::open(root.path(), "memory://t", WalSync::None).unwrap();
        w.append(&[op(1, "a")]).unwrap();

        // The write fails part-way, the cut fails, and no new segment can be started.
        w.inject.short_write = Some(7);
        w.inject.set_len_fails = true;
        w.inject.rotate_fails = true;
        assert!(w.append(&[op(2, "b")]).is_err());
        assert!(
            w.poisoned,
            "no way to undo the partial record: refuse writes"
        );
        assert!(w.append(&[op(3, "c")]).is_err(), "poisoned");

        // A successful rotation (the next flush) clears it.
        w.inject.rotate_fails = false;
        w.rotate().unwrap();
        w.append(&[op(4, "d")]).unwrap();
        // The partial record is the torn tail of a sealed segment; nothing acknowledged after
        // it is lost.
        assert_eq!(replay(root.path()), vec!["a", "d"]);
    }

    #[test]
    fn stale_tmp_directories_without_a_lock_are_removed() {
        let root = tempfile::tempdir().unwrap();
        let base = dataset_dir(root.path(), "memory://t");
        let stale = base.join(".0123.tmp");
        fs::create_dir_all(&stale).unwrap();
        let old = std::time::SystemTime::now() - Duration::from_secs(600);
        File::open(&stale).unwrap().set_modified(old).unwrap();
        let fresh = base.join(".4567.tmp");
        fs::create_dir_all(&fresh).unwrap();
        let _w = Wal::open(root.path(), "memory://t", WalSync::None).unwrap();
        assert!(
            !stale.exists(),
            "an old, unlocked .tmp directory is cleaned up"
        );
        assert!(
            fresh.exists(),
            "a young one may belong to an opener still running"
        );
    }

    #[test]
    fn failed_append_with_failed_truncate_rotates() {
        let root = tempfile::tempdir().unwrap();
        let (mut w, _) = Wal::open(root.path(), "memory://t", WalSync::None).unwrap();
        w.append(&[op(1, "a")]).unwrap();
        w.inject.short_write = Some(9);
        w.inject.set_len_fails = true;
        assert!(w.append(&[op(2, "b")]).is_err());
        assert!(!w.poisoned);
        w.append(&[op(3, "c")]).unwrap();
        assert_eq!(replay(root.path()), vec!["a", "c"]);
    }

    #[test]
    fn failed_append_is_cut_back() {
        let root = tempfile::tempdir().unwrap();
        let (mut w, _) = Wal::open(root.path(), "memory://t", WalSync::None).unwrap();
        w.append(&[op(1, "a")]).unwrap();
        w.inject.short_write = Some(9);
        assert!(w.append(&[op(2, "b")]).is_err());
        w.append(&[op(3, "c")]).unwrap();
        assert_eq!(replay(root.path()), vec!["a", "c"]);
    }
}
