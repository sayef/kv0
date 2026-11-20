//! A local snapshot of a key-value view, so a reopen loads it instead of rescanning the log.
//!
//! Stored next to the database's WAL directories (`<wal root>/<xxh3 of the URI>/view.snap`):
//! `KV0VIEW3`, the log version, the log's identity ([`OpLog::log_id`]), each file's id and
//! identity ([`crate::storage::oplog::LogFile`]), the engine's row-address table
//! ([`OpLog::addr_table`]), the view (see [`View::write_to`]), then the xxh3-64 of everything
//! before it. Counts are little-endian `u64`; strings are a little-endian `u32` length and UTF-8
//! bytes. A snapshot is used only when the log has the same identity and every file it covers is
//! still in the log unchanged: compaction (which may drop tombstones) or a recreated log
//! invalidates it, and the open falls back to a full scan. Files committed since are read and
//! applied as usual.
//!
//! Version 2 (`KV0VIEW2`) described Lance fragments. It is not read any more: a moraine log
//! cannot match it, so such a file is ignored like any unreadable snapshot and rewritten after
//! the full scan.

use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use xxhash_rust::xxh3::Xxh3;

use super::view::View;
use crate::storage::oplog::{FileId, OpLog};

const MAGIC: &[u8; 8] = b"KV0VIEW3";

/// Longest string a snapshot holds (file ids, identities, address-table entries).
const MAX_STR: usize = 4096;

pub(crate) fn path(root: &Path, uri: &str) -> PathBuf {
    super::wal::dataset_dir(root, uri).join("view.snap")
}

struct Hashing<T> {
    inner: T,
    h: Xxh3,
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.h.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.h.update(&buf[..n]);
        Ok(n)
    }
}

fn bad(m: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, m.to_string())
}

fn put_str(w: &mut impl Write, s: &str) -> io::Result<()> {
    if s.len() > MAX_STR {
        return Err(bad("string too long for a snapshot"));
    }
    w.write_all(&(s.len() as u32).to_le_bytes())?;
    w.write_all(s.as_bytes())
}

fn get_str(r: &mut impl Read) -> io::Result<String> {
    let mut l = [0u8; 4];
    r.read_exact(&mut l)?;
    let len = u32::from_le_bytes(l) as usize;
    if len > MAX_STR {
        return Err(bad("string too long"));
    }
    let mut s = vec![0u8; len];
    r.read_exact(&mut s)?;
    String::from_utf8(s).map_err(|_| bad("string is not UTF-8"))
}

fn get_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut u = [0u8; 8];
    r.read_exact(&mut u)?;
    Ok(u64::from_le_bytes(u))
}

/// What a snapshot records about the log besides the view.
pub(crate) struct Saved<'a> {
    pub version: u64,
    pub log_id: String,
    pub frags: &'a [(FileId, String)],
    pub addr_table: Vec<String>,
}

/// Writes a snapshot of `view` atomically (a temporary file, then a rename).
pub(crate) fn write(path: &Path, saved: &Saved<'_>, view: &View) -> io::Result<()> {
    let dir = path.parent().ok_or_else(|| bad("no snapshot directory"))?;
    fs::create_dir_all(dir)?;
    remove_abandoned(dir);
    let tmp = dir.join(format!(
        "view.snap.{}-{}.tmp",
        std::process::id(),
        crate::clock::now_nanos()
    ));
    let res = (|| {
        let f = fs::File::create(&tmp)?;
        let mut w = Hashing {
            inner: BufWriter::with_capacity(1 << 20, f),
            h: Xxh3::new(),
        };
        w.write_all(MAGIC)?;
        w.write_all(&saved.version.to_le_bytes())?;
        put_str(&mut w, &saved.log_id)?;
        w.write_all(&(saved.frags.len() as u64).to_le_bytes())?;
        for (id, ident) in saved.frags {
            put_str(&mut w, &id.0)?;
            put_str(&mut w, ident)?;
        }
        w.write_all(&(saved.addr_table.len() as u64).to_le_bytes())?;
        for e in &saved.addr_table {
            put_str(&mut w, e)?;
        }
        view.write_to(&mut w)?;
        let digest = w.h.digest();
        let mut f = w.inner;
        f.write_all(&digest.to_le_bytes())?;
        // On disk before the rename, and the rename on disk before returning: a power loss
        // leaves the old snapshot or the new one, not a torn file under the final name.
        f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        fs::rename(&tmp, path)?;
        #[cfg(unix)]
        fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if res.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    res
}

/// Removes temporary snapshot files that a crashed process left over 10 minutes ago.
fn remove_abandoned(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        if !(name.starts_with("view.snap.") && name.ends_with(".tmp")) {
            continue;
        }
        let old = e
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age > std::time::Duration::from_secs(600));
        if old {
            let _ = fs::remove_file(e.path());
        }
    }
}

pub(crate) struct Loaded {
    pub version: u64,
    pub log_id: String,
    pub frags: Vec<(FileId, String)>,
    pub addr_table: Vec<String>,
    pub view: View,
}

/// Reads a snapshot; `Ok(None)` when there is none.
pub(crate) fn read(path: &Path) -> io::Result<Option<Loaded>> {
    let f = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut r = Hashing {
        inner: BufReader::with_capacity(1 << 20, f),
        h: Xxh3::new(),
    };
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(bad("not a kv0 view snapshot of this version"));
    }
    let version = get_u64(&mut r)?;
    let log_id = get_str(&mut r)?;
    let n = get_u64(&mut r)?;
    let mut frags = Vec::new();
    for _ in 0..n {
        let id = get_str(&mut r)?;
        let ident = get_str(&mut r)?;
        frags.push((FileId(id), ident));
    }
    let n = get_u64(&mut r)?;
    let mut addr_table = Vec::new();
    for _ in 0..n {
        addr_table.push(get_str(&mut r)?);
    }
    let view = View::read_from(&mut r)?;
    let digest = r.h.digest();
    let mut rest = Vec::new();
    r.inner.read_to_end(&mut rest)?;
    if rest.len() != 8 || u64::from_le_bytes(rest[..8].try_into().expect("8 bytes")) != digest {
        return Err(bad("checksum mismatch"));
    }
    Ok(Some(Loaded {
        version,
        log_id,
        frags,
        addr_table,
        view,
    }))
}

/// Whether a snapshot still describes `log`: the same log, and every file it covers present
/// and unchanged.
pub(crate) fn still_valid(l: &Loaded, log: &dyn OpLog) -> bool {
    if log.version() < l.version || log.log_id() != l.log_id {
        return false;
    }
    let now: std::collections::HashMap<FileId, String> =
        log.files().into_iter().map(|f| (f.id, f.ident)).collect();
    l.frags.iter().all(|(id, ident)| now.get(id) == Some(ident))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_put() -> View {
        let mut view = View::default();
        view.apply(crate::kv::view::Op {
            seq: 7,
            kind: crate::kv::view::OP_PUT,
            key: bytes::Bytes::from_static(b"k"),
            value: Some(bytes::Bytes::from_static(b"v")),
        });
        view
    }

    #[test]
    fn round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("view.snap");
        let saved = Saved {
            version: 4,
            log_id: "id".into(),
            frags: &[(FileId("part-1.parquet".into()), "part-1.parquet|10".into())],
            addr_table: vec!["part-1.parquet|10".into()],
        };
        write(&p, &saved, &one_put()).unwrap();
        let l = read(&p).unwrap().unwrap();
        assert_eq!((l.version, l.log_id.as_str()), (4, "id"));
        assert_eq!(
            l.frags,
            vec![(FileId("part-1.parquet".into()), "part-1.parquet|10".into())]
        );
        assert_eq!(l.addr_table, vec!["part-1.parquet|10".to_string()]);
        assert_eq!(l.view.live, 1);
    }

    /// A `KV0VIEW2` file, as kv0 0.1.0-beta.2 wrote it for Lance, is refused (and so ignored).
    #[test]
    fn refuses_version_2() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("view.snap");
        let mut w = Hashing {
            inner: Vec::new(),
            h: Xxh3::new(),
        };
        w.write_all(b"KV0VIEW2").unwrap();
        w.write_all(&3u64.to_le_bytes()).unwrap();
        w.write_all(&1u64.to_le_bytes()).unwrap();
        w.write_all(&5u64.to_le_bytes()).unwrap();
        put_str(&mut w, "data/x.lance|1|").unwrap();
        one_put().write_to(&mut w).unwrap();
        let digest = w.h.digest();
        let mut bytes = w.inner;
        bytes.extend_from_slice(&digest.to_le_bytes());
        fs::write(&p, bytes).unwrap();
        assert!(read(&p).is_err());
    }
}
