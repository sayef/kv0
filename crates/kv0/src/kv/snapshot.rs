//! A local snapshot of a key-value view, so a reopen loads it instead of rescanning Lance.
//!
//! Stored next to the dataset's WAL directories (`<wal root>/<xxh3 of the URI>/view.snap`):
//! `KV0VIEW2`, the dataset version, each fragment's identity, the view (see
//! [`View::write_to`]), then the xxh3-64 of everything before it. A snapshot is used only when
//! every fragment it covers is still in the dataset unchanged: compaction (which may drop
//! tombstones) or a recreated dataset invalidates it, and the open falls back to a full scan.
//! Fragments committed since are read and applied as usual.

use std::fs;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use lance::dataset::Dataset;
use xxhash_rust::xxh3::Xxh3;

use super::view::View;

const MAGIC: &[u8; 8] = b"KV0VIEW2";

pub(crate) fn path(root: &Path, uri: &str) -> PathBuf {
    super::wal::dataset_dir(root, uri).join("view.snap")
}

/// Each fragment's id and identity (first data file, which Lance names uniquely, row count and
/// whether it has deletions).
pub(crate) fn fragments(ds: &Dataset) -> Vec<(u64, String)> {
    ds.fragments()
        .iter()
        .map(|f| {
            let file = f.files.first().map_or("", |d| d.path.as_str());
            let rows = f.physical_rows.unwrap_or(0);
            // Which deletion file, not only whether there is one: more deletions write a new one.
            let del = f
                .deletion_file
                .as_ref()
                .map_or(String::new(), |d| format!("{}.{}", d.id, d.read_version));
            (f.id, format!("{file}|{rows}|{del}"))
        })
        .collect()
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

/// Writes a snapshot of `view` at `version` atomically (a temporary file, then a rename).
pub(crate) fn write(
    path: &Path,
    version: u64,
    frags: &[(u64, String)],
    view: &View,
) -> io::Result<()> {
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
        w.write_all(&version.to_le_bytes())?;
        w.write_all(&(frags.len() as u64).to_le_bytes())?;
        for (id, ident) in frags {
            w.write_all(&id.to_le_bytes())?;
            w.write_all(&(ident.len() as u32).to_le_bytes())?;
            w.write_all(ident.as_bytes())?;
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
    pub frags: Vec<(u64, String)>,
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
        return Err(bad("not a kv0 view snapshot"));
    }
    let mut u = [0u8; 8];
    r.read_exact(&mut u)?;
    let version = u64::from_le_bytes(u);
    r.read_exact(&mut u)?;
    let n = u64::from_le_bytes(u);
    let mut frags = Vec::new();
    for _ in 0..n {
        r.read_exact(&mut u)?;
        let id = u64::from_le_bytes(u);
        let mut l = [0u8; 4];
        r.read_exact(&mut l)?;
        let len = u32::from_le_bytes(l) as usize;
        if len > 4096 {
            return Err(bad("fragment identity too long"));
        }
        let mut s = vec![0u8; len];
        r.read_exact(&mut s)?;
        frags.push((
            id,
            String::from_utf8(s).map_err(|_| bad("fragment identity"))?,
        ));
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
        frags,
        view,
    }))
}

/// Whether a snapshot still describes `ds`: every fragment it covers is present and unchanged.
pub(crate) fn still_valid(l: &Loaded, ds: &Dataset) -> bool {
    if ds.version().version < l.version {
        return false;
    }
    let now: std::collections::HashMap<u64, String> = fragments(ds).into_iter().collect();
    l.frags.iter().all(|(id, ident)| now.get(id) == Some(ident))
}
