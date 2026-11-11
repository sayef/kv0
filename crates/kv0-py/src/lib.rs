//! Python bindings: the native module `kv0.kv0`.
//!
//! The pure-Python package around it (`python/kv0`) provides the public API:
//! `DB`, `Options`, `WriteBatch`, the iterator types and `kv0.errors`.

use std::collections::HashMap;
use std::time::Duration;

use kv0_rs::kv::{Db, DbIterator, Options, WalSync, WriteBatch, WriteOptions};
use kv0_rs::tracking::{self, NewRun, Repo, RepoOptions, Run, RunStatus, StepRange, Track};
use kv0_rs::{rt, Error};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyFileNotFoundError, PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Returns memory freed by a large operation to the operating system. mimalloc keeps freed
/// pages for reuse; after an optimize or purge that would keep the peak resident for good.
fn release_memory() {
    #[cfg(target_os = "linux")]
    // SAFETY: mi_collect only walks mimalloc's own heaps.
    unsafe {
        libmimalloc_sys::mi_collect(true)
    };
}

create_exception!(
    kv0,
    NotFound,
    PyFileNotFoundError,
    "The database or table does not exist."
);
create_exception!(
    kv0,
    Corruption,
    PyException,
    "Stored data could not be decoded."
);
create_exception!(
    kv0,
    NotSupported,
    PyException,
    "The operation is not supported, for example a write to a read-only database."
);
create_exception!(
    kv0,
    InvalidArgument,
    PyException,
    "An argument was invalid, or the handle is closed."
);
create_exception!(
    kv0,
    StoreIOError,
    PyOSError,
    "Object store or file system failure."
);
create_exception!(
    kv0,
    Conflict,
    PyException,
    "A concurrent writer committed a conflicting change."
);
create_exception!(
    kv0,
    MergeInProgress,
    PyException,
    "Kept for compatibility; never raised."
);
create_exception!(
    kv0,
    Incomplete,
    PyException,
    "Kept for compatibility; never raised."
);

fn err(e: Error) -> PyErr {
    match e {
        Error::NotFound(m) => NotFound::new_err(m),
        Error::Corruption(m) => Corruption::new_err(m),
        Error::NotSupported(m) => NotSupported::new_err(m),
        Error::InvalidArgument(m) => InvalidArgument::new_err(m),
        Error::Conflict(m) => Conflict::new_err(m),
        Error::Io(m) => StoreIOError::new_err(m),
        other => StoreIOError::new_err(other.to_string()),
    }
}

type PyRes<T> = PyResult<T>;

/// Holds a native handle and drops it with the GIL released.
///
/// Dropping the last reference to a database or repository flushes buffered writes on
/// kv0's runtime, and Lance logs from those threads through `pyo3-log`, which needs the
/// GIL. Anything that can wait on the runtime must therefore run without the GIL.
struct Detached<T: Send>(std::mem::ManuallyDrop<T>);

impl<T: Send> Detached<T> {
    fn new(v: T) -> Self {
        Detached(std::mem::ManuallyDrop::new(v))
    }
}

impl<T: Send> std::ops::Deref for Detached<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Send> std::ops::DerefMut for Detached<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T: Send> Drop for Detached<T> {
    fn drop(&mut self) {
        // SAFETY: the value is taken exactly once, here, and never used again.
        let v = unsafe { std::mem::ManuallyDrop::take(&mut self.0) };
        Python::attach(|py| py.detach(move || drop(v)));
    }
}

fn attr<'py, T: FromPyObjectOwned<'py>>(obj: &Bound<'py, PyAny>, name: &str) -> PyRes<Option<T>> {
    match obj.getattr(name) {
        Ok(v) if v.is_none() => Ok(None),
        Ok(v) => Ok(Some(v.extract().map_err(Into::into)?)),
        Err(_) => Ok(None),
    }
}

fn seconds(v: Option<f64>) -> Option<Duration> {
    v.filter(|s| *s > 0.0).map(Duration::from_secs_f64)
}

/// Reads options from an `Options` object (or any object with matching attributes) and
/// keyword arguments, keyword arguments winning.
fn kv_options(
    opts: Option<&Bound<'_, PyAny>>,
    read_only: bool,
    kwargs: Option<&Bound<'_, PyDict>>,
) -> PyRes<Options> {
    let mut o = Options {
        read_only,
        ..Options::default()
    };
    let mut sources: Vec<Bound<'_, PyAny>> = Vec::new();
    if let Some(opts) = opts {
        sources.push(opts.clone());
    }
    if let Some(kw) = kwargs {
        let ns = kw.py().import("types")?.getattr("SimpleNamespace")?;
        sources.push(ns.call((), Some(kw))?);
    }
    for s in &sources {
        if let Some(v) = attr::<bool>(s, "create_if_missing")? {
            o.create_if_missing = v;
        }
        if let Some(v) = attr::<HashMap<String, String>>(s, "storage_options")? {
            o.storage_options = v;
        }
        if let Some(v) = attr::<usize>(s, "write_buffer_size")? {
            o.write_buffer_size = v;
        }
        if s.hasattr("flush_interval")? {
            o.flush_interval = seconds(attr(s, "flush_interval")?);
        }
        if s.hasattr("refresh_interval")? {
            o.refresh_interval = seconds(attr(s, "refresh_interval")?);
        }
        if let Some(v) = attr::<usize>(s, "max_fragments")? {
            o.max_fragments = v;
        }
        if let Some(v) = attr::<usize>(s, "lazy_value_threshold")? {
            o.lazy_value_threshold = v;
        }
        if let Some(v) = attr::<usize>(s, "value_cache_size")? {
            o.value_cache_size = v;
        }
        if s.hasattr("snapshot_min_entries")? {
            o.snapshot_min_entries =
                attr::<usize>(s, "snapshot_min_entries")?.unwrap_or(usize::MAX);
        }
        if s.hasattr("version_retention")? {
            o.version_retention = seconds(attr(s, "version_retention")?);
        }
        if let Some(v) = attr::<bool>(s, "wal")? {
            o.wal = v;
        }
        if let Some(v) = attr::<String>(s, "wal_sync")? {
            o.wal_sync = WalSync::parse(&v).map_err(err)?;
        }
        if let Some(v) = attr::<std::path::PathBuf>(s, "wal_dir")? {
            o.wal_dir = Some(v);
        }
        if let Some(v) = attr::<bool>(s, "replay_stale_wal")? {
            o.replay_stale_wal = v;
        }
        if let Some(v) = attr::<f64>(s, "mark_ttl")? {
            o.mark_ttl = Duration::from_secs_f64(v.max(0.0));
        }
        if let Some(v) = attr::<u64>(s, "version")? {
            o.version = Some(v);
        }
    }
    Ok(o)
}

/// The native ordered key-value database. `kv0.DB` subclasses it.
#[pyclass(subclass, module = "kv0.kv0", name = "_DB")]
struct PyDb {
    db: Detached<Db>,
}

#[pymethods]
impl PyDb {
    #[new]
    #[pyo3(signature = (db_path, opts=None, column_families=None, read_only=false, **kwargs))]
    fn new(
        py: Python<'_>,
        db_path: &Bound<'_, PyAny>,
        opts: Option<&Bound<'_, PyAny>>,
        column_families: Option<&Bound<'_, PyAny>>,
        read_only: bool,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyRes<Self> {
        let _ = column_families;
        let path: String = py
            .import("os")?
            .getattr("fspath")?
            .call1((db_path,))?
            .extract()?;
        let o = kv_options(opts, read_only, kwargs)?;
        let db = py.detach(|| Db::open(&path, o)).map_err(err)?;
        Ok(PyDb {
            db: Detached::new(db),
        })
    }

    #[pyo3(signature = (key, value, sync=false, disable_wal=false, column_family=None))]
    fn put(
        &self,
        py: Python<'_>,
        key: &[u8],
        value: &[u8],
        sync: bool,
        disable_wal: bool,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<()> {
        let _ = column_family;
        let mut b = WriteBatch::new();
        b.put(key, value);
        let o = WriteOptions { sync, disable_wal };
        py.detach(|| self.db.write_opt(b, &o)).map_err(err)
    }

    /// Same as `put`: merge operators are not supported, the last write wins.
    #[pyo3(signature = (key, value, sync=false, disable_wal=false, column_family=None))]
    fn merge(
        &self,
        py: Python<'_>,
        key: &[u8],
        value: &[u8],
        sync: bool,
        disable_wal: bool,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<()> {
        self.put(py, key, value, sync, disable_wal, column_family)
    }

    #[pyo3(signature = (key, column_family=None))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<Option<Bound<'py, PyBytes>>> {
        let _ = column_family;
        let v = py.detach(|| self.db.get(key)).map_err(err)?;
        Ok(v.map(|v| PyBytes::new(py, &v)))
    }

    #[pyo3(signature = (key, sync=false, disable_wal=false, column_family=None))]
    fn delete(
        &self,
        py: Python<'_>,
        key: &[u8],
        sync: bool,
        disable_wal: bool,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<()> {
        let _ = column_family;
        let mut b = WriteBatch::new();
        b.delete(key);
        let o = WriteOptions { sync, disable_wal };
        py.detach(|| self.db.write_opt(b, &o)).map_err(err)
    }

    #[pyo3(signature = (begin_key, end_key, sync=false, disable_wal=false, column_family=None))]
    fn delete_range(
        &self,
        py: Python<'_>,
        begin_key: &[u8],
        end_key: &[u8],
        sync: bool,
        disable_wal: bool,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<()> {
        let _ = column_family;
        let mut b = WriteBatch::new();
        b.delete_range(begin_key, end_key);
        let o = WriteOptions { sync, disable_wal };
        py.detach(|| self.db.write_opt(b, &o)).map_err(err)
    }

    /// Applies a `WriteBatch` (anything with an `operations` list of
    /// `(op, key, value, column_family)` tuples) atomically.
    #[pyo3(signature = (batch, sync=false, disable_wal=false))]
    fn write(
        &self,
        py: Python<'_>,
        batch: &Bound<'_, PyAny>,
        sync: bool,
        disable_wal: bool,
    ) -> PyRes<()> {
        let ops = batch.getattr("operations")?;
        let mut wb = WriteBatch::new();
        for op in ops.try_iter()? {
            let op = op?;
            let t = op.cast::<PyTuple>()?;
            let kind: String = t.get_item(0)?.extract()?;
            let key: Vec<u8> = t.get_item(1)?.extract()?;
            match kind.as_str() {
                "PUT" | "MERGE" => {
                    let v: Vec<u8> = t.get_item(2)?.extract()?;
                    wb.put(key, v);
                }
                "DELETE" => {
                    wb.delete(key);
                }
                "DELETE_RANGE" => {
                    let end: Vec<u8> = t.get_item(2)?.extract()?;
                    wb.delete_range(key, end);
                }
                other => {
                    return Err(PyValueError::new_err(format!(
                        "unknown batch operation {other:?}"
                    )))
                }
            }
        }
        let o = WriteOptions { sync, disable_wal };
        py.detach(|| self.db.write_opt(wb, &o)).map_err(err)
    }

    #[pyo3(signature = (keys, column_family=None))]
    fn multi_get<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Vec<u8>>,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<Bound<'py, PyDict>> {
        let _ = column_family;
        let vals = py.detach(|| self.db.multi_get(&keys)).map_err(err)?;
        let d = PyDict::new(py);
        for (k, v) in keys.iter().zip(vals) {
            d.set_item(PyBytes::new(py, k), v.map(|v| PyBytes::new(py, &v)))?;
        }
        Ok(d)
    }

    #[pyo3(signature = (key, fetch=false, column_family=None))]
    fn key_may_exist<'py>(
        &self,
        py: Python<'py>,
        key: &[u8],
        fetch: bool,
        column_family: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<(bool, Option<Bound<'py, PyBytes>>)> {
        let _ = column_family;
        match py.detach(|| self.db.get(key)).map_err(err)? {
            Some(v) => Ok((true, fetch.then(|| PyBytes::new(py, &v)))),
            None => Ok((false, None)),
        }
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn iterkeys(
        &self,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> KeysIterator {
        let _ = (args, kwargs);
        KeysIterator(Detached::new(self.db.iter()))
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn itervalues(
        &self,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> ValuesIterator {
        let _ = (args, kwargs);
        ValuesIterator(Detached::new(self.db.iter()))
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn iteritems(
        &self,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> ItemsIterator {
        let _ = (args, kwargs);
        ItemsIterator(Detached::new(self.db.iter()))
    }

    /// Commits buffered writes to storage.
    fn flush(&self, py: Python<'_>) -> PyRes<()> {
        py.detach(|| self.db.flush()).map_err(err)
    }

    #[pyo3(signature = (sync=false))]
    fn flush_wal(&self, py: Python<'_>, sync: bool) -> PyRes<()> {
        let _ = sync;
        self.flush(py)
    }

    /// Flushes and rewrites storage as the live data; the key range is ignored.
    #[pyo3(signature = (begin=None, end=None, column_family=None, **kwargs))]
    fn compact_range(
        &self,
        py: Python<'_>,
        begin: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        column_family: Option<&Bound<'_, PyAny>>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyRes<()> {
        let _ = (begin, end, column_family, kwargs);
        let r = py.detach(|| self.db.compact());
        release_memory();
        r.map_err(err)
    }

    /// Picks up other writers' commits now; returns whether anything changed.
    fn refresh(&self, py: Python<'_>) -> PyRes<bool> {
        py.detach(|| self.db.refresh()).map_err(err)
    }

    /// The storage version this handle has read.
    #[getter]
    fn version(&self) -> u64 {
        self.db.version()
    }

    /// `[(version, timestamp_micros), ...]` for every stored version.
    fn versions(&self, py: Python<'_>) -> PyRes<Vec<(u64, i64)>> {
        let v = py.detach(|| self.db.versions()).map_err(err)?;
        Ok(v.into_iter()
            .map(|v| (v.version, v.timestamp_micros))
            .collect())
    }

    /// Physically removes deleted and overwritten data; see `Db::purge` in Rust. Returns a
    /// dict of what was removed.
    #[pyo3(signature = (force=false))]
    fn purge<'py>(&self, py: Python<'py>, force: bool) -> PyRes<Bound<'py, PyDict>> {
        let r = py.detach(|| self.db.purge(force));
        release_memory();
        let r = r.map_err(err)?;
        report_dict(py, &r)
    }

    /// Deletes versions older than `older_than` seconds; returns bytes removed. Tagged
    /// versions are kept.
    fn cleanup_old_versions(&self, py: Python<'_>, older_than: f64) -> PyRes<u64> {
        let d = Duration::from_secs_f64(older_than.max(0.0));
        py.detach(|| self.db.cleanup_old_versions(d)).map_err(err)
    }

    /// Names a version (this handle's when `None`); open it later with
    /// `version=db.tag_version(tag)`. Tagged versions survive cleanup.
    #[pyo3(signature = (tag, version=None))]
    fn create_tag(&self, py: Python<'_>, tag: &str, version: Option<u64>) -> PyRes<()> {
        py.detach(|| self.db.create_tag(tag, version)).map_err(err)
    }

    /// `[(tag, version), ...]` sorted by tag.
    fn tags(&self, py: Python<'_>) -> PyRes<Vec<(String, u64)>> {
        py.detach(|| self.db.tags()).map_err(err)
    }

    fn tag_version(&self, py: Python<'_>, tag: &str) -> PyRes<u64> {
        py.detach(|| self.db.tag_version(tag)).map_err(err)
    }

    fn delete_tag(&self, py: Python<'_>, tag: &str) -> PyRes<()> {
        py.detach(|| self.db.delete_tag(tag)).map_err(err)
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyRes<Bound<'py, PyDict>> {
        let s = py.detach(|| self.db.stats());
        let d = PyDict::new(py);
        d.set_item("version", s.version)?;
        d.set_item("live_keys", s.live_keys)?;
        d.set_item("pending_ops", s.pending_ops)?;
        d.set_item("pending_bytes", s.pending_bytes)?;
        d.set_item("fragments", s.fragments)?;
        d.set_item("stored_rows", s.stored_rows)?;
        Ok(d)
    }

    #[getter]
    fn uri(&self) -> String {
        self.db.uri().to_string()
    }

    #[getter]
    fn read_only(&self) -> bool {
        self.db.is_read_only()
    }

    /// Flushes and closes the handle.
    fn close(&self, py: Python<'_>) -> PyRes<()> {
        py.detach(|| self.db.close()).map_err(err)
    }
}

macro_rules! iterator_class {
    ($name:ident, $doc:literal, |$py:ident, $k:ident, $v:ident| $project:expr) => {
        #[doc = $doc]
        #[pyclass(module = "kv0.kv0")]
        struct $name(Detached<DbIterator>);

        impl $name {
            fn project<'py>($py: Python<'py>, $k: &[u8], $v: &[u8]) -> PyRes<Bound<'py, PyAny>> {
                Ok($project.into_any())
            }
        }

        #[pymethods]
        impl $name {
            fn seek_to_first(&mut self, py: Python<'_>) {
                py.detach(|| self.0.seek_to_first())
            }

            fn seek_to_last(&mut self, py: Python<'_>) {
                py.detach(|| self.0.seek_to_last())
            }

            fn seek(&mut self, py: Python<'_>, key: &[u8]) {
                py.detach(|| self.0.seek(key))
            }

            fn seek_for_prev(&mut self, py: Python<'_>, key: &[u8]) {
                py.detach(|| self.0.seek_for_prev(key))
            }

            /// The entry at the cursor; raises `ValueError` past the end.
            fn get<'py>(&self, py: Python<'py>) -> PyRes<Bound<'py, PyAny>> {
                match self.0.current() {
                    Some((k, v)) => Self::project(py, k, v),
                    None => Err(PyValueError::new_err(
                        "the iterator is not positioned on an entry",
                    )),
                }
            }

            /// Returns the entry at the cursor and advances; `None` past the end.
            fn next<'py>(&mut self, py: Python<'py>) -> PyRes<Option<Bound<'py, PyAny>>> {
                match Iterator::next(&mut *self.0) {
                    Some((k, v)) => Ok(Some(Self::project(py, &k, &v)?)),
                    None => Ok(None),
                }
            }

            /// Advances without returning; raises `ValueError` past the end.
            fn skip(&mut self) -> PyRes<()> {
                if !self.0.valid() {
                    return Err(PyValueError::new_err(
                        "the iterator is not positioned on an entry",
                    ));
                }
                self.0.step();
                Ok(())
            }

            fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
                slf
            }

            fn __next__<'py>(&mut self, py: Python<'py>) -> PyRes<Option<Bound<'py, PyAny>>> {
                self.next(py)
            }
        }
    };
}

iterator_class!(KeysIterator, "Iterates over keys.", |py, k, _v| {
    PyBytes::new(py, k)
});
iterator_class!(ValuesIterator, "Iterates over values.", |py, _k, v| {
    PyBytes::new(py, v)
});
iterator_class!(
    ItemsIterator,
    "Iterates over `(key, value)` pairs.",
    |py, k, v| PyTuple::new(py, [PyBytes::new(py, k), PyBytes::new(py, v)])?
);

// ---- tracking -------------------------------------------------------------------------------

fn to_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyRes<serde_json::Value> {
    let s: String = py
        .import("json")?
        .getattr("dumps")?
        .call1((obj,))?
        .extract()?;
    serde_json::from_str(&s).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn from_json<'py>(py: Python<'py>, v: &serde_json::Value) -> PyRes<Bound<'py, PyAny>> {
    py.import("json")?.getattr("loads")?.call1((v.to_string(),))
}

fn run_dict<'py>(py: Python<'py>, r: &tracking::RunInfo) -> PyRes<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("run_id", &r.run_id)?;
    d.set_item("name", &r.name)?;
    d.set_item("experiment", &r.experiment)?;
    d.set_item("description", &r.description)?;
    d.set_item("tags", PyList::new(py, &r.tags)?)?;
    d.set_item("status", r.status.as_str())?;
    d.set_item("archived", r.archived)?;
    d.set_item("created_at", r.created_at)?;
    d.set_item("updated_at", r.updated_at)?;
    d.set_item("ended_at", r.ended_at)?;
    d.set_item("kind", &r.kind)?;
    match &r.target {
        Some(t) => d.set_item("target", from_json(py, t)?)?,
        None => d.set_item("target", py.None())?,
    }
    Ok(d)
}

fn series_dict<'py>(py: Python<'py>, s: &tracking::SeriesInfo) -> PyRes<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("series_id", s.series_id)?;
    d.set_item("run_id", &s.run_id)?;
    d.set_item("name", &s.name)?;
    d.set_item("context", from_json(py, &s.context)?)?;
    d.set_item("created_at", s.created_at)?;
    Ok(d)
}

/// Metadata changes from keyword arguments; `None` leaves a field unchanged.
#[derive(Default)]
struct RunChanges {
    name: Option<String>,
    experiment: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
    archived: Option<bool>,
    status: Option<RunStatus>,
    kind: Option<String>,
    target: Option<serde_json::Value>,
}

impl RunChanges {
    fn apply(self, i: &mut tracking::RunInfo) {
        if self.name.is_some() {
            i.name = self.name;
        }
        if self.experiment.is_some() {
            i.experiment = self.experiment;
        }
        if self.description.is_some() {
            i.description = self.description;
        }
        if let Some(t) = self.tags {
            i.tags = t;
        }
        if let Some(a) = self.archived {
            i.archived = a;
        }
        if let Some(s) = self.status {
            i.status = s;
        }
        if self.kind.is_some() {
            i.kind = self.kind;
        }
        if self.target.is_some() {
            i.target = self.target;
        }
    }
}

/// A tracking repository: runs, parameters and metric series at one location.
#[pyclass(subclass, module = "kv0.kv0", name = "Repo")]
struct PyRepo {
    repo: Detached<Repo>,
}

#[pymethods]
impl PyRepo {
    #[new]
    #[pyo3(signature = (uri, storage_options=None, create_if_missing=true, flush_rows=50_000, flush_interval=5.0, version_retention=604_800.0, wal=true, wal_sync="batch", wal_dir=None, replay_stale_wal=false, legacy_records="error"))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn new(
        py: Python<'_>,
        uri: &Bound<'_, PyAny>,
        storage_options: Option<HashMap<String, String>>,
        create_if_missing: bool,
        flush_rows: usize,
        flush_interval: Option<f64>,
        version_retention: Option<f64>,
        wal: bool,
        wal_sync: &str,
        wal_dir: Option<std::path::PathBuf>,
        replay_stale_wal: bool,
        legacy_records: &str,
    ) -> PyRes<Self> {
        let legacy_records = match legacy_records {
            "error" => tracking::LegacyRecords::Error,
            "allow" => tracking::LegacyRecords::Allow,
            other => {
                return Err(PyValueError::new_err(format!(
                    "legacy_records must be 'error' or 'allow', not {other:?}"
                )))
            }
        };
        let uri: String = py
            .import("os")?
            .getattr("fspath")?
            .call1((uri,))?
            .extract()?;
        let opts = RepoOptions {
            create_if_missing,
            storage_options: storage_options.unwrap_or_default(),
            flush_rows: flush_rows.max(1),
            flush_interval: seconds(flush_interval),
            version_retention: seconds(version_retention),
            wal,
            wal_sync: WalSync::parse(wal_sync).map_err(err)?,
            wal_dir,
            replay_stale_wal,
            legacy_records,
            ..RepoOptions::default()
        };
        let repo = py
            .detach(|| rt::block_on(async move { Repo::open(&uri, opts).await }))
            .map_err(err)?;
        Ok(PyRepo {
            repo: Detached::new(repo),
        })
    }

    #[getter]
    fn uri(&self) -> String {
        self.repo.uri().to_string()
    }

    /// Creates a run. `created_at` is microseconds since the Unix epoch (UTC), for imported
    /// runs; `target` is a JSON-compatible dict.
    #[pyo3(signature = (name=None, experiment=None, description=None, tags=None, run_id=None, created_at=None, kind=None, target=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn create_run(
        &self,
        py: Python<'_>,
        name: Option<String>,
        experiment: Option<String>,
        description: Option<String>,
        tags: Option<Vec<String>>,
        run_id: Option<String>,
        created_at: Option<i64>,
        kind: Option<String>,
        target: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<PyRun> {
        let target = match target {
            Some(t) if !t.is_none() => Some(to_json(py, t)?),
            _ => None,
        };
        let run = self
            .repo
            .create_run(NewRun {
                run_id,
                name,
                experiment,
                description,
                tags: tags.unwrap_or_default(),
                created_at,
                kind,
                target,
            })
            .map_err(err)?;
        Ok(PyRun {
            run: Detached::new(run),
        })
    }

    fn resume_run(&self, py: Python<'_>, run_id: String) -> PyRes<PyRun> {
        let repo = (*self.repo).clone();
        let run = py
            .detach(|| rt::block_on(async move { repo.resume_run(&run_id).await }))
            .map_err(err)?;
        Ok(PyRun {
            run: Detached::new(run),
        })
    }

    fn runs<'py>(&self, py: Python<'py>) -> PyRes<Vec<Bound<'py, PyDict>>> {
        let repo = (*self.repo).clone();
        let runs = py
            .detach(|| rt::block_on(async move { repo.runs().await }))
            .map_err(err)?;
        runs.iter().map(|r| run_dict(py, r)).collect()
    }

    fn run<'py>(&self, py: Python<'py>, run_id: String) -> PyRes<Option<Bound<'py, PyDict>>> {
        let repo = (*self.repo).clone();
        let r = py
            .detach(|| rt::block_on(async move { repo.run(&run_id).await }))
            .map_err(err)?;
        r.map(|r| run_dict(py, &r)).transpose()
    }

    fn params<'py>(&self, py: Python<'py>, run_id: String) -> PyRes<Bound<'py, PyAny>> {
        let repo = (*self.repo).clone();
        let v = py
            .detach(|| rt::block_on(async move { repo.params(&run_id).await }))
            .map_err(err)?;
        from_json(py, &v)
    }

    fn series<'py>(&self, py: Python<'py>, run_id: String) -> PyRes<Vec<Bound<'py, PyDict>>> {
        let repo = (*self.repo).clone();
        let rows = py
            .detach(|| rt::block_on(async move { repo.series(&run_id).await }))
            .map_err(err)?;
        rows.iter().map(|s| series_dict(py, s)).collect()
    }

    /// Every run's parameters as `{run_id: params}`, from one scan.
    fn all_params<'py>(&self, py: Python<'py>) -> PyRes<Bound<'py, PyDict>> {
        let repo = (*self.repo).clone();
        let all = py
            .detach(|| rt::block_on(async move { repo.all_params().await }))
            .map_err(err)?;
        let d = PyDict::new(py);
        for (run, v) in &all {
            d.set_item(run, from_json(py, v)?)?;
        }
        Ok(d)
    }

    /// Every series of every run, from one scan.
    fn all_series<'py>(&self, py: Python<'py>) -> PyRes<Vec<Bound<'py, PyDict>>> {
        let repo = (*self.repo).clone();
        let rows = py
            .detach(|| rt::block_on(async move { repo.all_series().await }))
            .map_err(err)?;
        rows.iter().map(|s| series_dict(py, s)).collect()
    }

    /// Changes a run's metadata without reading its points; only the fields given are
    /// written, so concurrent changes to other fields are kept. Returns the record as written.
    #[pyo3(signature = (run_id, name=None, experiment=None, description=None, tags=None, archived=None, status=None, kind=None, target=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn update_run<'py>(
        &self,
        py: Python<'py>,
        run_id: String,
        name: Option<String>,
        experiment: Option<String>,
        description: Option<String>,
        tags: Option<Vec<String>>,
        archived: Option<bool>,
        status: Option<&str>,
        kind: Option<String>,
        target: Option<&Bound<'py, PyAny>>,
    ) -> PyRes<Bound<'py, PyDict>> {
        let c = RunChanges {
            name,
            experiment,
            description,
            tags,
            archived,
            status: status.map(RunStatus::parse).transpose().map_err(err)?,
            kind,
            target: match target {
                Some(t) if !t.is_none() => Some(to_json(py, t)?),
                _ => None,
            },
        };
        let repo = (*self.repo).clone();
        let r = py
            .detach(|| rt::block_on(async move { repo.update_run(&run_id, |i| c.apply(i)).await }))
            .map_err(err)?;
        run_dict(py, &r)
    }

    /// The newest committed version of each table, `(runs, params, series, metrics)`, without
    /// scanning: compare with an earlier result to see whether anything changed.
    fn latest_versions(&self, py: Python<'_>) -> PyRes<(u64, u64, u64, u64)> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.latest_versions().await }))
            .map_err(err)
    }

    /// Appends rows (dicts with `item_id`, optional `run_id` and `data`) to the records table
    /// `table` in one commit; returns the records version that holds them.
    fn append_records(
        &self,
        py: Python<'_>,
        table: String,
        rows: Vec<Bound<'_, PyDict>>,
    ) -> PyRes<u64> {
        let mut new = Vec::with_capacity(rows.len());
        for r in &rows {
            let item_id: String = r
                .get_item("item_id")?
                .ok_or_else(|| PyValueError::new_err("a record needs an item_id"))?
                .extract()?;
            let run_id: Option<String> = match r.get_item("run_id")? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            let data = match r.get_item("data")? {
                Some(v) => to_json(py, &v)?,
                None => serde_json::Value::Null,
            };
            new.push(tracking::NewRecord {
                item_id,
                run_id,
                data,
            });
        }
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.append_records(&table, new).await }))
            .map_err(err)
    }

    /// The newest row per key of records table `table`, in append order, optionally for one
    /// run, several runs (`run_ids`) or one item, at a version or a tag.
    #[pyo3(signature = (table, run_id=None, item_id=None, version=None, tag=None, run_ids=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn records<'py>(
        &self,
        py: Python<'py>,
        table: String,
        run_id: Option<String>,
        item_id: Option<String>,
        version: Option<u64>,
        tag: Option<String>,
        run_ids: Option<Vec<String>>,
    ) -> PyRes<Vec<Bound<'py, PyDict>>> {
        let at = match (version, tag) {
            (Some(_), Some(_)) => {
                return Err(PyValueError::new_err("give a version or a tag, not both"))
            }
            (Some(v), None) => tracking::At::Version(v),
            (None, Some(t)) => tracking::At::Tag(t),
            (None, None) => tracking::At::Latest,
        };
        let q = tracking::RecordQuery {
            run_id,
            run_ids,
            item_id,
            at,
        };
        let repo = (*self.repo).clone();
        let rows = py
            .detach(|| rt::block_on(async move { repo.records(&table, q).await }))
            .map_err(err)?;
        record_dicts(py, &rows)
    }

    /// Every row of the shared records.lance of earlier builds (of `table`, or all), in
    /// append order, for a one-time migration; empty when there is none.
    #[pyo3(signature = (table=None))]
    fn legacy_records<'py>(
        &self,
        py: Python<'py>,
        table: Option<String>,
    ) -> PyRes<Vec<Bound<'py, PyDict>>> {
        let repo = (*self.repo).clone();
        let rows = py
            .detach(|| rt::block_on(async move { repo.legacy_records(table.as_deref()).await }))
            .map_err(err)?;
        record_dicts(py, &rows)
    }

    /// Whether the shared records.lance of earlier builds exists (no rows are read).
    fn has_legacy_records(&self, py: Python<'_>) -> PyRes<bool> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.has_legacy_records().await }))
            .map_err(err)
    }

    /// Deletes the shared records.lance of earlier builds; returns whether there was one.
    fn drop_legacy_records(&self, py: Python<'_>) -> PyRes<bool> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.drop_legacy_records().await }))
            .map_err(err)
    }

    /// Names of the records tables.
    fn record_tables(&self, py: Python<'_>) -> PyRes<Vec<String>> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.record_tables().await }))
            .map_err(err)
    }

    /// The latest committed version of records table `table`.
    fn records_version(&self, py: Python<'_>, table: String) -> PyRes<u64> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.records_version(&table).await }))
            .map_err(err)
    }

    /// Names a version of records table `table` (the latest when `None`); tagged versions
    /// survive cleanup. The tag keeps this table only.
    #[pyo3(signature = (table, tag, version=None))]
    fn tag_records(
        &self,
        py: Python<'_>,
        table: String,
        tag: String,
        version: Option<u64>,
    ) -> PyRes<()> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.tag_records(&table, &tag, version).await }))
            .map_err(err)
    }

    /// `[(tag, version), ...]` of records table `table`, sorted by tag.
    fn record_tags(&self, py: Python<'_>, table: String) -> PyRes<Vec<(String, u64)>> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.record_tags(&table).await }))
            .map_err(err)
    }

    fn delete_record_tag(&self, py: Python<'_>, table: String, tag: String) -> PyRes<()> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.delete_record_tag(&table, &tag).await }))
            .map_err(err)
    }

    /// Physically removes the data of deleted runs; see `Repo.purge` in Rust. Returns a dict
    /// of what was removed.
    #[pyo3(signature = (force=false))]
    fn purge<'py>(&self, py: Python<'py>, force: bool) -> PyRes<Bound<'py, PyDict>> {
        let repo = (*self.repo).clone();
        let r = py.detach(|| rt::block_on(async move { repo.purge(force).await }));
        release_memory();
        let r = r.map_err(err)?;
        report_dict(py, &r)
    }

    /// Stored totals of the series in `series_ids` (all series when `None`), as
    /// `{series_id: {"count", "first_step", "last_step", "last", "min", "min_step", "max",
    /// "max_step", "non_finite", "last_timestamp"}}`, without reading points.
    #[pyo3(signature = (series_ids=None))]
    fn series_stats<'py>(
        &self,
        py: Python<'py>,
        series_ids: Option<Vec<u64>>,
    ) -> PyRes<Bound<'py, PyDict>> {
        let repo = (*self.repo).clone();
        let all = py
            .detach(|| {
                rt::block_on(async move {
                    match series_ids {
                        Some(ids) => repo.series_stats(&ids).await,
                        None => repo.all_series_stats().await,
                    }
                })
            })
            .map_err(err)?;
        let out = PyDict::new(py);
        for (id, s) in &all {
            let d = PyDict::new(py);
            d.set_item("count", s.count)?;
            d.set_item("first_step", s.first_step)?;
            d.set_item("last_step", s.last_step)?;
            d.set_item("last", s.last)?;
            d.set_item("min", s.min)?;
            d.set_item("min_step", s.min_step)?;
            d.set_item("max", s.max)?;
            d.set_item("max_step", s.max_step)?;
            d.set_item("non_finite", s.non_finite)?;
            d.set_item("last_timestamp", s.last_timestamp)?;
            out.set_item(id, d)?;
        }
        Ok(out)
    }

    /// Recomputes stored totals from points (all series when `None`); returns how many.
    #[pyo3(signature = (series_ids=None))]
    fn recompute_series_stats(&self, py: Python<'_>, series_ids: Option<Vec<u64>>) -> PyRes<usize> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.recompute_series_stats(series_ids).await }))
            .map_err(err)
    }

    /// Deletes runs and their parameters, series and points; returns how many existed.
    fn delete_runs(&self, py: Python<'_>, run_ids: Vec<String>) -> PyRes<usize> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.delete_runs(&run_ids).await }))
            .map_err(err)
    }

    /// Points of one series as `{"steps", "values", "epochs", "timestamps"}` lists, ordered by
    /// step, for `start <= step < stop` and `start_time <= timestamp < stop_time`
    /// (microseconds since the Unix epoch, UTC).
    #[pyo3(signature = (run_id, name, context=None, start=None, stop=None, start_time=None, stop_time=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn metric<'py>(
        &self,
        py: Python<'py>,
        run_id: String,
        name: String,
        context: Option<&Bound<'py, PyAny>>,
        start: Option<i64>,
        stop: Option<i64>,
        start_time: Option<i64>,
        stop_time: Option<i64>,
    ) -> PyRes<Bound<'py, PyDict>> {
        let ctx = match context {
            Some(c) if !c.is_none() => to_json(py, c)?,
            _ => serde_json::Value::Object(Default::default()),
        };
        let repo = (*self.repo).clone();
        let d = py
            .detach(|| {
                rt::block_on(async move {
                    let id = tracking::series_id(&run_id, &name, &ctx);
                    let time = tracking::TimeRange {
                        start: start_time,
                        stop: stop_time,
                    };
                    repo.read_many_in(&[id], StepRange { start, stop }, time)
                        .await
                        .map(|mut m| m.remove(&id).unwrap_or_default())
                })
            })
            .map_err(err)?;
        let out = PyDict::new(py);
        out.set_item("steps", d.steps)?;
        out.set_item("values", d.values)?;
        out.set_item("epochs", d.epochs)?;
        out.set_item("timestamps", d.timestamps)?;
        Ok(out)
    }

    fn flush(&self, py: Python<'_>) -> PyRes<()> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.flush().await }))
            .map_err(err)
    }

    /// Deletes versions older than `older_than` seconds in every table; returns bytes removed.
    fn cleanup_old_versions(&self, py: Python<'_>, older_than: f64) -> PyRes<u64> {
        let repo = (*self.repo).clone();
        let d = Duration::from_secs_f64(older_than.max(0.0));
        py.detach(|| rt::block_on(async move { repo.cleanup_old_versions(d).await }))
            .map_err(err)
    }

    /// Compacts small files and builds the metrics index.
    fn optimize(&self, py: Python<'_>) -> PyRes<()> {
        let repo = (*self.repo).clone();
        let r = py.detach(|| rt::block_on(async move { repo.optimize().await }));
        release_memory();
        r.map_err(err)
    }

    fn close(&self, py: Python<'_>) -> PyRes<()> {
        let repo = (*self.repo).clone();
        py.detach(|| rt::block_on(async move { repo.close().await }))
            .map_err(err)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (*exc))]
    fn __exit__(&self, py: Python<'_>, exc: &Bound<'_, PyTuple>) -> PyRes<bool> {
        let _ = exc;
        self.close(py)?;
        Ok(false)
    }
}

/// A run being written.
#[pyclass(module = "kv0.kv0", name = "Run")]
struct PyRun {
    run: Detached<Run>,
}

#[pymethods]
impl PyRun {
    #[getter]
    fn id(&self) -> String {
        self.run.id()
    }

    #[getter]
    fn info<'py>(&self, py: Python<'py>) -> PyRes<Bound<'py, PyDict>> {
        run_dict(py, &self.run.info())
    }

    /// Buffers one metric point. `step` defaults to one past the series' last step.
    #[pyo3(signature = (value, name, step=None, epoch=None, context=None, timestamp=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn track(
        &self,
        py: Python<'_>,
        value: f64,
        name: &str,
        step: Option<i64>,
        epoch: Option<i64>,
        context: Option<&Bound<'_, PyAny>>,
        timestamp: Option<i64>,
    ) -> PyRes<()> {
        let context = match context {
            Some(c) if !c.is_none() => Some(to_json(py, c)?),
            _ => None,
        };
        let t = Track {
            step,
            epoch,
            context,
            timestamp,
        };
        py.detach(|| self.run.track(name, value, t)).map_err(err)
    }

    /// Merges a dict of parameters, by dotted key, into the run's parameters.
    fn set_params(&self, py: Python<'_>, params: &Bound<'_, PyAny>) -> PyRes<()> {
        let v = to_json(py, params)?;
        self.run.set_params(&v).map_err(err)
    }

    fn __setitem__(&self, py: Python<'_>, key: &str, value: &Bound<'_, PyAny>) -> PyRes<()> {
        let v = to_json(py, value)?;
        self.run.set_param(key, v).map_err(err)
    }

    /// Changes metadata; `None` leaves a field unchanged. Only changed fields are written.
    #[pyo3(signature = (name=None, experiment=None, description=None, tags=None, archived=None, kind=None, target=None))]
    #[allow(clippy::too_many_arguments)] // mirrors the Python signature
    fn update(
        &self,
        py: Python<'_>,
        name: Option<String>,
        experiment: Option<String>,
        description: Option<String>,
        tags: Option<Vec<String>>,
        archived: Option<bool>,
        kind: Option<String>,
        target: Option<&Bound<'_, PyAny>>,
    ) -> PyRes<()> {
        let c = RunChanges {
            name,
            experiment,
            description,
            tags,
            archived,
            status: None,
            kind,
            target: match target {
                Some(t) if !t.is_none() => Some(to_json(py, t)?),
                _ => None,
            },
        };
        self.run.update(|i| c.apply(i)).map_err(err)
    }

    /// Marks the run ended (`finished`, `failed` or `aborted`) and flushes. `ended_at` is
    /// microseconds since the Unix epoch (UTC); now when `None`.
    #[pyo3(signature = (status="finished", ended_at=None))]
    fn finish(&self, py: Python<'_>, status: &str, ended_at: Option<i64>) -> PyRes<()> {
        let status = RunStatus::parse(status).map_err(err)?;
        let run = (*self.run).clone();
        py.detach(|| rt::block_on(async move { run.finish_at(status, ended_at).await }))
            .map_err(err)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type, exc, tb))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: &Bound<'_, PyAny>,
        exc: &Bound<'_, PyAny>,
        tb: &Bound<'_, PyAny>,
    ) -> PyRes<bool> {
        let _ = (exc, tb);
        self.finish(
            py,
            if exc_type.is_none() {
                "finished"
            } else {
                "failed"
            },
            None,
        )?;
        Ok(false)
    }
}

#[pymodule]
fn kv0(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    let py = m.py();
    m.add_class::<PyDb>()?;
    m.add_class::<KeysIterator>()?;
    m.add_class::<ValuesIterator>()?;
    m.add_class::<ItemsIterator>()?;
    m.add_class::<PyRepo>()?;
    m.add_class::<PyRun>()?;
    m.add("NotFound", py.get_type::<NotFound>())?;
    m.add("Corruption", py.get_type::<Corruption>())?;
    m.add("NotSupported", py.get_type::<NotSupported>())?;
    m.add("InvalidArgument", py.get_type::<InvalidArgument>())?;
    m.add("StoreIOError", py.get_type::<StoreIOError>())?;
    m.add("Conflict", py.get_type::<Conflict>())?;
    m.add("MergeInProgress", py.get_type::<MergeInProgress>())?;
    m.add("Incomplete", py.get_type::<Incomplete>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

fn report_dict<'py>(py: Python<'py>, r: &kv0_rs::PurgeReport) -> PyRes<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("runs", r.runs)?;
    d.set_item("fragments_rewritten", r.fragments_rewritten)?;
    d.set_item("versions_removed", r.versions_removed)?;
    d.set_item("bytes_removed", r.bytes_removed)?;
    d.set_item("tags_deleted", r.tags_deleted.clone())?;
    d.set_item("snapshots_removed", r.snapshots_removed)?;
    d.set_item("tags_left", r.tags_left.clone())?;
    Ok(d)
}

fn record_dicts<'py>(py: Python<'py>, rows: &[tracking::Record]) -> PyRes<Vec<Bound<'py, PyDict>>> {
    rows.iter()
        .map(|r| {
            let d = PyDict::new(py);
            d.set_item("table", &r.table)?;
            d.set_item("item_id", &r.item_id)?;
            d.set_item("run_id", &r.run_id)?;
            d.set_item("data", from_json(py, &r.data)?)?;
            d.set_item("seq", r.seq)?;
            d.set_item("created_at", r.created_at)?;
            Ok(d)
        })
        .collect()
}
