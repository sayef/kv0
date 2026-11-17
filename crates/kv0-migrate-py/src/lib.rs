//! Python bindings of `kv0::migrate`: the native module `kv0_migrate._native`.
//!
//! The package around it (`python/kv0_migrate`) provides `migrate(...)` and the `kv0-migrate`
//! command. It is a separate distribution so the main `kv0` wheel does not carry the Lance
//! reader.
//!
//! Everything is behind the `migrate` feature, which the wheel build turns on.
#![cfg(feature = "migrate")]

use std::collections::HashMap;
use std::time::Duration;

use kv0_rs::migrate::{self, MigrateOptions};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    kv0_migrate,
    MigrationError,
    PyException,
    "The migration failed or was refused; the message says why."
);

/// Converts the kv0 database or repository at `old` (Lance, kv0 0.1.0-beta.2 and earlier) to
/// Delta Lake at `new`. Returns the report (also written to `<new>/_kv0/migration.json`) as
/// JSON text.
#[pyfunction]
#[pyo3(signature = (old, new, storage_options=None, target_storage_options=None, keep_tags=true, dry_run=false, resume=false, remove_source=false, force_in_place=false, optimize=true, quiet_period=10.0))]
#[allow(clippy::too_many_arguments)] // mirrors the Python signature
fn migrate_json(
    py: Python<'_>,
    old: String,
    new: String,
    storage_options: Option<HashMap<String, String>>,
    target_storage_options: Option<HashMap<String, String>>,
    keep_tags: bool,
    dry_run: bool,
    resume: bool,
    remove_source: bool,
    force_in_place: bool,
    optimize: bool,
    quiet_period: f64,
) -> PyResult<String> {
    let opts = MigrateOptions {
        storage_options: storage_options.unwrap_or_default(),
        target_storage_options,
        keep_tags,
        dry_run,
        resume,
        remove_source,
        force_in_place,
        optimize,
        quiet_period: Duration::from_secs_f64(quiet_period.max(0.0)),
        ..MigrateOptions::default()
    };
    let report = py
        .detach(|| migrate::migrate(&old, &new, &opts))
        .map_err(|e| MigrationError::new_err(e.to_string()))?;
    serde_json::to_string(&report).map_err(|e| MigrationError::new_err(e.to_string()))
}

/// Runs the command line (`migrate <from> <to> [options]`) and returns its exit code.
#[pyfunction]
fn cli(py: Python<'_>, args: Vec<String>) -> i32 {
    py.detach(|| migrate::cli(&args))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = pyo3_log::try_init();
    m.add_function(wrap_pyfunction!(migrate_json, m)?)?;
    m.add_function(wrap_pyfunction!(cli, m)?)?;
    m.add("MigrationError", m.py().get_type::<MigrationError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
