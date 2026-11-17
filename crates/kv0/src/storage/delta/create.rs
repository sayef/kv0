//! Opening a table, creating it when it does not exist.
//!
//! delta-rs commits a table's creation without a read snapshot, so two creators racing can
//! both "win": the second retries and commits another protocol and metadata at version 1
//! (delta-rs issue #4982). kv0 creates with no commit retries, so the loser fails and opens the
//! winner's table instead. As a tripwire, the first open of a table in a process checks that
//! version 1 holds no protocol or metadata action.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::kernel::StructField;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaTable, ObjectStore};

use super::table;
use crate::error::{Error, Result};
use crate::storage::{is_local, StorageOptions};

/// What a table is created with.
pub(crate) struct Spec {
    pub columns: Vec<StructField>,
    pub config: HashMap<String, Option<String>>,
}

/// Opens the table at `uri`, at `version` when given, else at the latest version, creating it
/// from `create` when there is none.
pub(crate) async fn open(
    uri: &str,
    options: &StorageOptions,
    version: Option<u64>,
    create: Option<&Spec>,
    store: Option<&Arc<dyn ObjectStore>>,
) -> Result<DeltaTable> {
    let mut t = table::handle_in(uri, options, store)?;
    if t.verify_deltatable_existence().await? {
        t = table::load_at(t.log_store(), version).await?;
    } else {
        let Some(spec) = create.filter(|_| version.is_none()) else {
            return Err(Error::NotFound(format!("no kv0 table at {uri}")));
        };
        if is_local(uri) && store.is_none() {
            let dir = uri.strip_prefix("file://").unwrap_or(uri);
            std::fs::create_dir_all(dir).map_err(|e| Error::Io(format!("{dir}: {e}")))?;
        }
        let created = t
            .create()
            .with_columns(spec.columns.clone())
            .with_configuration(spec.config.clone())
            .with_raise_if_key_not_exists(false)
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_commit_properties(CommitProperties::default().with_max_retries(0))
            .await;
        t = match created {
            Ok(t) => t,
            Err(e) => {
                // Another creator won; open its table. Any other failure is reported as is.
                match table::load_at(t.log_store(), None).await {
                    Ok(again) => again,
                    Err(_) => return Err(e.into()),
                }
            }
        };
    }
    check(&t, uri).await?;
    Ok(t)
}

/// Refuses tables kv0 cannot read correctly, and trips on a doubly created table.
async fn check(t: &DeltaTable, uri: &str) -> Result<()> {
    let snap = t.snapshot()?;
    let p = snap.protocol();
    // Column mapping, deletion vectors and the like all need reader version 2 or 3.
    if p.min_reader_version() > 1 {
        return Err(Error::NotSupported(format!(
            "{uri} uses Delta reader features kv0 does not implement (reader version {})",
            p.min_reader_version()
        )));
    }
    // Writer versions 3 to 6 bring check constraints, change data feed, generated columns,
    // column mapping and identity columns, which kv0's writes would not honour. Version 7 lists
    // its features; only those kv0 can live with pass.
    let writer = serde_json::to_value(p)?;
    let unknown: Vec<String> = match p.min_writer_version() {
        ..=2 => Vec::new(),
        7 => writer
            .get("writerFeatures")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .filter(|f| !["appendOnly", "invariants"].contains(f))
            .map(str::to_string)
            .collect(),
        v => vec![format!("writer version {v}")],
    };
    if !unknown.is_empty() {
        return Err(Error::NotSupported(format!(
            "{uri} uses Delta writer features kv0 does not implement: {}",
            unknown.join(", ")
        )));
    }
    let config = snap.metadata().configuration();
    // kv0's own tables say which version of kv0's layout they follow; a table without it was
    // written by another tool, one with a newer major version by a newer kv0.
    let format = config.get(super::props::FORMAT_KEY);
    let major = format
        .and_then(|f| f.strip_prefix("delta/"))
        .and_then(|v| v.split('.').next())
        .and_then(|v| v.parse::<u32>().ok());
    match major {
        Some(m) if m <= crate::storage::format::FORMAT_VERSION => {}
        Some(_) => {
            return Err(Error::NotSupported(format!(
                "{uri} was written in kv0 table format {}; this kv0 ({}) reads delta/{} and earlier. Upgrade kv0",
                format.map(String::as_str).unwrap_or_default(),
                env!("CARGO_PKG_VERSION"),
                crate::storage::format::FORMAT_VERSION
            )))
        }
        None => {
            return Err(Error::NotSupported(format!(
                "{uri} is a Delta Lake table that kv0 did not create (no `{}` table property)",
                super::props::FORMAT_KEY
            )))
        }
    }
    for (key, why) in [
        (
            "delta.enableChangeDataFeed",
            "kv0 rewrites files without recording changes",
        ),
        ("delta.appendOnly", "kv0 compacts and deletes"),
    ] {
        if config
            .get(key)
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
        {
            return Err(Error::NotSupported(format!(
                "{uri} sets {key}, which kv0 cannot honour ({why})"
            )));
        }
    }
    if snap.version() >= 1 {
        static CHECKED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
        let first = CHECKED
            .lock()
            .expect("not poisoned")
            .get_or_insert_with(HashSet::new)
            .insert(uri.to_string());
        if first {
            if let Some(body) = t.log_store().read_commit_entry(1).await? {
                for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                    let action: serde_json::Value = serde_json::from_slice(line)?;
                    if action.get("protocol").is_some() || action.get("metaData").is_some() {
                        return Err(Error::Corruption(format!(
                            "{uri}: version 1 creates the table a second time (concurrent creation, delta-rs issue #4982)"
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}
