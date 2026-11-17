//! Opening a table, creating it when it does not exist.
//!
//! delta-rs commits a table's creation without a read snapshot, so two creators racing can
//! both "win": the second retries and commits another protocol and metadata at version 1
//! (delta-rs issue #4982). kv0 creates with no commit retries, so the loser fails and opens the
//! winner's table instead. As a tripwire, the first open of a table in a process checks that
//! version 1 holds no protocol or metadata action.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::kernel::StructField;
use deltalake_core::protocol::SaveMode;
use deltalake_core::DeltaTable;

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
) -> Result<DeltaTable> {
    let mut t = table::handle(uri, options)?;
    if t.verify_deltatable_existence().await? {
        match version {
            Some(v) => t.load_version(v).await?,
            None => t.load().await?,
        }
    } else {
        let Some(spec) = create.filter(|_| version.is_none()) else {
            return Err(Error::NotFound(format!("no kv0 table at {uri}")));
        };
        if is_local(uri) {
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
                let mut again = DeltaTable::new(t.log_store());
                match again.load().await {
                    Ok(()) => again,
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
