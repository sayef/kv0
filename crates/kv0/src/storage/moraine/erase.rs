//! Version cleanup and erasure on a moraine table.
//!
//! moraine's vacuum keeps the files a log entry removed for the table's grace period (at least
//! ten minutes, on the store's clock), so a reader still on an older version can finish. kv0's
//! version cleanup takes an age, and a purge must leave no copy of deleted data at all, so
//! after a pass of moraine's own upkeep (checkpoint, log cleanup, vacuum) kv0 removes, itself,
//! the data files removed by commits older than the cleanup's cutoff (with erasure: by every
//! commit up to the version cleaned), unless the current version or a pin still holds them.
//! Those files are known to be committed and replaced, so no writer is about to use them; a
//! reader on an older version that meets a missing file moves to the newest version, as after
//! any vacuum. moraine's vacuum later lists the store and finds them gone.
//!
//! Erasure also reaches the log. A log entry's `add` carries the file's column statistics,
//! whose minimum and maximum of a string column are values (cut at 256 characters). The log
//! itself must stay whole: a writer still on an older version puts its next entry with
//! put-if-absent, and an entry deleted below a checkpoint would take that commit where no
//! reader sees it. So erasure rewrites, in place, the entries that add files no longer kept,
//! with the minimum and maximum dropped from those files' statistics (row and null counts
//! stay; a reader only prunes less), and the operation parameters dropped from the commit
//! information. It then writes a checkpoint at the current version and deletes older
//! checkpoints, and deletes the manifests of pins that no longer exist.
//!
//! The rewritten entries are newer objects than they were, so moraine's log cleanup takes them
//! a grace period later than it would have.

use std::collections::{HashMap, HashSet};

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _};

use super::{error, Repo};
use crate::error::Result;
use crate::storage::Cleanup;

/// Log entries read for their removals: those committed within this many grace periods of
/// now. Older removals are moraine's vacuum's.
const WINDOW_GRACE_PERIODS: i32 = 2;

/// Cleans up `table` (moraine name `name`) as `c` asks. Returns the versions whose data an
/// erasure removed (with `c.erase`; 0 otherwise) and the bytes removed.
pub(crate) async fn cleanup(
    repo: &Repo,
    table: &moraine::Table,
    name: &str,
    c: Cleanup,
) -> Result<(u64, u64)> {
    let mut bytes = super::maintain(table)
        .await?
        .vacuum
        .map_or(0, |v| v.bytes_deleted);
    let snap = table.snapshot().await.map_err(error)?;
    let store = repo.store().clone();
    let data = repo.path(&format!("{name}/data"));
    let log = data.clone().join("_delta_log");

    // What must stay: the current files and every pinned version's.
    let mut keep: HashSet<String> = snap.files().into_iter().map(|f| f.path).collect();
    let pins = table.pins().map_err(error)?;
    for pin in pins.keys() {
        let pinned = table.snapshot_at_pin(pin).await.map_err(error)?;
        keep.extend(pinned.files().into_iter().map(|f| f.path));
    }

    let entries: Vec<ObjectMeta> = store.list(Some(&log)).try_collect().await?;
    let grace = table.definition().grace_period;
    let window = chrono::Utc::now()
        - chrono::Duration::from_std(grace * WINDOW_GRACE_PERIODS as u32)
            .unwrap_or(chrono::Duration::days(365));
    let mut doomed: HashSet<String> = HashSet::new();
    for m in &entries {
        let Some(v) = commit_version(&m.location) else {
            continue;
        };
        // Erasure reads every entry: an old removal may still wait for a vacuum that another
        // client's lease held off.
        if v > snap.version()
            || m.last_modified >= c.before
            || (!c.erase && m.last_modified < window)
        {
            continue;
        }
        let body = store.get(&m.location).await?.bytes().await?;
        for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            let doc: serde_json::Value = serde_json::from_slice(line)?;
            if let Some(p) = doc
                .get("remove")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.as_str())
            {
                let p = percent_decode(p);
                if !keep.contains(&p) {
                    doomed.insert(p);
                }
            }
        }
    }
    if !doomed.is_empty() {
        let sizes: HashMap<String, u64> = store
            .list_with_delimiter(Some(&data))
            .await?
            .objects
            .into_iter()
            .filter_map(|m| Some((m.location.filename()?.to_string(), m.size)))
            .collect();
        for p in &doomed {
            match store.delete(&data.clone().join(p.as_str())).await {
                Ok(()) => bytes += sizes.get(p).copied().unwrap_or(0),
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    if !c.erase {
        return Ok((0, bytes));
    }

    // Statistics of files no longer kept, in every log entry up to the version cleaned.
    let mut versions = 0;
    for m in &entries {
        let Some(v) = commit_version(&m.location) else {
            continue;
        };
        if v > snap.version() {
            continue;
        }
        let body = match store.get(&m.location).await {
            Ok(r) => r.bytes().await?,
            // Cleaned up meanwhile by moraine's log cleanup.
            Err(object_store::Error::NotFound { .. }) => continue,
            Err(e) => return Err(e.into()),
        };
        let (stripped, changed, removed_gone) = strip(&body, &keep)?;
        versions += u64::from(removed_gone);
        if changed {
            store.put(&m.location, stripped.into()).await?;
        }
    }
    // A checkpoint at the current version, so older checkpoints (with older statistics) can
    // go; readers start from the newest one.
    table.checkpoint().await.map_err(error)?;
    let listed: Vec<ObjectMeta> = store.list(Some(&log)).try_collect().await?;
    if let Some(newest) = listed
        .iter()
        .filter_map(|m| checkpoint_version(&m.location))
        .max()
    {
        for m in &listed {
            if checkpoint_version(&m.location).is_some_and(|v| v < newest) {
                match store.delete(&m.location).await {
                    Ok(()) => bytes += m.size,
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    // Manifests of pins that are gone (or moved to another version).
    table.refresh().await.map_err(error)?;
    let pins = table.pins().map_err(error)?;
    let manifests = repo.path(&format!("{name}/meta/pins"));
    let listed: Vec<ObjectMeta> = store.list(Some(&manifests)).try_collect().await?;
    for m in listed {
        let parts: Vec<String> = m
            .location
            .prefix_match(&manifests)
            .map(|p| p.map(|s| s.as_ref().to_string()).collect())
            .unwrap_or_default();
        let current = match parts.as_slice() {
            [pin, file] => file
                .strip_suffix(".json")
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|v| pins.get(pin) == Some(&v)),
            _ => true,
        };
        if !current {
            match store.delete(&m.location).await {
                Ok(()) => bytes += m.size,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok((versions, bytes))
}

/// Log entry `body` with the minimum and maximum dropped from the statistics of the files it
/// adds that `keep` does not hold, and without operation parameters; whether that changed
/// anything; and whether the entry removed a file that is not kept (its version is gone).
fn strip(body: &[u8], keep: &HashSet<String>) -> Result<(Vec<u8>, bool, bool)> {
    let mut out = Vec::with_capacity(body.len());
    let (mut changed, mut gone) = (false, false);
    for line in body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let mut doc: serde_json::Value = serde_json::from_slice(line)?;
        let mut edited = false;
        if let Some(r) = doc
            .get("remove")
            .and_then(|r| r.get("path"))
            .and_then(|p| p.as_str())
        {
            gone |= !keep.contains(&percent_decode(r));
        }
        if let Some(add) = doc.get_mut("add").and_then(|a| a.as_object_mut()) {
            let path = add.get("path").and_then(|p| p.as_str()).map(percent_decode);
            if path.is_some_and(|p| !keep.contains(&p)) {
                if let Some(text) = add.get("stats").and_then(|s| s.as_str()) {
                    let mut st: serde_json::Value = serde_json::from_str(text)?;
                    if let Some(o) = st.as_object_mut() {
                        let had = o.remove("minValues").is_some() | o.remove("maxValues").is_some();
                        if had {
                            add.insert("stats".into(), serde_json::Value::String(st.to_string()));
                            edited = true;
                        }
                    }
                }
            }
        }
        if let Some(ci) = doc.get_mut("commitInfo").and_then(|c| c.as_object_mut()) {
            edited |= ci.remove("operationParameters").is_some();
        }
        if edited {
            changed = true;
            out.extend_from_slice(serde_json::to_string(&doc)?.as_bytes());
        } else {
            out.extend_from_slice(line);
        }
        out.push(b'\n');
    }
    Ok((out, changed, gone))
}

/// The version of a log entry (`<version>.json`).
fn commit_version(p: &Path) -> Option<u64> {
    p.filename()?.strip_suffix(".json")?.parse().ok()
}

/// The version of a single-file checkpoint (`<version>.checkpoint.parquet`).
fn checkpoint_version(p: &Path) -> Option<u64> {
    p.filename()?
        .strip_suffix(".checkpoint.parquet")?
        .parse()
        .ok()
}

/// A path as a Delta log entry writes it (percent-encoded), decoded.
fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_names_and_paths_parse() {
        assert_eq!(
            commit_version(&Path::from("t/data/_delta_log/00000000000000000012.json")),
            Some(12)
        );
        assert_eq!(
            checkpoint_version(&Path::from(
                "t/data/_delta_log/00000000000000000020.checkpoint.parquet"
            )),
            Some(20)
        );
        assert_eq!(
            commit_version(&Path::from("t/data/_delta_log/_last_checkpoint")),
            None
        );
        assert_eq!(percent_decode("a%20b%2"), "a b%2");
        assert_eq!(percent_decode("part-x.parquet"), "part-x.parquet");
    }

    #[test]
    fn erasure_strips_bounds_of_files_not_kept() {
        let body = concat!(
            r#"{"commitInfo":{"timestamp":1,"operation":"DELETE","operationParameters":{"predicate":"x"}}}"#,
            "\n",
            r#"{"add":{"path":"a.parquet","size":1,"stats":"{\"numRecords\":2,\"minValues\":{\"name\":\"secret\"},\"maxValues\":{\"name\":\"secret\"},\"nullCount\":{\"name\":0}}"}}"#,
            "\n",
            r#"{"add":{"path":"b.parquet","size":1,"stats":"{\"numRecords\":1,\"minValues\":{\"name\":\"live\"}}"}}"#,
            "\n",
            r#"{"remove":{"path":"c.parquet","dataChange":true}}"#,
            "\n",
        );
        let keep: HashSet<String> = ["b.parquet".to_string()].into();
        let (out, changed, gone) = strip(body.as_bytes(), &keep).unwrap();
        let out = String::from_utf8(out).unwrap();
        assert!(changed && gone);
        assert!(
            !out.contains("secret") && !out.contains("predicate"),
            "{out}"
        );
        assert!(
            out.contains("live") && out.contains(r#"\"numRecords\":2"#),
            "{out}"
        );
        assert_eq!(out.lines().count(), 4);
    }
}
