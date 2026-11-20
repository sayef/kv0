//! Version cleanup and erasure on a moraine table.
//!
//! moraine's vacuum keeps the files a log entry removed for the table's grace period (at least
//! ten minutes, on the store's clock), so a reader still on an older version can finish. kv0's
//! version cleanup takes an age, and a purge must leave no copy of deleted data at all. So after
//! a pass of moraine's own upkeep (checkpoint, log cleanup, vacuum), a cleanup runs a moraine
//! purge (`Table::purge`, moraine D66): the files removed by commits older than the cleanup's
//! cutoff go at once, unless the current version or a pin still holds them. With erasure the
//! purge also takes their values out of the log: the statistics of the erased files in every
//! log entry, older checkpoints, and manifests of pins that no longer hold.
//!
//! A reader on an older version that meets a missing file moves to the newest version, as after
//! any vacuum. Only one client purges a table at a time (moraine's purge lease): a cleanup that
//! finds another one running leaves the work to it, an erasure waits for it.
//!
//! moraine compares a removal's commit time (the writer's clock) with the store's clock, which
//! it reads from an object's last-modified time and so sees up to that time's resolution late:
//! a millisecond tick or a coarse file system timestamp on local disk, a whole second on S3. A
//! purge right after the commit that removed a file would then keep it, and an erasure must not.
//! So an erasure first waits out that resolution.

use super::error;
use crate::error::{Error, Result};
use crate::storage::Cleanup;

/// How long kv0's purges hold moraine's purge lease (moraine's default).
const LEASE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

/// How long an erasure waits for another client's purge to finish before it fails: longer than
/// the lease, so that the lease of a purger that died (or whose release failed) expires within
/// the wait. A cleanup without erasure tries a few times and then leaves the work to that purge.
const ERASE_WAIT: std::time::Duration = std::time::Duration::from_secs(LEASE_TTL.as_secs() + 30);
const CLEANUP_ATTEMPTS: u32 = 3;

/// How long an erasure waits before its purge, so that commits just made are before the store's
/// clock: on local disk, and elsewhere (S3 last-modified times have one-second resolution).
const SETTLE_LOCAL: std::time::Duration = std::time::Duration::from_millis(25);
const SETTLE_REMOTE: std::time::Duration = std::time::Duration::from_millis(1_100);

/// Cleans up `table` as `c` asks. Returns the data files the purge erased (with `c.erase`;
/// the files it collected otherwise) and the bytes removed. `local` says the table is on local
/// disk or in memory.
pub(crate) async fn cleanup(table: &moraine::Table, c: Cleanup, local: bool) -> Result<(u64, u64)> {
    let vacuumed = super::maintain(table)
        .await?
        .vacuum
        .map_or(0, |v| v.bytes_deleted);
    let mut purge = moraine::Purge::new()
        .removed_before(c.before)
        .lease_ttl(LEASE_TTL);
    if c.erase {
        purge = purge.erase();
    }
    if c.erase {
        tokio::time::sleep(if local { SETTLE_LOCAL } else { SETTLE_REMOTE }).await;
    }
    let started = std::time::Instant::now();
    let mut attempt = 0;
    let report = loop {
        match table.purge(&purge).await {
            Ok(r) => break r,
            Err(moraine::Error::LeaseHeld { .. })
                if (c.erase && started.elapsed() < ERASE_WAIT)
                    || (!c.erase && attempt + 1 < CLEANUP_ATTEMPTS) =>
            {
                attempt += 1;
                tokio::time::sleep(crate::storage::backoff(attempt, 100, 2_000)).await;
            }
            // Another client's purge is collecting removed files now; a cleanup without
            // erasure leaves it to that one.
            Err(moraine::Error::LeaseHeld { .. }) if !c.erase => {
                tracing::debug!(
                    table = table.name(),
                    "kv0: another client is purging; cleanup skipped"
                );
                return Ok((0, vacuumed));
            }
            Err(e @ moraine::Error::LeaseHeld { .. }) => {
                return Err(Error::Conflict(format!(
                    "{e}: another handle has been purging {} for over {} s; try again later",
                    table.name(),
                    ERASE_WAIT.as_secs()
                )))
            }
            Err(e) => return Err(error(e)),
        }
    };
    let files = if c.erase {
        report.files_erased
    } else {
        report.files_deleted
    };
    Ok((files as u64, vacuumed + report.bytes_deleted))
}
