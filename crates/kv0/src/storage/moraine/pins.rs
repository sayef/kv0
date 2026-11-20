//! kv0 tags as moraine pins.
//!
//! A tag names a version and keeps it readable through vacuum; a moraine pin does exactly that
//! (moraine D49). Pin names are `[a-z0-9][a-z0-9_-]{0,63}`, tag names any string, so a tag's pin
//! is `t` and the base32 of its name, cut with a hash when long; the name of a cut one is kept
//! in `_kv0/tags/<table>/<pin>`, written before the pin.
//!
//! moraine moves a pin that is pinned again, while kv0's tags are created once: two handles
//! creating one tag at the same time must not both succeed. Creating a tag therefore takes a
//! moraine lease named after the pin first, and checks again under it that the tag does not
//! exist. The losing handle finds the lease held or, after the winner released it, the pin.
//!
//! moraine keeps no old version readable but pinned ones, so reading a version means reading
//! the latest one or a tagged one.

use std::time::Duration;

use bytes::Bytes;
use object_store::{ObjectStoreExt as _, PutMode, PutOptions};

use super::{base32, error, unbase32, Repo};
use crate::error::{Error, Result};

/// Encoded characters kept before the hash, when the tag's name is too long.
const KEPT: usize = 46;

/// Where the names of tags with a cut pin name are kept.
const NAMES: &str = "_kv0/tags";

/// How long the lease that guards a tag's creation lasts at most.
const LEASE_TTL: Duration = Duration::from_secs(30);

/// The pin of tag `tag`.
pub(crate) fn pin_name(tag: &str) -> String {
    let enc = base32(tag.as_bytes());
    if enc.len() < 64 {
        format!("t{enc}")
    } else {
        let h = xxhash_rust::xxh3::xxh3_64(tag.as_bytes());
        format!("t{}-{h:016x}", &enc[..KEPT])
    }
}

/// The tag of pin `pin`, when its name is not cut.
fn decode(pin: &str) -> Option<String> {
    let enc = pin.strip_prefix('t')?;
    if enc.contains('-') {
        return None;
    }
    String::from_utf8(unbase32(enc)?).ok()
}

/// The pins of a table: its moraine handle, the repository it is in, and its moraine name.
pub(crate) struct Pins<'a> {
    pub repo: &'a Repo,
    pub table: &'a moraine::Table,
    pub name: &'a str,
}

impl Pins<'_> {
    fn names_path(&self, pin: &str) -> object_store::path::Path {
        self.repo.path(&format!("{NAMES}/{}/{pin}", self.name))
    }

    /// The tags and the versions they name, as of the newest version.
    pub(crate) async fn list(&self) -> Result<Vec<(String, u64)>> {
        self.table.refresh().await.map_err(error)?;
        let mut out = Vec::new();
        for (pin, v) in self.table.pins().map_err(error)? {
            match decode(&pin) {
                Some(tag) => out.push((tag, v)),
                None => match self.repo.store().get(&self.names_path(&pin)).await {
                    Ok(r) => {
                        let body = r.bytes().await?;
                        out.push((
                            String::from_utf8(body.to_vec())
                                .map_err(|e| Error::Corruption(format!("tag of pin {pin}: {e}")))?,
                            v,
                        ));
                    }
                    // Not a pin kv0 made.
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => return Err(e.into()),
                },
            }
        }
        Ok(out)
    }

    /// The version tag `tag` names, as of the newest version.
    pub(crate) async fn version(&self, tag: &str) -> Result<u64> {
        self.table.refresh().await.map_err(error)?;
        self.pinned(tag)?
            .ok_or_else(|| Error::NotFound(format!("tag {tag:?} of {}", self.name)))
    }

    fn pinned(&self, tag: &str) -> Result<Option<u64>> {
        Ok(self
            .table
            .pins()
            .map_err(error)?
            .get(&pin_name(tag))
            .copied())
    }

    /// Tags `version` as `tag`; fails when the tag exists, another handle is creating it, or
    /// the version can no longer be kept (vacuum may have taken its files).
    pub(crate) async fn create(&self, tag: &str, version: u64) -> Result<()> {
        let exists = || Error::Conflict(format!("tag {tag:?} of {} exists", self.name));
        self.table.refresh().await.map_err(error)?;
        if self.pinned(tag)?.is_some() {
            return Err(exists());
        }
        let pin = pin_name(tag);
        let job = format!(
            "kv0-tag-{:016x}",
            xxhash_rust::xxh3::xxh3_64(pin.as_bytes())
        );
        let Some(lease) = self.table.try_lease(&job, LEASE_TTL).await.map_err(error)? else {
            return Err(Error::Conflict(format!(
                "tag {tag:?} of {} is being created by another handle",
                self.name
            )));
        };
        let r = self.create_leased(tag, &pin, version).await;
        if let Err(e) = self.table.release_lease(&lease).await {
            tracing::debug!(error = %e, "kv0: releasing a tag lease failed; it expires");
        }
        r.map_err(|e| match e {
            Error::NotFound(m) => Error::NotFound(format!(
                "version {version} of {} can no longer be tagged: {m}",
                self.name
            )),
            other => other,
        })
    }

    async fn create_leased(&self, tag: &str, pin: &str, version: u64) -> Result<()> {
        // Under the lease, against the newest version: a winner released it after pinning.
        self.table.refresh().await.map_err(error)?;
        if self.pinned(tag)?.is_some() {
            return Err(Error::Conflict(format!(
                "tag {tag:?} of {} exists",
                self.name
            )));
        }
        if decode(pin).is_none() {
            let put = self
                .repo
                .store()
                .put_opts(
                    &self.names_path(pin),
                    Bytes::copy_from_slice(tag.as_bytes()).into(),
                    PutOptions::from(PutMode::Create),
                )
                .await;
            match put {
                Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.table.pin(pin, version).await.map_err(error)
    }

    /// Removes tag `tag`.
    pub(crate) async fn delete(&self, tag: &str) -> Result<()> {
        self.table.refresh().await.map_err(error)?;
        if self.pinned(tag)?.is_none() {
            return Err(Error::NotFound(format!("tag {tag:?} of {}", self.name)));
        }
        self.table.unpin(&pin_name(tag)).await.map_err(error)
    }

    /// The snapshot tag `tag` names.
    pub(crate) async fn snapshot(&self, tag: &str) -> Result<moraine::TableSnapshot> {
        self.table.refresh().await.map_err(error)?;
        if self.pinned(tag)?.is_none() {
            return Err(Error::NotFound(format!("tag {tag:?} of {}", self.name)));
        }
        self.table
            .snapshot_at_pin(&pin_name(tag))
            .await
            .map_err(error)
    }

    /// The snapshot of version `version`: `current` when it is that version, else the newest
    /// version or a tagged one. moraine keeps no other old version readable.
    pub(crate) async fn at_version(
        &self,
        current: &moraine::TableSnapshot,
        version: u64,
    ) -> Result<moraine::TableSnapshot> {
        if current.version() == version {
            return Ok(current.clone());
        }
        self.table.refresh().await.map_err(error)?;
        let latest = self.table.snapshot_in_hand();
        if latest.version() == version {
            return Ok(latest);
        }
        let pins = self.table.pins().map_err(error)?;
        match pins.iter().find(|(_, v)| **v == version) {
            Some((pin, _)) => self.table.snapshot_at_pin(pin).await.map_err(error),
            None => Err(Error::NotFound(format!(
                "version {version} of {} is not kept: the moraine engine keeps the latest \
                 version and tagged ones readable (tag a version to read it later)",
                self.name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_names_are_valid_and_decode() {
        let valid = |n: &str| {
            n.len() <= 64
                && n.bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-')
        };
        for tag in ["", "v1", "qa-v1", "Ünïcode/tag name", &"x".repeat(39)] {
            let p = pin_name(tag);
            assert!(valid(&p), "{p}");
            assert_eq!(decode(&p).as_deref(), Some(tag));
        }
        let long = "y".repeat(100);
        let p = pin_name(&long);
        assert!(valid(&p), "{p}");
        assert_eq!(decode(&p), None);
        assert_ne!(p, pin_name(&format!("{long}z")));
    }
}
