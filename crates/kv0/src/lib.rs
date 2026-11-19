//! # kv0
//!
//! Storage for experiment trackers on object storage: a local directory, S3 and S3-compatible
//! stores, or `memory://`. Data lives in [moraine](https://docs.rs/morainedb) databases, whose
//! tables are Delta Lake tables committed with one conditional write each.
//!
//! Two layers share one storage engine:
//!
//! - [`kv`]: an ordered key-value store (`get`, `put`,
//!   `delete`, `delete_range`, atomic batches, seekable iterators in byte-wise key order).
//! - [`tracking`]: typed, columnar tables for runs, parameters and metric series.
//!
//! ```no_run
//! use kv0::kv::{Db, Options};
//!
//! let db = Db::open("/tmp/example-db", Options::default())?;
//! db.put(b"key", b"value")?;
//! assert_eq!(db.get(b"key")?.as_deref(), Some(&b"value"[..]));
//! db.close()?;
//! # Ok::<(), kv0::Error>(())
//! ```

mod background;
mod clock;
pub mod error;
pub mod kv;
pub mod purge;
pub mod rt;
pub mod storage;
#[doc(hidden)]
pub mod testing;

pub use error::{Error, Result};
pub use purge::PurgeReport;
pub use storage::StorageOptions;
pub mod tracking;
