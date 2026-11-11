use std::time::Duration;

use kv0::kv::{Db, Options, WriteBatch};

fn opts() -> Options {
    Options {
        flush_interval: None,
        ..Options::default()
    }
}

fn tmp() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("db").to_string_lossy().into_owned();
    (dir, p)
}

fn keys(db: &Db) -> Vec<Vec<u8>> {
    let mut it = db.iter();
    it.seek_to_first();
    it.map(|(k, _)| k.to_vec()).collect()
}

#[test]
fn put_get_delete() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    assert_eq!(db.get(b"x").unwrap(), None);
    db.put(b"a", b"b").unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"b"[..]));
    db.delete(b"a").unwrap();
    assert_eq!(db.get(b"a").unwrap(), None);
}

#[test]
fn batch_and_delete_range() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    for k in [&b"a1"[..], b"a2", b"a3", b"b1"] {
        db.put(k, k).unwrap();
    }
    let mut b = WriteBatch::new();
    b.delete_range(b"a", b"b");
    b.put(b"key", b"v1").delete(b"key").put(b"key", b"v2");
    db.write(b).unwrap();
    assert_eq!(keys(&db), vec![b"b1".to_vec(), b"key".to_vec()]);
    assert_eq!(db.get(b"key").unwrap().as_deref(), Some(&b"v2"[..]));
}

#[test]
fn iterator_contract() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    for x in 0..300 {
        let k = x.to_string();
        db.put(k.as_bytes(), k.as_bytes()).unwrap();
    }
    let mut it = db.iter();
    assert_eq!(it.next(), None, "an unpositioned iterator is empty");
    it.seek_to_last();
    assert_eq!(
        it.map(|(k, _)| k.to_vec()).collect::<Vec<_>>(),
        vec![b"99".to_vec()]
    );

    let mut want: Vec<Vec<u8>> = (0..300).map(|x| x.to_string().into_bytes()).collect();
    want.sort();
    assert_eq!(keys(&db), want);

    let mut it = db.iter();
    it.seek(b"90");
    let got: Vec<_> = it.map(|(k, _)| k.to_vec()).collect();
    let tail: Vec<_> = want
        .iter()
        .filter(|k| k.as_slice() >= &b"90"[..])
        .cloned()
        .collect();
    assert_eq!(got, tail);

    let mut it = db.iter();
    it.seek_for_prev(b"299a");
    assert_eq!(it.current().unwrap().0.as_ref(), b"299");
    it.step();
    assert_eq!(it.current().unwrap().0.as_ref(), b"3");
}

#[test]
fn persists_across_reopen_and_compaction() {
    let (_d, p) = tmp();
    {
        let db = Db::open(&p, opts()).unwrap();
        for i in 0..1000u32 {
            db.put(&i.to_be_bytes(), b"v0").unwrap();
            db.flush().unwrap();
            if i % 100 == 99 {
                db.put(&i.to_be_bytes(), b"v1").unwrap();
            }
        }
        db.delete_range(&0u32.to_be_bytes(), &10u32.to_be_bytes())
            .unwrap();
        db.close().unwrap();
        // Automatic compaction kept the fragment count bounded.
    }
    let db = Db::open(&p, opts()).unwrap();
    assert!(db.stats().fragments <= 33, "{:?}", db.stats());
    assert_eq!(db.stats().live_keys, 990);
    assert_eq!(db.get(&5u32.to_be_bytes()).unwrap(), None);
    assert_eq!(
        db.get(&99u32.to_be_bytes()).unwrap().as_deref(),
        Some(&b"v1"[..])
    );
    db.compact().unwrap();
    let s = db.stats();
    assert_eq!(s.fragments, 1);
    assert_eq!(
        s.stored_rows,
        990 + 1 + 1,
        "live rows, one range tombstone and one WAL mark"
    );
    drop(db);
    let db = Db::open(&p, opts()).unwrap();
    assert_eq!(db.stats().live_keys, 990);
    assert_eq!(
        db.get(&99u32.to_be_bytes()).unwrap().as_deref(),
        Some(&b"v1"[..])
    );
}

#[test]
fn reader_sees_writer_after_refresh() {
    let (_d, p) = tmp();
    let w = Db::open(&p, opts()).unwrap();
    let r = Db::open(
        &p,
        Options {
            read_only: true,
            refresh_interval: None,
            ..opts()
        },
    )
    .unwrap();
    w.put(b"k", b"v").unwrap();
    assert_eq!(r.get(b"k").unwrap(), None);
    w.flush().unwrap();
    assert_eq!(r.get(b"k").unwrap(), None, "no automatic refresh");
    assert!(r.refresh().unwrap());
    assert_eq!(r.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert!(r.put(b"x", b"y").is_err());
}

#[test]
fn concurrent_writers_converge() {
    let (_d, p) = tmp();
    let a = Db::open(&p, opts()).unwrap();
    let b = Db::open(&p, opts()).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            for i in 0..50u32 {
                a.put(format!("a{i:03}").as_bytes(), b"1").unwrap();
                a.flush().unwrap();
            }
        });
        s.spawn(|| {
            for i in 0..50u32 {
                b.put(format!("b{i:03}").as_bytes(), b"2").unwrap();
                b.flush().unwrap();
            }
        });
    });
    a.put(b"shared", b"from-a").unwrap();
    a.flush().unwrap();
    b.refresh().unwrap();
    b.put(b"shared", b"from-b").unwrap();
    b.flush().unwrap();
    a.compact().unwrap();
    a.refresh().unwrap();
    b.refresh().unwrap();
    assert_eq!(keys(&a).len(), 101);
    assert_eq!(keys(&a), keys(&b));
    assert_eq!(a.get(b"shared").unwrap().as_deref(), Some(&b"from-b"[..]));
    let c = Db::open(&p, opts()).unwrap();
    assert_eq!(keys(&c), keys(&a));
}

#[test]
fn time_travel() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();
    let v = db.version();
    db.put(b"k", b"new").unwrap();
    db.flush().unwrap();
    assert!(db.versions().unwrap().len() >= 3);
    let old = Db::open(
        &p,
        Options {
            version: Some(v),
            ..opts()
        },
    )
    .unwrap();
    assert_eq!(old.get(b"k").unwrap().as_deref(), Some(&b"old"[..]));
    assert!(old.is_read_only());
    assert_eq!(
        db.cleanup_old_versions(Duration::from_secs(3600)).unwrap(),
        0
    );
}

#[test]
fn memory_uri_and_missing() {
    let db = Db::open("memory://kv-test", opts()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    let (_d, p) = tmp();
    let err = Db::open(
        &p,
        Options {
            create_if_missing: false,
            ..opts()
        },
    )
    .unwrap_err();
    assert!(matches!(err, kv0::Error::NotFound(_)), "{err:?}");
}

#[test]
fn write_buffer_triggers_flush_and_background_flush() {
    let (_d, p) = tmp();
    let db = Db::open(
        &p,
        Options {
            write_buffer_size: 1024,
            flush_interval: Some(Duration::from_millis(200)),
            ..opts()
        },
    )
    .unwrap();
    let v0 = db.version();
    for i in 0..100u32 {
        db.put(&i.to_be_bytes(), &[0u8; 32]).unwrap();
    }
    assert!(db.version() > v0, "buffer-size flush");
    db.put(b"late", b"x").unwrap();
    std::thread::sleep(Duration::from_millis(600));
    assert_eq!(db.stats().pending_ops, 0, "background flush");
}

#[test]
fn closed_handle_rejects_calls() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.close().unwrap();
    assert!(db.get(b"a").is_err());
    let db = Db::open(&p, opts()).unwrap();
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

#[test]
fn tags_name_versions_that_cleanup_keeps() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.create_tag("release", None).unwrap();
    let v1 = db.version();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    db.compact().unwrap();
    db.cleanup_old_versions(Duration::ZERO).unwrap();
    assert_eq!(db.tags().unwrap(), vec![("release".to_string(), v1)]);
    let old = Db::open(
        &p,
        Options {
            version: Some(db.tag_version("release").unwrap()),
            ..opts()
        },
    )
    .unwrap();
    assert_eq!(old.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    db.delete_tag("release").unwrap();
    assert!(db.tags().unwrap().is_empty());
}
