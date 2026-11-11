//! Values above `lazy_value_threshold` leave memory once committed and are read from Lance.

use kv0::kv::{Db, Options};

fn opts() -> Options {
    Options {
        flush_interval: None,
        lazy_value_threshold: 16,
        ..Options::default()
    }
}

fn tmp() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("db").to_string_lossy().into_owned();
    (dir, p)
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn big(i: u32, gen: u32) -> Vec<u8> {
    format!("{gen}-{i}-").repeat(10).into_bytes()
}

fn all(db: &Db) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = db.iter();
    it.seek_to_first();
    it.map(|(k, v)| (k.to_vec(), v.to_vec())).collect()
}

fn expected(n: u32, gen: impl Fn(u32) -> u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let v = if i.is_multiple_of(4) {
                b"small".to_vec()
            } else {
                big(i, gen(i))
            };
            (key(i), v)
        })
        .collect()
}

fn write(db: &Db, n: u32, gen: impl Fn(u32) -> u32) {
    for (k, v) in expected(n, gen) {
        db.put(&k, &v).unwrap();
    }
}

fn check(db: &Db, n: u32, gen: impl Fn(u32) -> u32 + Copy) {
    let want = expected(n, gen);
    assert_eq!(all(db), want);
    for (k, v) in &want {
        assert_eq!(db.get(k).unwrap().as_deref(), Some(&v[..]));
    }
    let ks: Vec<&[u8]> = want.iter().map(|(k, _)| &k[..]).collect();
    let got = db.multi_get(&ks).unwrap();
    assert_eq!(got.len(), want.len());
    for (g, (_, v)) in got.iter().zip(&want) {
        assert_eq!(g.as_deref(), Some(&v[..]));
    }
    let mut it = db.iter();
    it.seek_for_prev(&key(n));
    assert_eq!(
        it.current().map(|(_, v)| v.to_vec()),
        Some(want.last().unwrap().1.clone())
    );
}

#[test]
fn large_values_leave_memory_and_read_back() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    write(&db, 200, |_| 0);
    assert_eq!(db.stored_values(), 0, "unflushed values stay in memory");
    check(&db, 200, |_| 0);
    db.flush().unwrap();
    assert_eq!(db.stored_values(), 150);
    check(&db, 200, |_| 0);

    // Overwrites are in memory until flushed, then replace the stored ones.
    for i in (0..200u32).filter(|i| i.is_multiple_of(3)) {
        let v = if i.is_multiple_of(4) {
            b"small".to_vec()
        } else {
            big(i, 1)
        };
        db.put(&key(i), &v).unwrap();
    }
    let gen = |i: u32| u32::from(i.is_multiple_of(3));
    check(&db, 200, gen);
    db.flush().unwrap();
    check(&db, 200, gen);

    // A full rewrite moves every row; the view follows.
    db.compact().unwrap();
    assert_eq!(db.stored_values(), 150);
    check(&db, 200, gen);
    db.close().unwrap();

    let db = Db::open(&p, opts()).unwrap();
    assert_eq!(db.stored_values(), 150);
    check(&db, 200, gen);

    // Keeping everything in memory reads the same data.
    let all_in = Db::open(
        &p,
        Options {
            lazy_value_threshold: 0,
            read_only: true,
            ..opts()
        },
    )
    .unwrap();
    assert_eq!(all_in.stored_values(), 0);
    check(&all_in, 200, gen);
}

#[test]
fn file_merging_compaction_moves_stored_values() {
    let (_d, p) = tmp();
    let db = Db::open(
        &p,
        Options {
            max_fragments: 4,
            ..opts()
        },
    )
    .unwrap();
    // Mostly live rows in many small fragments: compaction merges files.
    let want = expected(200, |_| 0);
    for i in 0..40 {
        for (k, v) in &want[i as usize * 5..(i as usize + 1) * 5] {
            db.put(k, v).unwrap();
        }
        db.flush().unwrap();
        check(&db, (i + 1) * 5, |_| 0);
    }
    let s = db.stats();
    assert!(s.fragments <= 5, "{s:?}");
    assert!(
        s.stored_rows < 2 * 200,
        "merged files, not rewritten: {s:?}"
    );
    assert_eq!(db.stored_values(), 150);
    check(&db, 200, |_| 0);
}

#[test]
fn another_writers_compaction_is_followed_on_refresh() {
    let (_d, p) = tmp();
    let a = Db::open(&p, opts()).unwrap();
    write(&a, 100, |_| 0);
    a.flush().unwrap();
    let b = Db::open(
        &p,
        Options {
            refresh_interval: None,
            ..opts()
        },
    )
    .unwrap();
    assert_eq!(b.stored_values(), 75);
    check(&b, 100, |_| 0);
    a.compact().unwrap();
    // b still reads the version its addresses refer to.
    check(&b, 100, |_| 0);
    assert!(b.refresh().unwrap());
    check(&b, 100, |_| 0);
    assert_eq!(b.stored_values(), 75);
    // And a cache that holds nothing still reads correctly.
    let c = Db::open(
        &p,
        Options {
            value_cache_size: 0,
            read_only: true,
            ..opts()
        },
    )
    .unwrap();
    check(&c, 100, |_| 0);
}

#[test]
fn values_moved_and_cleaned_up_by_another_writer_still_read() {
    // Review M1: a reader's addresses point at files that another writer's compaction and
    // cleanup delete. Reads refresh and find the values at their new addresses.
    for refresh in [None, Some(std::time::Duration::from_secs(1))] {
        let (_d, p) = tmp();
        let a = Db::open(&p, opts()).unwrap();
        for i in 0..50u32 {
            a.put(&key(i), &vec![i as u8; 8192]).unwrap();
        }
        a.flush().unwrap();
        let b = Db::open(
            &p,
            Options {
                read_only: true,
                refresh_interval: refresh,
                value_cache_size: 0,
                snapshot_min_entries: usize::MAX,
                ..opts()
            },
        )
        .unwrap();
        assert_eq!(b.get(&key(1)).unwrap().unwrap()[..], vec![1u8; 8192][..]);
        a.put(b"x", b"y").unwrap();
        a.compact().unwrap();
        assert!(a.cleanup_old_versions(std::time::Duration::ZERO).unwrap() > 0);
        assert_eq!(b.get(&key(2)).unwrap().unwrap()[..], vec![2u8; 8192][..]);

        // Again for an iterator and multi_get, after a second compaction and cleanup.
        a.put(b"y", b"z").unwrap();
        a.compact().unwrap();
        a.cleanup_old_versions(std::time::Duration::ZERO).unwrap();
        let got = b.multi_get(&[key(3), key(4)]).unwrap();
        assert_eq!(got[1].as_deref(), Some(&vec![4u8; 8192][..]));
        a.put(b"z", b"w").unwrap();
        a.compact().unwrap();
        a.cleanup_old_versions(std::time::Duration::ZERO).unwrap();
        let mut it = b.iter();
        it.seek_to_first();
        let n = it
            .filter(|(k, v)| k.starts_with(b"k") && v.len() == 8192)
            .count();
        assert_eq!(n, 50);
    }
}

#[test]
fn concurrent_misses_on_one_value_read_correctly() {
    let (_d, p) = tmp();
    let db = Db::open(&p, opts()).unwrap();
    db.put(&key(0), &vec![7u8; 100_000]).unwrap();
    db.flush().unwrap();
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                for _ in 0..20 {
                    assert_eq!(db.get(&key(0)).unwrap().unwrap().len(), 100_000);
                }
            });
        }
    });
}
