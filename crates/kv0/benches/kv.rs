//! Key-value micro-benchmarks on a local directory: buffered puts, flush, gets and scans.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kv0::kv::{Db, Options};

fn key(i: u32) -> Vec<u8> {
    format!(
        "seqs/chunks/run{:04}/metric{:02}/val/{:08}",
        i % 50,
        (i / 50) % 20,
        i
    )
    .into_bytes()
}

fn opts() -> Options {
    Options {
        flush_interval: None,
        write_buffer_size: usize::MAX,
        ..Options::default()
    }
}

fn bench(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let n = 50_000u32;
    let db = Db::open(dir.path().join("read").to_str().unwrap(), opts()).unwrap();
    for i in 0..n {
        db.put(&key(i), &i.to_le_bytes()).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("put_10k_and_flush", |b| {
        let mut round = 0;
        b.iter_batched(
            || {
                round += 1;
                Db::open(
                    dir.path().join(format!("w{round}")).to_str().unwrap(),
                    opts(),
                )
                .unwrap()
            },
            |w| {
                for i in 0..10_000 {
                    w.put(&key(i), b"value").unwrap();
                }
                w.flush().unwrap();
            },
            BatchSize::PerIteration,
        )
    });

    c.bench_function("get_hit", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = (i + 7919) % n;
            db.get(&key(i)).unwrap()
        })
    });

    c.bench_function("seek_and_next_100", |b| {
        let mut r = 0u32;
        b.iter(|| {
            r = (r + 1) % 50;
            let mut it = db.iter();
            it.seek(format!("seqs/chunks/run{r:04}/").as_bytes());
            it.take(100).count()
        })
    });

    c.bench_function("full_scan_50k", |b| {
        b.iter(|| {
            let mut it = db.iter();
            it.seek_to_first();
            it.count()
        })
    });

    c.bench_function("open_50k", |b| {
        b.iter(|| Db::open(dir.path().join("read").to_str().unwrap(), opts()).unwrap())
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
