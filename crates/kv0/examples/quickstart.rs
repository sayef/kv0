//! Key-value and tracking quickstart: `cargo run -p kv0 --example quickstart`.

use kv0::kv::{Db, Options, WriteBatch};
use kv0::tracking::{NewRun, Repo, RepoOptions, StepRange, Track};
use serde_json::json;

fn main() -> kv0::Result<()> {
    let dir = std::env::temp_dir().join(format!("kv0-quickstart-{}", std::process::id()));

    let db = Db::open(dir.join("kv").to_str().unwrap(), Options::default())?;
    let mut batch = WriteBatch::new();
    batch.put(b"key_1", b"value_1").put(b"key_2", b"value_2");
    db.write(batch)?;
    let mut it = db.iter();
    it.seek(b"key_");
    for (k, v) in it {
        println!(
            "{} = {}",
            String::from_utf8_lossy(&k),
            String::from_utf8_lossy(&v)
        );
    }
    db.close()?;

    let uri = dir.join("tracking").to_string_lossy().into_owned();
    kv0::rt::block_on(async move {
        let repo = Repo::open(&uri, RepoOptions::default()).await?;
        let run = repo.create_run(NewRun {
            name: Some("baseline".into()),
            ..NewRun::default()
        })?;
        run.set_params(&json!({"lr": 3e-4}))?;
        for step in 0..100 {
            run.track("loss", 1.0 / (step as f64 + 1.0), Track::default())?;
        }
        run.finish(kv0::tracking::RunStatus::Finished).await?;
        let tail = repo
            .read_metric(
                &run.id(),
                "loss",
                &json!({}),
                StepRange {
                    start: Some(95),
                    stop: None,
                },
            )
            .await?;
        println!("steps {:?}", tail.steps);
        repo.close().await
    })
}
