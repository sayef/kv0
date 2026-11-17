//! The `kv0` command line: `kv0 migrate <from> <to>` converts a database or repository written
//! with the Lance engine to Delta Lake. Built with the `lance-migrate` feature:
//! `cargo install kv0 --features lance-migrate`.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(kv0::migrate::cli(&args));
}
