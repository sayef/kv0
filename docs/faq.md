# FAQ

## Is kv0 a database server?

No. It is a library you link into a Rust program or import in Python. Every process that opens a
database reads and writes its tables directly, through [moraine](https://github.com/sayef/moraine).

## Why moraine?

It is a client-only database of Delta Lake tables on object storage: atomic commits (one
conditional write of a log entry each), concurrent writers without a coordinator, transaction ids
for exactly-once replay, pinned versions for tags, and compaction. kv0 has no commit protocol of
its own, so the same code runs on a laptop and on a bucket. The
[design](design.md#why-moraine-and-what-it-costs) has the costs.

## Can I open data written by kv0 0.1.0-beta.2 or earlier?

No. Those releases stored data in Lance datasets, and kv0 now has only the moraine engine. Opening
such a location fails with `NotSupported`; while kv0 is in beta there is no migration. See
[Earlier formats](guides/storage.md#earlier-formats).

## How durable is a write?

It survives a killed process once `put` returns (it is in the local write-ahead log). With the
default `wal_sync="batch"`, a power loss can lose about the last 100 ms; `"always"` fsyncs every
write. Losing the machine loses writes not yet committed; `flush()` or `sync=True` commits before
returning. See [Concepts](concepts.md#durability-the-write-ahead-log).

## Can several processes write at once?

Yes, on one machine or many. Appends do not conflict; when two writers write one key, the higher
sequence number wins everywhere. There are no transactions across handles.

## How much memory does it use?

The keys of the live data, about 100 bytes per entry plus key and small value; values over 4 KiB
stay in storage. 10M keys used about 1 GB when measured on the Lance engine; the
[benchmarks](benchmarks.md) have the numbers.

## How long does opening take?

With a snapshot from a previous close on the same machine, about 70 ms per million keys. Without
one, a full scan: about 0.5 s per million keys. Both were measured on the Lance engine of kv0
0.1.0-beta.2; measurements on moraine follow.

## Does it support transactions, merge operators or column families?

`WriteBatch` is atomic. There are no multi-key transactions with reads. `merge` is accepted and
behaves as a put (last write wins); column-family arguments are accepted and every key lives in one
keyspace.

## What does it cost on S3?

A commit is a few requests and takes tens to hundreds of milliseconds, so kv0 buffers writes and
commits in batches. Reads are served from memory; a refresh reads the new log entries and data.

## Where are the benchmarks from?

One machine, five repetitions per cell, a stated significance rule, and every regression reported in
the same tables. [Benchmarks](benchmarks.md).
