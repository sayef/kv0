The same harness and machine as [REPORT.md](REPORT.md), 5 repetitions per cell, the same
significance rule (non-overlapping ranges and more than 20% apart). The two builds were measured
one after the other on the same day and machine, on local storage only. The machine is shared:
the last remeasurement of the bulk, large-value and reopen cells started at a load average of 13
and ended at 3, which widens the write-throughput ranges; the memory and reopen results do not
depend on it.

- **before**: kv0 before these changes: the view as a `BTreeMap` of shared byte buffers, every value
  in memory, every open a full scan of the dataset.
- **after**: with these changes: one allocation per view entry, values over 4 KiB left in Lance
  (`lazy_value_threshold`) behind a 64 MiB cache, rows applied batch by batch while loading, and
  a view snapshot saved on close (`snapshot_min_entries`, 100 000) that the next open loads
  before reading only newer fragments.

The read cells open a dataset that the `bulk` cell built and closed, so "after" reopens from the
snapshot that close saved; the first open on a machine without a snapshot costs what "before"
costs, less the memory saved by the compact entries. Writable reopen opens the same dataset
without `read_only`.
