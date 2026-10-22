# lsmkv

[![CI](https://github.com/Momin-Hasson/lsmkv/actions/workflows/ci.yml/badge.svg)](https://github.com/Momin-Hasson/lsmkv/actions/workflows/ci.yml)

A small, crash-safe **LSM-tree key-value store** in pure Python (stdlib only, ~350 lines). It implements the same core design as LevelDB/RocksDB/Cassandra: write-ahead log, memtable, immutable sorted tables, Bloom filters, and compaction.

```python
from lsmkv import DB

with DB("./data") as db:
    db.put(b"user:1", b"ada")
    db.delete(b"user:2")
    db.get(b"user:1")                    # b"ada"
    list(db.scan(b"user:", b"user;"))    # ordered range scan
```

## Design

```
 put/delete ──► WAL (CRC32 per record, append + flush) ──► memtable (dict)
                                                              │ size ≥ limit
                                                              ▼
                                   SSTable  [records | sparse index | bloom | footer]
                                                              │ ≥ N tables
                                                              ▼
                                     k-way heap merge compaction (drops tombstones)
```

- **Durability:** every write hits the WAL first. Replay stops at the first record with a bad CRC, so a torn write loses only the unfinished tail. `sync=True` adds `fsync` per write.
- **Atomic flush/compaction:** tables are written to `*.tmp`, fsynced, then `os.replace`d. Leftover temp files from a crash are cleaned on open. Replaying a WAL whose flush already completed is idempotent.
- **Reads:** memtable → SSTables newest-first. Each table checks a Bloom filter (1% target FP rate), binary-searches a sparse in-memory index (every 16th key), then scans one small block.
- **Deletes:** tombstones shadow older versions and are physically dropped during full compaction.
- **Scans:** a k-way `heapq.merge` across memtable and all tables; the newest source wins per key.

## Tests

```bash
python -m unittest discover -s tests -t .
```

Includes a randomized model-based test (3,000 mixed ops with tiny memtables to force constant flushes and compactions, checked against a `dict`, then re-verified after reopen), torn-WAL recovery, and a Bloom false-positive-rate check.

## Benchmark

`python bench.py` (100k keys, 100-byte values, Python 3.14, Apple Silicon laptop):

| op | throughput |
|---|---|
| sequential writes | ~140k ops/s |
| random hit reads | ~48k ops/s |
| random miss reads (Bloom short-circuit) | ~860k ops/s |

## Known limitations

Deliberate scope cuts, not oversights: single-process (no file lock), full compaction only (no leveled/tiered strategy), no block cache or compression, and the manifest is implicit in filenames.

## CLI

```bash
python -m lsmkv ./data     # put k v | get k | del k | scan [a] [b] | flush | compact
```

MIT licensed.
