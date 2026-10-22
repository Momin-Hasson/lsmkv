"""LSM-tree key-value store: WAL + memtable + SSTables + size-tiered full compaction."""
import heapq
import os
import re
import threading
from typing import Iterator

from . import sstable
from .sstable import SSTable
from .wal import DEL, PUT, WAL

_SST_RE = re.compile(r"^(\d{6})\.sst$")


class DB:
    def __init__(self, path: str, memtable_limit: int = 1 << 20, compact_at: int = 4, sync: bool = False):
        os.makedirs(path, exist_ok=True)
        self.path = path
        self.memtable_limit = memtable_limit
        self.compact_at = compact_at
        self._lock = threading.RLock()
        self._mem: dict[bytes, bytes | None] = {}
        self._mem_bytes = 0
        self._tables: list[tuple[int, SSTable]] = []  # oldest -> newest
        self._seq = 0
        for name in sorted(os.listdir(path)):
            m = _SST_RE.match(name)
            if m:
                seq = int(m.group(1))
                self._tables.append((seq, SSTable(os.path.join(path, name))))
                self._seq = max(self._seq, seq)
            elif name.endswith(".tmp"):
                os.remove(os.path.join(path, name))  # leftover from a crash mid-flush
        wal_path = os.path.join(path, "wal.log")
        for op, k, v in WAL.replay(wal_path):
            self._apply(k, v if op == PUT else None)
        self._wal = WAL(wal_path, sync=sync)

    # -- public API ---------------------------------------------------------
    def put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            self._wal.append(PUT, key, value)
            self._apply(key, value)
            self._maybe_flush()

    def delete(self, key: bytes) -> None:
        with self._lock:
            self._wal.append(DEL, key)
            self._apply(key, None)
            self._maybe_flush()

    def get(self, key: bytes) -> bytes | None:
        with self._lock:
            if key in self._mem:
                return self._mem[key]
            for _, t in reversed(self._tables):
                found, val = t.get(key)
                if found:
                    return val
            return None

    def scan(self, start: bytes = b"", end: bytes | None = None) -> Iterator[tuple[bytes, bytes]]:
        """Ordered live entries in [start, end). Newest source wins; tombstones hidden."""
        with self._lock:
            mem = sorted((k, v) for k, v in self._mem.items() if k >= start and (end is None or k < end))
            sources = [iter(mem)] + [t.scan(start, end) for _, t in reversed(self._tables)]
            merged = self._merge(sources)
            rows = [(k, v) for k, v in merged if v is not None]
        yield from rows

    def flush(self) -> None:
        with self._lock:
            if not self._mem:
                return
            self._seq += 1
            path = os.path.join(self.path, f"{self._seq:06d}.sst")
            items = sorted(self._mem.items())
            sstable.write(path, items, len(items))
            self._tables.append((self._seq, SSTable(path)))
            self._mem.clear()
            self._mem_bytes = 0
            self._wal.reset()
            if len(self._tables) >= self.compact_at:
                self.compact()

    def compact(self) -> None:
        """Merge every table into one; tombstones are dropped since nothing older remains."""
        with self._lock:
            if len(self._tables) < 2:
                return
            sources = [iter(t) for _, t in reversed(self._tables)]
            live = ((k, v) for k, v in self._merge(sources) if v is not None)
            self._seq += 1
            path = os.path.join(self.path, f"{self._seq:06d}.sst")
            total = sum(t.count for _, t in self._tables)
            sstable.write(path, live, total)
            old = self._tables
            self._tables = [(self._seq, SSTable(path))]
            for _, t in old:
                os.remove(t.path)

    def close(self) -> None:
        with self._lock:
            self.flush()
            self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- internals ----------------------------------------------------------
    def _apply(self, key: bytes, value: bytes | None) -> None:
        self._mem[key] = value
        self._mem_bytes += len(key) + (len(value) if value else 0) + 16

    def _maybe_flush(self) -> None:
        if self._mem_bytes >= self.memtable_limit:
            self.flush()

    @staticmethod
    def _merge(sources):
        """K-way merge of sorted (key, value) streams, ordered newest-first; first hit per key wins."""
        tagged = [((k, rank, v) for k, v in src) for rank, src in enumerate(sources)]
        last = None
        for k, _, v in heapq.merge(*tagged, key=lambda t: (t[0], t[1])):
            if k != last:
                last = k
                yield k, v
