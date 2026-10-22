"""Immutable sorted string table.

Layout:  [records][sparse index][bloom filter][footer]
record:  klen u32 | vlen i32 (-1 = tombstone) | key | value
footer:  index_off u64 | bloom_off u64 | count u64 | magic u32
"""
import bisect
import os
import struct
from typing import Iterable, Iterator

from .bloom import BloomFilter

MAGIC = 0x4C534D31  # "LSM1"
_REC = struct.Struct("<Ii")
_FOOT = struct.Struct("<QQQI")
INDEX_EVERY = 16
TOMBSTONE = None


def write(path: str, items: Iterable[tuple[bytes, bytes | None]], expected: int) -> int:
    bloom = BloomFilter.for_capacity(expected)
    index: list[tuple[bytes, int]] = []
    count = 0
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        for key, val in items:
            if count % INDEX_EVERY == 0:
                index.append((key, f.tell()))
            bloom.add(key)
            f.write(_REC.pack(len(key), -1 if val is None else len(val)) + key + (val or b""))
            count += 1
        index_off = f.tell()
        for key, off in index:
            f.write(struct.pack("<IQ", len(key), off) + key)
        bloom_off = f.tell()
        f.write(struct.pack("<I", len(bloom.to_bytes())) + bloom.to_bytes())
        f.write(_FOOT.pack(index_off, bloom_off, count, MAGIC))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return count


class SSTable:
    def __init__(self, path: str):
        self.path = path
        with open(path, "rb") as f:
            f.seek(-_FOOT.size, os.SEEK_END)
            self.index_off, self.bloom_off, self.count, magic = _FOOT.unpack(f.read(_FOOT.size))
            if magic != MAGIC:
                raise ValueError(f"{path}: bad magic")
            f.seek(self.index_off)
            raw = f.read(self.bloom_off - self.index_off)
            self.keys: list[bytes] = []
            self.offsets: list[int] = []
            pos = 0
            while pos < len(raw):
                klen, off = struct.unpack_from("<IQ", raw, pos)
                pos += 12
                self.keys.append(raw[pos:pos + klen])
                self.offsets.append(off)
                pos += klen
            f.seek(self.bloom_off)
            blen = struct.unpack("<I", f.read(4))[0]
            self.bloom = BloomFilter.from_bytes(f.read(blen))

    def _iter_from(self, offset: int) -> Iterator[tuple[bytes, bytes | None]]:
        with open(self.path, "rb") as f:
            f.seek(offset)
            while f.tell() < self.index_off:
                klen, vlen = _REC.unpack(f.read(_REC.size))
                key = f.read(klen)
                yield key, (None if vlen < 0 else f.read(vlen))

    def get(self, key: bytes) -> tuple[bool, bytes | None]:
        """Returns (found, value); found with value None means tombstone."""
        if key not in self.bloom:
            return False, None
        i = bisect.bisect_right(self.keys, key) - 1
        if i < 0:
            return False, None
        for k, v in self._iter_from(self.offsets[i]):
            if k == key:
                return True, v
            if k > key:
                break
        return False, None

    def scan(self, start: bytes = b"", end: bytes | None = None):
        i = max(0, bisect.bisect_right(self.keys, start) - 1)
        for k, v in self._iter_from(self.offsets[i]):
            if k < start:
                continue
            if end is not None and k >= end:
                return
            yield k, v

    def __iter__(self):
        return self._iter_from(0)
