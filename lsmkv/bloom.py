"""Bloom filter used by SSTables to skip files that cannot contain a key."""
import hashlib
import math


class BloomFilter:
    def __init__(self, nbits: int, nhashes: int, bits: bytearray | None = None):
        self.nbits = max(8, nbits)
        self.nhashes = max(1, nhashes)
        self.bits = bits if bits is not None else bytearray((self.nbits + 7) // 8)

    @classmethod
    def for_capacity(cls, n: int, fp_rate: float = 0.01) -> "BloomFilter":
        n = max(1, n)
        nbits = math.ceil(-n * math.log(fp_rate) / (math.log(2) ** 2))
        nhashes = max(1, round(nbits / n * math.log(2)))
        return cls(nbits, nhashes)

    def _positions(self, key: bytes):
        # Double hashing: g_i(x) = h1(x) + i*h2(x)
        d = hashlib.blake2b(key, digest_size=16).digest()
        h1 = int.from_bytes(d[:8], "little")
        h2 = int.from_bytes(d[8:], "little") | 1
        for i in range(self.nhashes):
            yield (h1 + i * h2) % self.nbits

    def add(self, key: bytes) -> None:
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    def __contains__(self, key: bytes) -> bool:
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def to_bytes(self) -> bytes:
        return self.nbits.to_bytes(8, "little") + self.nhashes.to_bytes(4, "little") + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        nbits = int.from_bytes(data[:8], "little")
        nhashes = int.from_bytes(data[8:12], "little")
        return cls(nbits, nhashes, bytearray(data[12:]))
