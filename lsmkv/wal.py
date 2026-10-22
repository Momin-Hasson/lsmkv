"""Write-ahead log. Each record is CRC-protected; a torn tail is discarded on replay."""
import os
import struct
import zlib
from typing import Iterator

_HDR = struct.Struct("<IBII")  # crc, op, klen, vlen
PUT, DEL = 1, 2


class WAL:
    def __init__(self, path: str, sync: bool = False):
        self.path = path
        self.sync = sync
        self._f = open(path, "ab")

    def append(self, op: int, key: bytes, value: bytes = b"") -> None:
        body = struct.pack("<BII", op, len(key), len(value)) + key + value
        self._f.write(struct.pack("<I", zlib.crc32(body)) + body)
        self._f.flush()
        if self.sync:
            os.fsync(self._f.fileno())

    def reset(self) -> None:
        self._f.close()
        self._f = open(self.path, "wb")
        self._f.close()
        self._f = open(self.path, "ab")

    def close(self) -> None:
        self._f.close()

    @staticmethod
    def replay(path: str) -> Iterator[tuple[int, bytes, bytes]]:
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            while True:
                hdr = f.read(_HDR.size)
                if len(hdr) < _HDR.size:
                    return
                crc, op, klen, vlen = _HDR.unpack(hdr)
                payload = f.read(klen + vlen)
                if len(payload) < klen + vlen:
                    return
                body = struct.pack("<BII", op, klen, vlen) + payload
                if zlib.crc32(body) != crc:
                    return  # corruption / torn write: stop at last good record
                yield op, payload[:klen], payload[klen:]
