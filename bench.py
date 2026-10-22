"""Quick benchmark: python bench.py [N]"""
import random, sys, tempfile, time
from lsmkv import DB

N = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
with tempfile.TemporaryDirectory() as d, DB(d, memtable_limit=1 << 20) as db:
    keys = [f"user:{i:08}".encode() for i in range(N)]
    t = time.perf_counter()
    for k in keys:
        db.put(k, b"x" * 100)
    w = time.perf_counter() - t
    db.flush()
    random.Random(0).shuffle(keys)
    sample = keys[:20_000]
    t = time.perf_counter()
    for k in sample:
        assert db.get(k) is not None
    r = time.perf_counter() - t
    t = time.perf_counter()
    for i in range(20_000):
        assert db.get(f"miss:{i}".encode()) is None
    m = time.perf_counter() - t
    print(f"writes: {N/w:,.0f} ops/s | hit reads: {len(sample)/r:,.0f} ops/s | miss reads (bloom): {20_000/m:,.0f} ops/s")
