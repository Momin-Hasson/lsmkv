import os
import random
import tempfile
import unittest

from lsmkv import DB
from lsmkv.bloom import BloomFilter
from lsmkv.wal import WAL, PUT


class DBTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.dir = self._td.name

    def tearDown(self):
        self._td.cleanup()

    def test_put_get_delete(self):
        with DB(self.dir) as db:
            db.put(b"a", b"1")
            self.assertEqual(db.get(b"a"), b"1")
            db.delete(b"a")
            self.assertIsNone(db.get(b"a"))

    def test_survives_restart_via_wal(self):
        db = DB(self.dir)
        db.put(b"k", b"v")
        db._wal.close()  # simulate crash: no flush
        with DB(self.dir) as db2:
            self.assertEqual(db2.get(b"k"), b"v")

    def test_torn_wal_tail_is_ignored(self):
        db = DB(self.dir)
        db.put(b"good", b"1")
        db.put(b"bad", b"2")
        db._wal.close()
        p = os.path.join(self.dir, "wal.log")
        with open(p, "r+b") as f:
            f.truncate(os.path.getsize(p) - 3)
        with DB(self.dir) as db2:
            self.assertEqual(db2.get(b"good"), b"1")
            self.assertIsNone(db2.get(b"bad"))

    def test_flush_and_newest_wins_across_tables(self):
        with DB(self.dir, compact_at=99) as db:
            db.put(b"k", b"old"); db.flush()
            db.put(b"k", b"new"); db.flush()
            self.assertEqual(db.get(b"k"), b"new")
            db.delete(b"k"); db.flush()
            self.assertIsNone(db.get(b"k"))

    def test_compaction_drops_tombstones(self):
        with DB(self.dir, compact_at=99) as db:
            for i in range(50):
                db.put(f"k{i:03}".encode(), b"x")
            db.flush()
            for i in range(0, 50, 2):
                db.delete(f"k{i:03}".encode())
            db.flush()
            db.compact()
            self.assertEqual(len([f for f in os.listdir(self.dir) if f.endswith(".sst")]), 1)
            self.assertEqual(len(list(db.scan())), 25)
            self.assertIsNone(db.get(b"k000"))

    def test_scan_range_ordered(self):
        with DB(self.dir, compact_at=99) as db:
            for i in [5, 1, 9, 3, 7]:
                db.put(f"k{i}".encode(), str(i).encode())
            db.flush()
            db.put(b"k4", b"4")
            self.assertEqual([k for k, _ in db.scan(b"k3", b"k8")], [b"k3", b"k4", b"k5", b"k7"])

    def test_randomized_against_dict_model(self):
        rng = random.Random(1234)
        model: dict[bytes, bytes] = {}
        with DB(self.dir, memtable_limit=512, compact_at=3) as db:
            for _ in range(3000):
                k = f"key{rng.randrange(200):03}".encode()
                if rng.random() < 0.3:
                    db.delete(k); model.pop(k, None)
                else:
                    v = str(rng.random()).encode()
                    db.put(k, v); model[k] = v
            for k in {f"key{i:03}".encode() for i in range(200)}:
                self.assertEqual(db.get(k), model.get(k))
            self.assertEqual(list(db.scan()), sorted(model.items()))
        with DB(self.dir) as db2:  # reopen
            self.assertEqual(list(db2.scan()), sorted(model.items()))


class BloomTests(unittest.TestCase):
    def test_no_false_negatives_and_low_fp(self):
        bf = BloomFilter.for_capacity(2000, 0.01)
        for i in range(2000):
            bf.add(f"in{i}".encode())
        self.assertTrue(all(f"in{i}".encode() in bf for i in range(2000)))
        fp = sum(f"out{i}".encode() in bf for i in range(5000)) / 5000
        self.assertLess(fp, 0.03)


if __name__ == "__main__":
    unittest.main()
