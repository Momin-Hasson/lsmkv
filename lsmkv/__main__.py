"""Tiny REPL:  python -m lsmkv ./data   then  put k v | get k | del k | scan [a] [b] | flush | compact"""
import sys

from .db import DB


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "./lsmdata"
    with DB(path) as db:
        for line in sys.stdin if not sys.stdin.isatty() else iter(lambda: input("lsm> "), None):
            parts = line.split()
            if not parts:
                continue
            cmd, args = parts[0], [a.encode() for a in parts[1:]]
            try:
                if cmd == "put":
                    db.put(args[0], b" ".join(args[1:]))
                elif cmd == "get":
                    v = db.get(args[0])
                    print(v.decode() if v is not None else "(nil)")
                elif cmd == "del":
                    db.delete(args[0])
                elif cmd == "scan":
                    for k, v in db.scan(*(args[:2])):
                        print(f"{k.decode()} = {v.decode()}")
                elif cmd == "flush":
                    db.flush()
                elif cmd == "compact":
                    db.compact()
                elif cmd in ("quit", "exit"):
                    break
                else:
                    print("commands: put get del scan flush compact quit")
            except IndexError:
                print("missing argument")


if __name__ == "__main__":
    try:
        main()
    except (EOFError, KeyboardInterrupt):
        pass
