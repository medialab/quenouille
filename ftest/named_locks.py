import time
from operator import itemgetter
from quenouille import imap_unordered, NamedLocks

DATA = [("A", 1), ("B", 2), ("B", 3)]

locks = NamedLocks()


def worker(t):
    lock = locks[t[0]]
    print(t, lock)

    with lock:
        time.sleep(3)
        return t


print("start")
for i in imap_unordered(DATA, worker, 5):
    print(i)

print(locks)
