import threading
from quenouille import imap_unordered


def worker(item):
    pass


for i in range(1_000):
    print("Pass %i - (%i active threads)" % (i + 1, threading.active_count()))
    for _ in imap_unordered(range(1_000), worker, 100):
        pass
