import threading
import time
from quenouille import imap_unordered


def print_active_threads(i=-1):
    print("Pass %s - (%i active threads)" % (i + 1, threading.active_count()))


def worker(item):
    return item


for i in range(1_000):
    print_active_threads(i)
    for j in imap_unordered(range(1_000), worker, 100):
        if j > 500:
            break

print_active_threads()
time.sleep(2)
print_active_threads()
