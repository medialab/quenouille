import threading
import time
from quenouille import imap


def print_active_threads(i=-1):
    print("Pass %s - (%i active threads)" % (i + 1, threading.active_count()))


def worker(item):
    time.sleep(0.01)
    return item


for i in range(10):
    print_active_threads(i)
    for t in threading.enumerate():
        print(t)
    for j in imap(range(1_000), worker, 100):
        if j > 500:
            break

print_active_threads()
time.sleep(2)
print_active_threads()
