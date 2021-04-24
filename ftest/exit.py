import sys
import time
from quenouille import imap_unordered


def granular_sleep(t):
    n = t * 10

    for _ in range(n):
        time.sleep(0.1)


def worker(i):
    # granular_sleep(2)
    time.sleep(5)
    return i


try:
    for i in imap_unordered(range(100_000), worker, 25, daemonic=True, wait=False):
        print(i)
except KeyboardInterrupt:
    raise
