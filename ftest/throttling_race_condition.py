import time
from quenouille import imap_unordered
from random import random

def work(i):
    time.sleep(0.1)
    return i

DATA = range(100_000)

def group(_):
    return 'A' if random() < 0.5 else 'B'

# TODO: problems to solve:
# 1. keyboard interrupt should work with group and throttling even with wait and not daemonic
# 2. a nasty throttling race condition is able to shunt an assertion in the timer callback
for i in imap_unordered(
    DATA,
    work,
    key=group,
    # parallelism=1,
    # throttle=0.2,
    # threads=25,
    # buffer_size=1024,
    # wait=False,
    # daemonic=True
):
    print(i)
