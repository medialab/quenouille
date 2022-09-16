import time
from quenouille import imap_unordered
from random import random

def work(i):
    time.sleep(0.1)
    return i

DATA = range(100_000)

def group(_):
    return 'A' if random() < 0.5 else 'B'

for i in imap_unordered(
    DATA,
    work,
    key=group,
    parallelism=1,
    throttle=0.2,
    threads=25,
    buffer_size=1024,
    wait=False
):
    print(i)
