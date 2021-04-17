import time
from quenouille import imap_unordered


def key(i):
    print('Grouped', i)
    if i == 5:
        raise RuntimeError

    return 'A'


def worker(i):
    time.sleep(1)
    return i


for i in imap_unordered(range(10), worker, 3, group=key, group_parallelism=2):
    print('Got ', i)
