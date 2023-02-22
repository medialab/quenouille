import time
from quenouille import imap_unordered
from operator import itemgetter

TASKS = [("A", 1), ("A", 1), ("A", 1), ("B", 1), ("C", 2)]


def worker(payload):
    time.sleep(payload[1])

    return payload


iterator = imap_unordered(
    TASKS, worker, 2, group=itemgetter(0), group_buffer_size=0, group_parallelism=1
)

for g, t in iterator:
    print(g, t)
