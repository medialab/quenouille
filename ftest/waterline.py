import time
import threading
from quenouille import imap_unordered

DATA = ["A", "A", "B", "C", "D", "E", "F", "C", "E", "G", "H", "A", "J", "L", "A", "A"]


def sleeper(job):
    time.sleep(1)
    return job


def grouper(job):
    return job


for x in imap_unordered(DATA, sleeper, threads=50, group=grouper, group_parallelism=1):
    print(x)
