# =============================================================================
# Quenouille Functional Testing
# =============================================================================
#
# Unfortunately, testing multithreaded behavior is kind of a nightmare. This
# script serves as a visual functional test to ensure we don't make stupid
# things.
#
import time
from operator import itemgetter
from quenouille import imap

DATA = [
    ('A', 0.3, 0),
    ('A', 0.2, 1),
    ('B', 0.1, 2),
    ('B', 0.2, 3),
    ('B', 0.3, 4),
    ('B', 0.2, 5),
    ('C', 0.5, 6),
    ('B', 0.1, 7),
    ('C', 0.1, 8),
    ('D', 0.4, 9),
    ('D', 0.1, 10)
]

HOMEGENEOUS_DATA = [
    ('B', 0.3, 0),
    ('B', 0.2, 1),
    ('B', 0.1, 2),
    ('B', 0.2, 3),
    ('B', 0.3, 4),
    ('B', 0.2, 5),
    ('B', 0.1, 6),
    ('B', 0.1, 7),
    ('B', 0.4, 8),
    ('B', 0.1, 9),
]

def sleeper(job):
    time.sleep(job[1])
    return job

print('2 threads')
for result in imap(DATA, sleeper, 2):
    print(result)
print()

print('10 threads / homogeneous (result should be ordered by sleep time)')
for result in imap(HOMEGENEOUS_DATA, sleeper, 10):
    print(result)
print()

print('10 threads / 1 parallelism / homogeneous (jobs processed sequentially)')
for result in imap(HOMEGENEOUS_DATA, sleeper, 10, group=itemgetter(0), group_parallelism=1):
    print(result)
print()

print('2 threads / 1 parallelism / ordered')
for result in imap(DATA, sleeper, 2, group=itemgetter(0), group_parallelism=1, ordered=True):
    print(result)
print()

print('10 threads / 1 parallelism')
for result in imap(DATA, sleeper, 10, group=itemgetter(0), group_parallelism=1, ordered=False):
    print(result)
print()

print('10 threads / 1 parallelism / ordered')
for result in imap(DATA, sleeper, 10, group=itemgetter(0), group_parallelism=1, ordered=True):
    print(result)
print()

print('10 threads / 2 parallelism')
for result in imap(DATA, sleeper, 10, group=itemgetter(0), group_parallelism=2, ordered=False):
    print(result)
print()

print('10 threads / 2 parallelism / ordered')
for result in imap(DATA, sleeper, 10, group=itemgetter(0), group_parallelism=2, ordered=True):
    print(result)
print()
