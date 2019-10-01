# =============================================================================
# Quenouille Throttling Testing
# =============================================================================
#
# Testing throttling logic.
#
import time
from operator import itemgetter
from quenouille import imap, imap_unordered

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
    ('A', 0.3, 10),
    ('A', 0.2, 11),
    ('B', 0.1, 12),
    ('B', 0.2, 13),
    ('B', 0.3, 14),
    ('B', 0.2, 15),
    ('B', 0.1, 16),
    ('C', 0.1, 17),
    ('D', 0.4, 18),
    ('D', 0.1, 19),
    ('B', 0.1, 20),
    ('B', 0.1, 21),
    ('B', 0.4, 22),
    ('B', 0.1, 23),
]

def sleeper(job):
    time.sleep(job[1] * 10)
    return job

print('Unordered')
t = time.time()
for result in imap_unordered(HOMEGENEOUS_DATA, sleeper, 3, group_throttle=5, group=itemgetter(0)):
    n = time.time()
    print(result, n - t)
    t = n
print()

print('Ordered')
t = time.time()
for result in imap(HOMEGENEOUS_DATA, sleeper, 3, group_throttle=5, group_throttle_entropy=0.5, group=itemgetter(0)):
    n = time.time()
    print(result, n - t)
    t = n
