# =============================================================================
# Quenouille Throttling Testing
# =============================================================================
#
# Testing throttling logic.
#
import time
from operator import itemgetter
from quenouille import imap_unordered

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
    time.sleep(job[1] * 10)
    return job

for result in imap_unordered(HOMEGENEOUS_DATA, sleeper, 3, group_throttle=2, group=itemgetter(0)):
    print(result)
