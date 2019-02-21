# =============================================================================
# Quenouille Functional Testing
# =============================================================================
#
# Unfortunately, testing multithreaded behavior is kind of a nightmare. This
# script serves as a visual functional test to ensure we don't make stupid
# things.
#
import time
from quenouille import imap

DATA = [
    ('A', 0.3),
    ('A', 0.2),
    ('B', 0.1),
    ('B', 0.2),
    ('B', 0.3),
    ('B', 0.2),
    ('B', 0.1),
    ('C', 0.1),
    ('D', 0.4),
    ('D', 0.1)
]

def sleeper(job):
    time.sleep(job[1])
    return job

for result in imap(DATA, sleeper, 2):
    print(result)
