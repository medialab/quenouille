# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
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


class TestImap(object):
    def test_basics(self):

        results = list(imap(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_less_jobs_than_threads(self):

        results = list(imap(DATA[:2], sleeper, 2))

        assert results == [('A', 0.2), ('A', 0.3)]

    def test_ordered(self):

        results = list(imap(DATA, sleeper, 2, ordered=True))

        assert results == DATA
