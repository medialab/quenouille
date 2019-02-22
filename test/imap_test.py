# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
import time
import pytest
from collections import defaultdict
from operator import itemgetter
from quenouille import imap, imap_unordered

DATA = [
    ('A', 0.3, 0),
    ('A', 0.2, 1),
    ('B', 0.1, 2),
    ('B', 0.2, 3),
    ('B', 0.3, 4),
    ('B', 0.2, 5),
    ('B', 0.1, 6),
    ('C', 0.1, 7),
    ('D', 0.4, 8),
    ('D', 0.1, 9)
]


def sleeper(job):
    time.sleep(job[1] / 10)
    return job


class TestImap(object):
    def test_arguments(self):
        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, group_parallelism=1, group=None)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, listener=4)

    def test_basics(self):

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_less_jobs_than_threads(self):

        results = list(imap_unordered(DATA[:2], sleeper, 2))

        assert results == [('A', 0.2, 1), ('A', 0.3, 0)]

    def test_ordered(self):

        results = list(imap(DATA, sleeper, 2))

        assert results == DATA

    def test_group_parallelism(self):

        # Unordered
        results = list(imap_unordered(DATA, sleeper, 2, group_parallelism=1, group=itemgetter(0)))

        assert set(results) == set(DATA)

        results = list(imap_unordered(DATA, sleeper, 2, group_parallelism=1, group=itemgetter(0), group_buffer_size=3))

        assert set(results) == set(DATA)

        results = list(imap_unordered(DATA, sleeper, 2, group_parallelism=3, group=itemgetter(0), group_buffer_size=3))

        assert set(results) == set(DATA)

        # Ordered
        results = list(imap(DATA, sleeper, 2, group_parallelism=1, group=itemgetter(0)))

        assert set(results) == set(DATA)

        results = list(imap(DATA, sleeper, 2, group_parallelism=1, group=itemgetter(0), group_buffer_size=3))

        assert set(results) == set(DATA)

        results = list(imap(DATA, sleeper, 2, group_parallelism=3, group=itemgetter(0), group_buffer_size=3))

        assert set(results) == set(DATA)

    def test_listener(self):
        events = defaultdict(list)

        listener = lambda event, job: events[event].append(job)

        list(imap_unordered(DATA, sleeper, 5, listener=listener))

        assert set(events.keys()) == {'start'}

        assert set(events['start']) == set(DATA)
