# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
import time
import pytest
import threading
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


def enumerated_sleeper(job):
    time.sleep(job[1][1] / 10)
    return job[0]


class TestImap(object):
    def test_arguments(self):

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 'test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, 45, 'test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, group_parallelism=1, group=None)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, listener=4)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, group_buffer_size=-45)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 3, group_throttle=-46)

    def test_basics(self):

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_less_jobs_than_threads(self):

        results = list(imap_unordered(DATA[:2], sleeper, 2))

        assert set(results) == set([('A', 0.2, 1), ('A', 0.3, 0)])

    def test_one(self):
        results = list(imap_unordered(DATA[:1], sleeper, 2))

        assert results == [('A', 0.3, 0)]

    def test_empty(self):
        results = list(imap_unordered(iter([]), sleeper, 5))

        assert results == []

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

    def test_break(self):

        for i in imap(enumerate(DATA), enumerated_sleeper, 5):
            if i == 2:
                break

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_throttle(self):

        group = lambda x: 'SAME'

        nbs = set(imap(range(10), lambda x: x, 10, group=group, group_throttle=0.1))

        assert nbs == set(range(10))

    def test_function_throttle(self):

        def throttling(group, nb):
            if group == 'odd':
                return None

            return 0.1

        group = lambda x: 'even' if x % 2 == 0 else 'odd'

        nbs = set(imap(range(10), lambda x: x, 10, group=group, group_throttle=throttling))

        assert nbs == set(range(10))
