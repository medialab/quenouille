# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
import time
import pytest
import threading
from collections import defaultdict
from operator import itemgetter
from quenouille import imap_unordered, imap

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
            imap_unordered(DATA, sleeper, 4, key='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=-1, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=1, key=itemgetter(0), buffer_size='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, parallelism=4, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, key=itemgetter(0), throttle='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, key=itemgetter(0), throttle=-4)

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
        results = list(imap_unordered(DATA, sleeper, 2, parallelism=1, key=itemgetter(0)))
        assert set(results) == set(DATA)

        results = list(imap_unordered(DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=3))
        assert set(results) == set(DATA)

        results = list(imap_unordered(DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=1))
        assert set(results) == set(DATA)

        results = list(imap_unordered(DATA, sleeper, 4, parallelism=3, key=itemgetter(0), buffer_size=3))
        assert set(results) == set(DATA)

        # Ordered
        results = list(imap(DATA, sleeper, 2, parallelism=1, key=itemgetter(0)))
        assert results == DATA

        results = list(imap(DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=3))
        assert results == DATA

        results = list(imap(DATA, sleeper, 4, parallelism=3, key=itemgetter(0), buffer_size=3))
        assert results == DATA

    def test_break(self):

        for i in imap(enumerate(DATA), enumerated_sleeper, 5):
            if i == 2:
                break

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_throttle(self):

        group = lambda x: 'SAME'

        nbs = set(imap(range(10), lambda x: x, 10, key=group, throttle=0.01))

        assert nbs == set(range(10))
        # TODO: add a test with buffer_size 1 and 3

    # def test_function_throttle(self):

    #     def throttling(group, nb):
    #         if group == 'odd':
    #             return None

    #         return 0.1

    #     group = lambda x: 'even' if x % 2 == 0 else 'odd'

    #     nbs = set(imap(range(10), lambda x: x, 10, group=group, group_throttle=throttling))

    #     assert nbs == set(range(10))

    def test_raise(self):
        def hellraiser(i):
            if i > 5:
                raise RuntimeError

            return i

        with pytest.raises(RuntimeError):
            for i in imap(range(10), hellraiser, 1):
                pass

        with pytest.raises(RuntimeError):
            for i in imap(range(6, 15), hellraiser, 4):
                pass
