# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
import time
import pytest
import threading
from queue import Queue
from collections import defaultdict
from operator import itemgetter

from quenouille import imap_unordered, imap, ThreadPoolExecutor
from quenouille.utils import put

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


def identity(x):
    return x


def sleeper(job):
    time.sleep(job[1] / 10)
    return job


def enumerated_sleeper(job):
    time.sleep(job[1][1] / 10)
    return job[0]


class TestImap(object):
    def test_arguments(self):
        with pytest.raises(TypeError):
            imap_unordered(None, sleeper, 4)

        with pytest.raises(TypeError):
            imap_unordered(DATA, 'test', 4)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 'test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, key='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=-1, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=1, key=itemgetter(0), buffer_size='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=1, buffer_size=0)

        # with pytest.raises(TypeError):
        #     imap_unordered(DATA, sleeper, 2, parallelism=4, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, key=itemgetter(0), throttle='test')

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, key=itemgetter(0), throttle=-4)

        with pytest.raises(RuntimeError):
            with ThreadPoolExecutor(4) as executor:
                pass

            executor.imap(DATA, sleeper)

        with pytest.raises(RuntimeError):
            with ThreadPoolExecutor(4) as executor:
                def work(item):
                    executor.imap_unordered(DATA, sleeper)

                list(executor.imap(DATA, work))

    def test_basics(self):

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_less_jobs_than_threads(self):

        results = list(imap_unordered(DATA[:2], sleeper, 2))

        assert set(results) == set([('A', 0.2, 1), ('A', 0.3, 0)])

    def test_one_thread(self):
        results = list(imap(DATA, sleeper, 1))

        assert results == DATA

    def test_one_item(self):
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

        nbs = set(imap_unordered(range(10), identity, 10, key=group, throttle=0.01))
        assert nbs == set(range(10))

        nbs = set(imap_unordered(range(10), identity, 10, key=group, throttle=0.01, buffer_size=1))
        assert nbs == set(range(10))

        nbs = set(imap_unordered(range(10), identity, 10, key=group, throttle=0.01, buffer_size=3))
        assert nbs == set(range(10))

        nbs = list(imap(range(10), identity, 10, key=group, throttle=0.01))
        assert nbs == list(range(10))

        nbs = list(imap(range(10), identity, 10, key=group, throttle=0.01, buffer_size=1))
        assert nbs == list(range(10))

        nbs = list(imap(range(10), identity, 10, key=group, throttle=0.01, buffer_size=3))
        assert nbs == list(range(10))

        results = list(imap_unordered(DATA, sleeper, 4, key=itemgetter(0), throttle=0.01))
        assert set(results) == set(DATA)

        results = list(imap(DATA, sleeper, 4, key=itemgetter(0), throttle=0.01))
        assert results == DATA

    def test_callable_throttle(self):

        def throttling(group, nb):
            if group == 'odd':
                return 0

            return 0.1

        group = lambda x: 'even' if x % 2 == 0 else 'odd'

        nbs = set(imap(range(10), identity, 10, key=group, throttle=throttling))

        assert nbs == set(range(10))

        def hellraiser(g, i):
            if i > 2:
                raise TypeError

            return 0.01

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 4, throttle=hellraiser))

        def wrong_type(g, i):
            return 'test'

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 2, throttle=wrong_type))

        def negative(g, i):
            return -30

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 2, throttle=negative))

    def test_callable_parallelism(self):
        def per_group(g):
            if g == 'B':
                return 3
            else:
                return 1

        result = list(imap(DATA, identity, 4, parallelism=per_group, key=itemgetter(0)))
        assert result == DATA

        def per_group_raising(g):
            if g == 'B':
                raise RuntimeError

            return 1

        with pytest.raises(RuntimeError):
            result = list(imap(DATA, identity, 4, parallelism=per_group_raising, key=itemgetter(0)))

        def per_group_invalid(g):
            if g == 'B':
                return 'test'

            return 1

        with pytest.raises(TypeError):
            result = list(imap(DATA, identity, 4, parallelism=per_group_invalid, key=itemgetter(0)))

        def per_group_zero(g):
            if g == 'B':
                return 0

            return 1

        with pytest.raises(TypeError):
            result = list(imap(DATA, identity, 4, parallelism=per_group_zero, key=itemgetter(0)))

        def per_group_negative(g):
            if g == 'B':
                return -3

            return 1

        with pytest.raises(TypeError):
            result = list(imap(DATA, identity, 4, parallelism=per_group_negative, key=itemgetter(0)))

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

    def test_executor(self):
        with ThreadPoolExecutor(max_workers=4) as executor:
            result = list(executor.imap(DATA, sleeper))
            assert result == DATA

            result = set(executor.imap_unordered(DATA, sleeper))
            assert result == set(DATA)

    def test_blocking_iterator(self):
        def sleeping():
            for i in range(5):
                yield i
                time.sleep(0.01)

        def blocking():
            condition = threading.Condition()

            def release():
                with condition:
                    condition.notify_all()

            for i in range(5):
                yield i
                timer = threading.Timer(0.01, release)
                timer.start()
                with condition:
                    condition.wait()

        result = list(imap(sleeping(), identity, 4))
        assert result == list(range(5))

        result = list(imap(blocking(), identity, 4))
        assert result == list(range(5))

    def test_queue(self):
        # TODO: try with maxsize
        # TODO: also try with q.put into imap loop
        # TODO: when is task_done necessary
        q = Queue()
        q.put(1)

        def worker(i):
            if i == 1:
                put(q, 2)
                put(q, 3)
                put(q, 4)

            if i == 3:
                put(q, 4)

            time.sleep(0.01)

            if i == 4:
                put(q, 5)

            return i

        result = list(imap(q, worker, 2))
        assert result == [1, 2, 3, 4, 4, 5, 5]
