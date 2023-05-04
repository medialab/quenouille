# =============================================================================
# Quenouille imap Unit Tests
# =============================================================================
import time
import pytest
import threading
from queue import Queue, LifoQueue
from operator import itemgetter

from quenouille import imap_unordered, imap, ThreadPoolExecutor
from quenouille.exceptions import (
    BrokenThreadPool,
    InvalidThrottleParallelismCombination,
)

DATA = [
    ("A", 0.3, 0),
    ("A", 0.2, 1),
    ("B", 0.1, 2),
    ("B", 0.2, 3),
    ("B", 0.3, 4),
    ("B", 0.2, 5),
    ("B", 0.1, 6),
    ("C", 0.1, 7),
    ("D", 0.4, 8),
    ("D", 0.1, 9),
]


def identity(x):
    return x


def queue_from(iterable, maxsize=0):
    q = Queue(maxsize=maxsize)

    for i in iterable:
        q.put(i)

    return q


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
            imap_unordered(DATA, "test", 4)

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, "test")

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, key="test")

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 4, parallelism=-1, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(
                DATA, sleeper, 4, parallelism=1, key=itemgetter(0), buffer_size="test"
            )

        # with pytest.raises(TypeError):
        #     imap_unordered(DATA, sleeper, 4, parallelism=1, buffer_size=0)

        # with pytest.raises(TypeError):
        #     imap_unordered(DATA, sleeper, 2, parallelism=4, key=itemgetter(0))

        with pytest.raises(TypeError):
            imap_unordered(DATA, sleeper, 2, key=itemgetter(0), throttle="test")

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

        with pytest.raises(TypeError):
            imap(DATA, sleeper, 2, join="test")

        with pytest.raises(TypeError):
            imap(DATA, sleeper, 2, daemonic="test")

    def test_basics(self):
        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_none_iterator(self):
        iterable = [None] * 3

        results = list(imap_unordered(iterable, identity, 2))
        assert results == iterable

    def test_less_jobs_than_threads(self):
        results = list(imap_unordered(DATA[:2], sleeper, 2))

        assert set(results) == set([("A", 0.2, 1), ("A", 0.3, 0)])

    def test_one_thread(self):
        results = list(imap(DATA, sleeper, 1))

        assert results == DATA

    def test_one_item(self):
        results = list(imap_unordered(DATA[:1], sleeper, 2))

        assert results == [("A", 0.3, 0)]

    def test_empty(self):
        results = list(imap_unordered(iter([]), sleeper, 5))

        assert results == []

    def test_ordered(self):
        results = list(imap(DATA, sleeper, 2))

        assert results == DATA

    def test_group_parallelism(self):
        # Unordered
        results = list(
            imap_unordered(DATA, sleeper, 2, parallelism=1, key=itemgetter(0))
        )
        assert set(results) == set(DATA)

        results = list(
            imap_unordered(
                DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=3
            )
        )
        assert set(results) == set(DATA)

        results = list(
            imap_unordered(
                DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=1
            )
        )
        assert set(results) == set(DATA)

        results = list(
            imap_unordered(
                DATA, sleeper, 4, parallelism=3, key=itemgetter(0), buffer_size=3
            )
        )
        assert set(results) == set(DATA)

        # Ordered
        results = list(imap(DATA, sleeper, 2, parallelism=1, key=itemgetter(0)))
        assert results == DATA

        results = list(
            imap(DATA, sleeper, 2, parallelism=1, key=itemgetter(0), buffer_size=3)
        )
        assert results == DATA

        results = list(
            imap(DATA, sleeper, 4, parallelism=3, key=itemgetter(0), buffer_size=3)
        )
        assert results == DATA

    def test_break(self):
        for i in imap(enumerate(DATA), enumerated_sleeper, 5):
            if i == 2:
                break

        results = list(imap_unordered(DATA, sleeper, 2))

        assert len(results) == len(DATA)
        assert set(results) == set(DATA)

    def test_throttle(self):
        group = lambda x: "SAME"

        nbs = set(imap_unordered(range(10), identity, 10, key=group, throttle=0.01))
        assert nbs == set(range(10))

        nbs = set(
            imap_unordered(
                range(10), identity, 10, key=group, throttle=0.01, buffer_size=1
            )
        )
        assert nbs == set(range(10))

        nbs = set(
            imap_unordered(
                range(10), identity, 10, key=group, throttle=0.01, buffer_size=3
            )
        )
        assert nbs == set(range(10))

        nbs = list(imap(range(10), identity, 10, key=group, throttle=0.01))
        assert nbs == list(range(10))

        nbs = list(
            imap(range(10), identity, 10, key=group, throttle=0.01, buffer_size=1)
        )
        assert nbs == list(range(10))

        nbs = list(
            imap(range(10), identity, 10, key=group, throttle=0.01, buffer_size=3)
        )
        assert nbs == list(range(10))

        results = list(
            imap_unordered(DATA, sleeper, 4, key=itemgetter(0), throttle=0.01)
        )
        assert set(results) == set(DATA)

        results = list(imap(DATA, sleeper, 4, key=itemgetter(0), throttle=0.01))
        assert results == DATA

    def test_callable_throttle(self):
        def throttling(group, nb, result):
            assert nb == result

            if group == "odd":
                return 0

            return 0.1

        group = lambda x: "even" if x % 2 == 0 else "odd"

        nbs = set(imap(range(10), identity, 10, key=group, throttle=throttling))

        assert nbs == set(range(10))

        def hellraiser(g, i, result):
            if i > 2:
                raise TypeError

            return 0.01

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 4, key=group, throttle=hellraiser))

        def wrong_type(g, i, result):
            return "test"

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 2, key=group, throttle=wrong_type))

        def negative(g, i, result):
            return -30

        with pytest.raises(TypeError):
            list(imap_unordered(range(5), identity, 2, key=group, throttle=negative))

    def test_callable_parallelism(self):
        def per_group(g):
            if g == "B":
                return 3
            else:
                return 1

        result = list(imap(DATA, identity, 4, parallelism=per_group, key=itemgetter(0)))
        assert result == DATA

        def per_group_with_special(g):
            if g == "B":
                return None

            return 1

        result = list(imap(DATA, identity, 4, parallelism=per_group, key=itemgetter(0)))
        assert result == DATA

        def per_group_raising(g):
            if g == "B":
                raise RuntimeError

            return 1

        with pytest.raises(RuntimeError):
            result = list(
                imap(
                    DATA, identity, 4, parallelism=per_group_raising, key=itemgetter(0)
                )
            )

        def per_group_invalid(g):
            if g == "B":
                return "test"

            return 1

        with pytest.raises(TypeError):
            result = list(
                imap(
                    DATA, identity, 4, parallelism=per_group_invalid, key=itemgetter(0)
                )
            )

        def per_group_zero(g):
            if g == "B":
                return 0

            return 1

        with pytest.raises(TypeError):
            result = list(
                imap(DATA, identity, 4, parallelism=per_group_zero, key=itemgetter(0))
            )

        def per_group_negative(g):
            if g == "B":
                return -3

            return 1

        with pytest.raises(TypeError):
            result = list(
                imap(
                    DATA, identity, 4, parallelism=per_group_negative, key=itemgetter(0)
                )
            )

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

    def test_key_raise(self):
        def group(i):
            if i > 2:
                raise RuntimeError

            return "SAME"

        with pytest.raises(RuntimeError):
            list(imap_unordered(range(5), identity, 2, key=group))

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
        q = queue_from([1])

        def worker(i):
            if i == 1:
                q.put(2)
                q.put(3)
                q.put(4)

            if i == 3:
                q.put(4)

            time.sleep(0.01)

            if i == 4:
                q.put(5)

            return i

        result = list(imap(q, worker, 2))
        assert q.empty()
        assert result == [1, 2, 3, 4, 4, 5, 5]

        q = queue_from([1])
        result = []

        for i in imap(q, identity, 2):
            result.append(i)

            if i == 1:
                q.put(2)
                q.put(3)
                q.put(4)

            if i == 3:
                q.put(4)

            time.sleep(0.01)

            if i == 4:
                q.put(5)

        assert q.empty()
        assert result == [1, 2, 3, 4, 4, 5, 5]

        q = queue_from([])
        result = list(imap(q, worker, 2))

        assert q.empty()
        assert result == []

    def test_default_threads(self):
        result = list(imap(DATA, identity))

        assert result == DATA

    def test_initializer(self):
        context = threading.local()

        def iterable_initargs():
            yield 10

        def constant_init(offset=0):
            context.number = 3 + offset

        def hellraiser():
            raise RuntimeError

        c = 0
        lock = threading.Lock()

        def stateful_hellraiser():
            context.number = 6
            nonlocal c

            with lock:
                if c > 1:
                    raise RuntimeError

                c += 1
                time.sleep(0.01)

        def worker(n):
            return n + context.number

        def worker_sleep(n):
            time.sleep(0.1)
            return n + context.number

        result = list(imap(range(5), worker, 2, initializer=constant_init))
        assert result == [3, 4, 5, 6, 7]

        result = list(
            imap(range(5), worker, 2, initializer=constant_init, initargs=(3,))
        )
        assert result == [6, 7, 8, 9, 10]

        result = list(
            imap(
                range(5),
                worker,
                2,
                initializer=constant_init,
                initargs=iterable_initargs(),
            )
        )
        assert result == [13, 14, 15, 16, 17]

        with pytest.raises(BrokenThreadPool):
            result = list(imap(range(5), worker, 2, initializer=hellraiser))

        with pytest.raises(BrokenThreadPool):
            result = list(
                imap(range(10), worker_sleep, 4, initializer=stateful_hellraiser)
            )

        with pytest.raises(BrokenThreadPool):
            ThreadPoolExecutor(2, initializer=hellraiser)

    def test_error_throttling_plus_parallelism(self):
        def group(_):
            return 1

        def worker(n):
            return n * 2

        with ThreadPoolExecutor(4) as executor:
            with pytest.raises(InvalidThrottleParallelismCombination):
                for _ in executor.imap_unordered(
                    range(10), worker, throttle=1.0, parallelism=4, key=group
                ):
                    pass

    def test_empty_buffer(self):
        def worker(n):
            return n * 10

        result = list(imap(range(10), worker, 4, buffer_size=0))

        assert result == [worker(n) for n in range(10)]

        stack = LifoQueue()

        for n in range(10):
            stack.put(n)

        def group(_):
            return 1

        result = list(imap(stack, worker, 1, buffer_size=0, parallelism=1, key=group))

        assert result == [90, 80, 70, 60, 50, 40, 30, 20, 10, 0]

    def test_sync_pool(self):
        def worker(n):
            return n * 10

        with ThreadPoolExecutor(0) as executor:
            result = list(executor.imap_unordered(range(10), worker))

            assert result == [worker(n) for n in range(10)]

            q = Queue()

            for n in range(5):
                q.put(n)

            result = []

            for n in executor.imap_unordered(q, lambda x: x):
                result.append(n)

                if n < 5:
                    q.put(5 + n)

            assert result == list(range(10))
