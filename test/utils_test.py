# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
import time
import pytest
from queue import Queue
from collections import Counter

from quenouille.utils import is_queue, QueueIterator, NamedLocks


class TestUtils(object):
    def test_is_queue(self):
        assert is_queue(Queue())
        assert not is_queue(True)
        assert not is_queue(object())
        assert not is_queue(dict())
        assert not is_queue(list())
        assert not is_queue((i for i in range(4)))

    def test_iter_queue(self):
        with pytest.raises(TypeError):
            QueueIterator('test')

        q = Queue()

        q.put(2)
        q.put(1)
        q.put(3)

        iterator = QueueIterator(q)
        result = list(iterator)

        assert result == [2, 1, 3]

    def test_parallel(self):
        q = Queue()

        q.put(1)
        q.put(1)
        q.put(1)
        q.put(1)

        iterator = QueueIterator(q)

        def consume():
            for idx, i in enumerate(iterator):
                if i == 1:
                    q.put(2)
                    q.put(3)

                if i == 3:
                    q.put(4)

                time.sleep(idx * 0.01)

                yield i

        result = list(consume())

        assert Counter(result) == {
            1: 4,
            2: 4,
            3: 4,
            4: 4
        }

    def test_named_locks(self):
        locks = NamedLocks()

        assert len(locks) == 0

        one_lock = locks['one']
        two_lock = locks['two']

        assert len(locks) == 2

        assert not one_lock.locked()
        assert not two_lock.locked()

        other_one_lock = locks['one']

        with one_lock:
            assert other_one_lock.locked()

        assert not other_one_lock.locked()

        other_one_lock.acquire()
        two_lock.acquire()
        two_lock.release()
        other_one_lock.release()

        del one_lock
        del two_lock
        del other_one_lock

        assert len(locks) == 0
