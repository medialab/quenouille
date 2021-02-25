# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
import time
from queue import Queue
from collections import Counter
from quenouille import QueueIterator


class TestUtils(object):
    def test_iter_queue(self):
        q = Queue()

        q.put(2)
        q.put(1)
        q.put(3)

        iterator = QueueIterator(q)

        def consume():
            for i in iterator:
                with iterator:
                    yield i

        result = list(consume())

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
                with iterator:

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
