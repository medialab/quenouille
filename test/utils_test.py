# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
from queue import Queue
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
