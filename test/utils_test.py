# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
from queue import Queue
from quenouille import iter_queue


class TestUtils(object):
    def test_iter_queue(self):
        q = Queue()

        q.put(2)
        q.put(1)
        q.put(3)

        result = list(iter_queue(q))

        assert result == [2, 1, 3]
