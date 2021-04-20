# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
import time
import pytest
from queue import Queue
from collections import Counter

from quenouille import QueueIterator, ThreadPoolExecutor
from quenouille.utils import is_queue, put


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

    # def test_queue_imap(self):
    #     with ThreadPoolExecutor(2) as executor:
    #         q = Queue()
    #         q.put(1)

    #         # TODO: try with maxsize
    #         # TODO: also try with q.put into imap loop

    #         iterator = QueueIterator(q)

    #         def worker(i):
    #             if i == 1:
    #                 put(q, 2)
    #                 put(q, 3)
    #                 put(q, 4)

    #             time.sleep(0.01)

    #             if i == 4:
    #                 put(q, 5)

    #             return i

    #         result = list(executor.imap(iterator, worker))
    #         print(result)
