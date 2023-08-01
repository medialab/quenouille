# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
from queue import Queue

from quenouille.imap import QuenouilleQueueProtocol
from quenouille.utils import NamedLocks, queue_iter


def is_usable_queue(v):
    return isinstance(v, QuenouilleQueueProtocol)


class TestUtils(object):
    def test_is_usable_queue(self):
        assert is_usable_queue(Queue())
        assert not is_usable_queue(True)
        assert not is_usable_queue(object())
        assert not is_usable_queue(dict())
        assert not is_usable_queue(list())
        assert not is_usable_queue((i for i in range(4)))

    def test_queue_iter(self):
        q = Queue()

        for n in range(10):
            q.put(n)

        result = []

        for n in queue_iter(q):
            result.append(n)

            if n < 10:
                q.put(10 + n)

        assert result == list(range(20))

    def test_named_locks(self):
        locks = NamedLocks()

        assert len(locks) == 0

        one_lock = locks["one"]
        two_lock = locks["two"]

        assert len(locks) == 2

        assert not one_lock.locked()
        assert not two_lock.locked()

        other_one_lock = locks["one"]

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
