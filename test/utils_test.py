# =============================================================================
# Quenouille Utils Unit Tests
# =============================================================================
from queue import Queue

from quenouille.utils import is_queue, NamedLocks


class TestUtils(object):
    def test_is_queue(self):
        assert is_queue(Queue())
        assert not is_queue(True)
        assert not is_queue(object())
        assert not is_queue(dict())
        assert not is_queue(list())
        assert not is_queue((i for i in range(4)))

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
