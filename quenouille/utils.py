# =============================================================================
# Quenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
import os
import time
from threading import Lock, Timer
from weakref import WeakValueDictionary
from queue import Empty, Full


def clear(q):
    while True:
        try:
            q.get_nowait()
        except Empty:
            break


def flush(q, n, msg):
    for _ in range(n):
        try:
            q.put_nowait(msg)
        except Full:
            break


def smash(q, v):
    clear(q)
    q.put_nowait(v)


def is_queue(v):
    return (
        hasattr(v, 'get') and
        callable(v.get) and
        hasattr(v, 'put') and
        callable(v.put) and
        hasattr(v, 'task_done') and
        callable(v.task_done)
    )


# As per: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
def get_default_maxworkers():
    return min(32, (os.cpu_count() or 1) + 4)


class SmartTimer(Timer):
    """
    A Timer subclass able to return information about its execution time.
    """
    def __init__(self, *args, **kwargs):
        self.started_time = time.time()
        super().__init__(*args, **kwargs)

    def elapsed(self):
        return time.time() - self.started_time

    def remaining(self):
        return self.interval - self.elapsed()


class NamedLocks(object):
    def __init__(self):
        self.own_lock = Lock()
        self.locks = WeakValueDictionary()

    def __repr__(self):
        with self.own_lock:
            return '<{name} acquired={acquired!r}>'.format(
                name=self.__class__.__name__,
                acquired=list(self.locks)
            )

    def __len__(self):
        with self.own_lock:
            return len(self.locks)

    def __contains__(self, key):
        with self.own_lock:
            return key in self.locks

    def __getitem__(self, key):
        with self.own_lock:
            if key not in self.locks:
                lock = Lock()
                self.locks[key] = lock
                return lock

            return self.locks[key]

    def __call__(self, key):
        return self[key]
