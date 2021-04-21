# =============================================================================
# Quenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
import os
import time
from threading import Lock, Condition, Timer
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


class ThreadSafeIterator(object):
    """
    The ThreadSafeIterator class. Wraps the given iterator to make it
    thread-safe.

    Args:
        iterable (iterable): target iterable to wrap

    """

    def __init__(self, iterable):
        self.__iterator = iter(iterable)
        self.lock = Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.__iterator)


class QueueIterator(object):
    def __init__(self, queue):
        if not is_queue(queue):
            raise TypeError('argument is not a queue')

        self.queue = queue
        self.lock = Lock()
        self.condition = Condition()
        self.working_threads = 0

    def __inc(self):
        self.working_threads += 1

        with self.condition:
            self.condition.notify_all()

    def __dec(self):
        self.working_threads -= 1

        assert self.working_threads >= 0

        self.queue.task_done()

        with self.condition:
            self.condition.notify_all()

    def __threads_still_working(self):
        return self.working_threads != 0

    def task_done(self):
        with self.lock:
            return self.__dec()

    def __iter__(self):
        while True:
            with self.lock:

                # If queue is empty and all threads finished we stop
                if self.queue.empty() and not self.__threads_still_working():
                    break

            # The queue is empty but some threads are still working, we need to wait
            if self.queue.empty():

                with self.condition:
                    self.condition.wait()

                continue

            with self.lock:
                item = self.queue.get()
                self.__inc()

            yield item
            self.task_done()

    def __repr__(self):
        return '<{name} working={working!r} qsize={qsize!r}>'.format(
            name=self.__class__.__name__,
            working=self.working_threads,
            qsize=self.queue.qsize()
        )


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
