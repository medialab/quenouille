# =============================================================================
# Quenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
from threading import Lock, Event
from quenouille.constants import FOREVER


class QueueIterator(object):
    def __init__(self, queue):
        self.queue = queue
        self.lock = Lock()
        self.event = Event()
        self.working_threads = 0

    def inc(self):
        with self.lock:
            self.working_threads += 1
            self.event.set()

    def dec(self):
        with self.lock:
            self.working_threads -= 1

            if self.working_threads < 0:
                raise RuntimeError('Negative number of workers')

            self.event.set()

    def task_done(self):
        return self.dec()

    def threads_still_working(self):
        return self.working_threads != 0

    def __iter__(self):
        return self

    def __next__(self):

        while True:
            with self.lock:

                # If queue is empty and all threads finished we stop
                if self.queue.qsize() == 0 and not self.threads_still_working():
                    raise StopIteration

            # The queue is empty but some threads are still working, we need to wait
            if self.queue.qsize() == 0:
                self.event.clear()
                self.event.wait()
                continue

            item = self.queue.get(timeout=FOREVER)

            self.inc()

            return item

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dec()
