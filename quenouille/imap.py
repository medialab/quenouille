# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
from queue import Queue, Full
from threading import Thread, Event, Lock, Condition
from collections import namedtuple, deque

from quenouille.utils import get, put, ThreadSafeIterator
from quenouille.constants import THE_END_IS_NIGH

Result = namedtuple('Result', ['exception', 'job', 'value'])

# TODO: fully document this complex code...
# TODO: test two executor successive imap calls
# TODO: add unit test with blocking iterator
# TODO: need an after job func callback to cleanup group counters + task counter (or just do it in output, since it is lock free)
# TODO: need a condition wait for the buffer later


class OrderedResultQueue(Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_index = 0
        self.condition = Condition()

    def put(self, result, timeout=None):
        with self.condition:
            while self.last_index != result.job.index:
                self.condition.wait()

            self.last_index += 1
            self.condition.notify_all()

        return super().put(result, timeout=timeout)


class IterationState(object):
    def __init__(self):
        self.lock = Lock()
        self.started_count = 0
        self.finished_count = 0
        self.finished = False

    def start_task(self):
        with self.lock:
            self.started_count += 1

            return self.started_count - 1

    def finish_task(self):
        with self.lock:
            self.finished_count += 1

    def declare_end(self):
        with self.lock:
            self.finished = True

    def should_stop(self):
        with self.lock:
            return self.finished and self.finished_count == self.started_count


class Job(object):
    def __init__(self, func, args, kwargs={}, index=None, key=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.index = index
        self.key = key

    def __call__(self):

        # TODO: try
        value = self.func(*self.args, **self.kwargs)
        result = Result(None, self, value)

        return result


class LazyGroupedThreadPoolExecutor(object):
    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.job_queue = Queue(maxsize=max_workers)
        self.output_queue = None
        self.teardown_event = Event()
        self.closed = False

        self.threads = [
            Thread(target=self.__worker, daemon=True)
            for _ in range(max_workers)
        ]

        for thread in self.threads:
            thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.__teardown()

    def __teardown(self):
        if self.closed:
            return

        self.teardown_event.set()

        for thread in self.threads:
            try:
                self.job_queue.put_nowait(THE_END_IS_NIGH)

            # If the job queue is full, we can safely stop because
            # the teardown event will prevent subsequent queue get
            except Full:
                break

        for thread in self.threads:
            thread.join()

        self.closed = True

    def __worker(self):
        while not self.teardown_event.is_set():
            job = get(self.job_queue)

            # Signaling we must tear down the worker thread
            if job is THE_END_IS_NIGH:
                break

            result = job()
            put(self.output_queue, result)

    def __imap(self, iterable, func, *, ordered=False):
        iterator = ThreadSafeIterator(iterable)
        self.output_queue = Queue() if not ordered else OrderedResultQueue()

        # State
        state = IterationState()

        def enqueue():
            while True:
                try:
                    item = next(iterator)
                except StopIteration:
                    state.declare_end()
                    break

                index = state.start_task()

                job = Job(func, args=(item,), index=index)
                put(self.job_queue, job)

        def output():
            while not state.should_stop():
                result = get(self.output_queue)
                state.finish_task()

                # NOTE: do we need a lock here?
                yield result.value

        dispatcher = Thread(target=enqueue, daemon=True)
        dispatcher.start()

        return output()

    def imap_unordered(self, iterable, func):
        return self.__imap(iterable, func, ordered=False)

    def imap(self, iterable, func):
        return self.__imap(iterable, func, ordered=True)


def imap_unordered(iterable, func, threads):
    with LazyGroupedThreadPoolExecutor(max_workers=threads) as executor:
        yield from executor.imap_unordered(iterable, func)


def imap(iterable, func, threads):
    with LazyGroupedThreadPoolExecutor(max_workers=threads) as executor:
        yield from executor.imap(iterable, func)
