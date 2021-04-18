# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
import sys
from queue import Queue
from threading import Thread, Event, Lock, Condition
from collections import namedtuple, OrderedDict
from itertools import count

from quenouille.utils import get, put, clear, flush, ThreadSafeIterator
from quenouille.constants import THE_END_IS_NIGH

Result = namedtuple('Result', ['exc_info', 'job', 'value'])

# TODO: fully document this complex code...
# TODO: test two executor successive imap calls
# TODO: add unit test with blocking iterator
# TODO: need an after job func callback to cleanup group counters + task counter (or just do it in output, since it is lock free)
# TODO: need a condition wait for the buffer later
# TODO: test with one thread
# TODO: lazy thread init?
# TODO: type checking in imap function also for convenience
# TODO: still test the iterator to queue (reverse than the current queue to iterator, with blocking)
# TODO: maybe the conditions in OrderedResultQueue and Buffer must be shuntable
# TODO: there seems to be room for improvement regarding keyboardinterrupts etc.


class OrderedResultQueue(Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_index = 0
        self.condition = Condition()

    def put(self, result, *args, **kwargs):
        with self.condition:
            while self.last_index != result.job.index:
                self.condition.wait()

            self.last_index += 1
            self.condition.notify_all()

        return super().put(result, *args, **kwargs)


class IterationState(object):
    def __init__(self):
        self.lock = Lock()
        self.started_count = 0
        self.finished_count = 0
        self.finished = False

    def start_task(self):
        with self.lock:
            self.started_count += 1

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
    def __init__(self, func, args, kwargs={}, index=None, group=None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.index = index
        self.group = group

    def __call__(self):
        try:
            value = self.func(*self.args, **self.kwargs)
        except BaseException as e:
            return Result(sys.exc_info(), self, None)

        return Result(None, self, value)


class Buffer(object):
    def __init__(self, maxsize=0, parallelism=None):

        # Properties
        self.maxsize = 0
        self.parallelism = parallelism

        # Threading
        self.condition = Condition()
        self.lock = Lock()

        # Containers
        self.items = OrderedDict()
        self.worked_groups = {}  # NOTE: not using a Counter here to avoid magic

    def __can_work(self, job):
        if self.parallelism is None:
            return True

        count = self.worked_groups.get(job.group, 0)

        if count < self.parallelism:
            return True

        return False

    def can_work(self, job: Job):
        with self.lock:
            return self.__can_work(job)

    def put(self, job: Job):
        """
        Add a value to the buffer and blocks if the buffer is already full.
        """
        self.lock.acquire()

        assert len(self.item) <= self.maxsize

        if len(self.items) == self.maxsize:
            self.lock.release()

            with self.condition:
                self.condition.wait()

            self.lock.acquire()

        self.items[id(job)] = job

        self.lock.release()

    def get(self)-> Job:
        with self.lock:
            if len(self.items) == 0:
                return None

            suitable_job = None

            for job in self.items.values():
                if self.__can_work(job):
                    suitable_job = job
                    break

            return self.items.popitem(id(job))

    def register_job(self, job: Job):
        with self.lock:
            group = job.group

            if group not in self.worked_groups:
                self.worked_groups[group] = 1
            else:
                self.worked_groups[group] += 1

    def unregister_job(self, job: Job):
        with self.lock:
            group = job.group

            assert group in self.worked_groups

            if self.worked_groups[group] == 1:
                del self.worked_groups[group]
            else:
                self.worked_groups[group] -= 1

        with self.condition:
            self.condition.notify_all()


class LazyGroupedThreadPoolExecutor(object):
    def __init__(self, max_workers):
        if not isinstance(max_workers, int) or max_workers < 1:
            raise TypeError('"max_workers/threads" should be a positive number')

        self.max_workers = max_workers
        self.job_queue = Queue(maxsize=max_workers)
        self.output_queue = None
        self.teardown_event = Event()
        self.teardown_lock = Lock()
        self.closed = False

        self.threads = [
            Thread(
                name='Thread-quenouille-%i-%i' % (id(self), n),
                target=self.__worker,
                daemon=True
            )
            for n in range(max_workers)
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

        with self.teardown_lock:
            self.teardown_event.set()

            # Clearing the job queue to cancel next jobs
            clear(self.job_queue)

            # We flush the job queue with end messages
            flush(self.job_queue, self.max_workers, THE_END_IS_NIGH)

            # Clearing the ouput queue since we won't be iterating anymore
            clear(self.output_queue)

            # We wait for worker threads to end
            for thread in self.threads:
                thread.join()

            # Clearing the job queue once more to unblock enqueuer
            clear(self.job_queue)

            self.closed = True

    def __worker(self):
        while not self.teardown_event.is_set():
            job = get(self.job_queue)

            # Signaling we must tear down the worker thread
            if job is THE_END_IS_NIGH:
                self.job_queue.task_done()
                break

            result = job()
            self.job_queue.task_done()
            put(self.output_queue, result)

    def __imap(self, iterable, func, *, ordered=False):
        iterator = ThreadSafeIterator(iterable)
        self.output_queue = Queue() if not ordered else OrderedResultQueue()

        # State
        job_counter = count()
        state = IterationState()
        buffer = Buffer()

        def enqueue():
            while not self.teardown_event.is_set():

                # First we check to see if there is a suitable buffered job
                job = buffer.get()

                if job is None:

                    # Else we consume the iterator to find one
                    try:
                        item = next(iterator)
                    except StopIteration:
                        state.declare_end()
                        break

                    job = Job(func, args=(item,), index=next(job_counter))

                # Registering the job
                state.start_task()
                buffer.register_job(job)
                put(self.job_queue, job)

        def output():
            while not state.should_stop() and not self.teardown_event.is_set():
                result = get(self.output_queue)

                # Acknowledging this job was finished
                self.output_queue.task_done()
                state.finish_task()
                buffer.unregister_job(result.job)

                # Raising an error that occurred within worker function
                if result.exc_info is not None:
                    raise result.exc_info[1].with_traceback(result.exc_info[2])

                # Actually yielding the value to main thread
                yield result.value

            # Making sure we are getting rid of the dispatcher thread
            dispatcher.join()

        dispatcher = Thread(
            name='Thread-quenouille-%i-dispatcher' % id(self),
            target=enqueue,
            daemon=True
        )
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
