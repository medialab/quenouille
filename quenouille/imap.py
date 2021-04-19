# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
import sys
from queue import Queue
from threading import Thread, Event, Lock, Condition
from collections import OrderedDict
from itertools import count

from quenouille.utils import get, put, clear, flush, ThreadSafeIterator
from quenouille.constants import THE_END_IS_NIGH, DEFAULT_BUFFER_SIZE

# TODO: fully document this complex code...
# TODO: test two executor successive imap calls
# TODO: add unit test with blocking iterator
# TODO: need an after job func callback to cleanup group counters + task counter (or just do it in output, since it is lock free)
# TODO: need a condition wait for the buffer later
# TODO: test with one thread
# TODO: lazy thread init?
# TODO: type checking in imap function also for convenience
# TODO: still test the iterator to queue (reverse than the current queue to iterator, with blocking)
# TODO: maybe the conditions in OrderedOutputQueue and Buffer must be shuntable
# TODO: there seems to be room for improvement regarding keyboardinterrupts etc. wrapping enqueue + __worker?
# TODO: validate iterable, func, threads
# TODO: transfer doctypes from imap_old
# TODO: test buffer_size = 0
# TODO: test with small buffer sizes


class OrderedOutputQueue(Queue):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_index = 0
        self.condition = Condition()

    def put(self, job, *args, **kwargs):
        with self.condition:
            while self.last_index != job.index:
                self.condition.wait()

            self.last_index += 1
            self.condition.notify_all()

        return super().put(job, *args, **kwargs)


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
        self.result = None
        self.exc_info = None

    def __call__(self):
        try:
            self.result = self.func(*self.args, **self.kwargs)
        except BaseException as e:
            self.exc_info = sys.exc_info()

    def __repr__(self):
        return '<{name} id={id!r} index={index!r} group={group!r} args={args!r}>'.format(
            name=self.__class__.__name__,
            index=self.index,
            group=self.group,
            args=self.args,
            id=id(self)
        )


class Buffer(object):
    def __init__(self, maxsize=0, parallelism=1):

        # Properties
        self.maxsize = maxsize
        self.parallelism = parallelism

        # Threading
        self.condition = Condition()
        self.lock = Lock()

        # Containers
        self.items = OrderedDict()
        self.worked_groups = {}  # NOTE: not using a Counter here to avoid magic

    def is_clean(self):
        with self.lock:
            return (
                len(self.items) == 0 and
                len(self.worked_groups) == 0
            )

    def __full(self):
        count = len(self.items)
        assert count <= self.maxsize
        return count == self.maxsize

    def full(self):
        with self.lock:
            return self.__full()

    def __empty(self):
        count = len(self.items)
        assert count <= self.maxsize
        return count == 0

    def empty(self):
        with self.lock:
            return self.__empty()

    def __can_work(self, job):

        # None group is the default and can always be processed without constraints
        if job.group is None:
            return True

        count = self.worked_groups.get(job.group, 0)

        if count < self.parallelism:
            return True

        return False

    def can_work(self, job: Job):
        with self.lock:
            return self.__can_work(job)

    def __find_suitable_job(self):
        for job in self.items.values():
            if self.__can_work(job):
                return job

        return None

    def put(self, job: Job):
        """
        Add a value to the buffer and blocks if the buffer is already full.
        """
        self.lock.acquire()

        if self.__full():
            self.lock.release()

            with self.condition:
                self.condition.wait()

            self.lock.acquire()

        # TODO: for maxsize = 0, return the job instead of setting
        self.items[id(job)] = job

        self.lock.release()

    def get(self):
        while True:
            self.lock.acquire()

            if len(self.items) == 0:
                self.lock.release()
                return None

            job = self.__find_suitable_job()

            if job is not None:
                del self.items[id(job)]
                self.lock.release()
                return job

            if self.__full():
                self.lock.release()

                # TODO: with throttling, we might want to block until timer is free
                with self.condition:
                    self.condition.wait()

                # Simulating recursion
                continue

            self.lock.release()
            return None

        raise RuntimeError

    def register_job(self, job: Job):
        with self.lock:
            group = job.group

            # None group does not count
            if group is None:
                return

            if group not in self.worked_groups:
                self.worked_groups[group] = 1
            else:
                self.worked_groups[group] += 1

    def unregister_job(self, job: Job):
        with self.lock:
            group = job.group

            # None group does not count
            if group is None:
                return

            assert group in self.worked_groups

            if self.worked_groups[group] == 1:
                del self.worked_groups[group]
            else:
                self.worked_groups[group] -= 1

        with self.condition:
            self.condition.notify_all()


def validate_max_workers(name, max_workers):
    if not isinstance(max_workers, int) or max_workers < 1:
        raise TypeError('"%s" should be an integer > 0' % name)


def validate_imap_kwargs(*, key, parallelism, buffer_size):
    if key is not None and not callable(key):
        raise TypeError('"key" should be callable')

    if not isinstance(parallelism, int) or parallelism < 1:
        raise TypeError('"parallelism" should be an integer > 0')

    if not isinstance(buffer_size, int) or buffer_size < 0:
        raise TypeError('"buffer_size" should be a positive integer')


class LazyGroupedThreadPoolExecutor(object):
    def __init__(self, max_workers):
        validate_max_workers('max_workers', max_workers)

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

            job()
            self.job_queue.task_done()
            put(self.output_queue, job)

    def __imap(self, iterable, func, *, ordered=False, key=None, parallelism=1,
               buffer_size=DEFAULT_BUFFER_SIZE):
        iterator = ThreadSafeIterator(iterable)
        self.output_queue = Queue() if not ordered else OrderedOutputQueue()

        # State
        job_counter = count()
        state = IterationState()
        buffer = Buffer(maxsize=buffer_size, parallelism=parallelism)

        def enqueue():
            while not self.teardown_event.is_set():

                # First we check to see if there is a suitable buffered job
                job = buffer.get()

                if job is None:

                    # Else we consume the iterator to find one
                    try:
                        item = next(iterator)
                    except StopIteration:
                        if not buffer.empty():
                            continue

                        state.declare_end()
                        break

                    group = None

                    if key is not None:
                        group = key(item)

                    job = Job(
                        func,
                        args=(item,),
                        index=next(job_counter),
                        group=group
                    )

                    buffer.put(job)
                    continue

                # Registering the job
                buffer.register_job(job)
                state.start_task()
                put(self.job_queue, job)

        def output():
            while not state.should_stop() and not self.teardown_event.is_set():
                job = get(self.output_queue)

                # Acknowledging this job was finished
                self.output_queue.task_done()
                buffer.unregister_job(job)
                state.finish_task()

                # Raising an error that occurred within worker function
                if job.exc_info is not None:
                    raise job.exc_info[1].with_traceback(job.exc_info[2])

                # Actually yielding the value to main thread
                yield job.result

            # Sanity tests
            assert buffer.is_clean()

            # Making sure we are getting rid of the dispatcher thread
            dispatcher.join()

        dispatcher = Thread(
            name='Thread-quenouille-%i-dispatcher' % id(self),
            target=enqueue,
            daemon=True
        )
        dispatcher.start()

        return output()

    def imap_unordered(self, iterable, func, *, key=None, parallelism=1,
                       buffer_size=DEFAULT_BUFFER_SIZE):

        validate_imap_kwargs(key=key, parallelism=parallelism, buffer_size=buffer_size)

        return self.__imap(
            iterable,
            func,
            ordered=False,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size
        )

    def imap(self, iterable, func, *, key=None, parallelism=1,
             buffer_size=DEFAULT_BUFFER_SIZE):

        validate_imap_kwargs(key=key, parallelism=parallelism, buffer_size=buffer_size)

        return self.__imap(
            iterable,
            func,
            ordered=True,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size
        )


def imap_unordered(iterable, func, threads, *, key=None, parallelism=1,
                   buffer_size=DEFAULT_BUFFER_SIZE):
    validate_max_workers('threads', threads)
    validate_imap_kwargs(key=key, parallelism=parallelism, buffer_size=buffer_size)

    def generator():
        with LazyGroupedThreadPoolExecutor(max_workers=threads) as executor:
            yield from executor.imap_unordered(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size
            )

    return generator()


def imap(iterable, func, threads, *, key=None, parallelism=1,
         buffer_size=DEFAULT_BUFFER_SIZE):
    validate_max_workers('threads', threads)
    validate_imap_kwargs(key=key, parallelism=parallelism, buffer_size=buffer_size)

    def generator():
        with LazyGroupedThreadPoolExecutor(max_workers=threads) as executor:
            yield from executor.imap(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size
            )

    return generator()
