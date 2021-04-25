# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
import sys
import time
from queue import Queue, Empty
from threading import Thread, Event, Lock, Condition, Barrier, BrokenBarrierError
from collections import OrderedDict
from collections.abc import Iterable, Sized
from itertools import count

from quenouille.exceptions import BrokenThreadPool
from quenouille.utils import (
    clear,
    flush,
    smash,
    is_queue,
    get_default_maxworkers,
    ThreadSafeIterator,
    SmartTimer
)
from quenouille.constants import (
    THE_END_IS_NIGH,
    DEFAULT_BUFFER_SIZE,
    TIMER_EPSILON
)


class Job(object):
    """
    Class representing a job to be performed by a worker thread.
    """

    __slots__ = ('func', 'item', 'index', 'group', 'result', 'exc_info')

    def __init__(self, func, item, index=None, group=None):
        self.func = func
        self.item = item
        self.index = index
        self.group = group
        self.result = None
        self.exc_info = None

    def __call__(self):
        try:
            self.result = self.func(self.item)
        except BaseException as e:

            # We catch the exception before flushing downstream to be
            # sure the job can be passed down the line but it might not be
            # the best thing to do. Time will tell...
            self.exc_info = sys.exc_info()

    def __repr__(self):
        return '<{name} id={id!r} index={index!r} group={group!r} item={item!r}>'.format(
            name=self.__class__.__name__,
            index=self.index,
            group=self.group,
            item=self.item,
            id=id(self)
        )


class IterationState(object):
    """
    Class representing an executor imap iteration state. It keeps tracks of
    counts of jobs started and finished, and is used to declare the end of
    the iterated stream so that everything can be synchronized.
    """

    def __init__(self):
        self.lock = Lock()
        self.started_count = 0
        self.finished_count = 0
        self.finished = False
        self.task_is_finished = Condition()

    def start_task(self):
        with self.lock:
            self.started_count += 1

    def finish_task(self):
        with self.lock:
            self.finished_count += 1

        with self.task_is_finished:
            self.task_is_finished.notify_all()

    def wait_for_any_task_to_finish(self):
        with self.task_is_finished:
            self.task_is_finished.wait()

    def has_running_tasks(self):
        with self.lock:
            return self.started_count > self.finished_count

    def declare_end(self):
        with self.lock:
            self.finished = True
            return self.__should_stop()

    def __should_stop(self):
        return self.finished and self.finished_count == self.started_count

    def should_stop(self):
        with self.lock:
            return self.__should_stop()

    def __repr__(self):
        return '<{name}{finished} started={started!r} done={done!r}>'.format(
            name=self.__class__.__name__,
            finished=(' finished' if self.finished else ''),
            started=self.started_count,
            done=self.finished_count
        )


class ThrottledGroups(object):
    """
    Class representing the groups being currently throttled. It also manages
    a timer tasked to release groups in the future.

    Note that this class does not use a lock to manage its resources because
    it is only managed either by the main thread or a Buffer instance which
    already perform the necessary actions using a lock.
    """
    def __init__(self, output_queue, throttle=0):

        # Properties
        self.output_queue = output_queue
        self.throttle = throttle
        self.identity = None

        # Containers
        self.groups = {}

        # Threading
        self.timer = None
        self.__registered_calback = None

    def __contains__(self, group):
        return group in self.groups

    def __reset(self):
        self.__cancel_timer()
        self.groups = {}

    def callback(self, fn):
        self.__registered_calback = fn

    def detach(self):
        self.__registered_calback = None

    def update(self, key, throttle):
        assert key is None or callable(key)

        new_identity = id(key)

        if new_identity != self.identity:
            self.__reset()

        self.identity = new_identity
        self.throttle = throttle

        assert isinstance(self.throttle, (int, float)) or callable(self.throttle)

    def __fire_callback(self):
        assert self.__registered_calback is not None
        self.__registered_calback()

    def add(self, job: Job):
        throttle_time = self.throttle

        if callable(self.throttle):
            throttle_time = self.throttle(
                job.group,
                job.item,
                job.result
            )

            if throttle_time is None:
                throttle_time = 0

            if not isinstance(throttle_time, (int, float)) or throttle_time < 0:
                raise TypeError('callable "throttle" must return numbers >= 0')

        if throttle_time != 0:
            assert job.group not in self

            self.groups[job.group] = time.time() + throttle_time
            self.__spawn_timer(throttle_time)

    def __cancel_timer(self):
        if self.timer is None:
            return

        self.timer.cancel()

        if self.timer.is_alive():
            self.timer.join()

        self.timer = None

    def __spawn_timer(self, throttle_time):
        if self.timer is not None:
            if throttle_time < self.timer.remaining():
                self.__cancel_timer()
            else:
                return

        timer = SmartTimer(
            throttle_time,
            self.timer_callback
        )

        self.timer = timer
        timer.start()

    def timer_callback(self):
        try:
            self.timer = None
            assert len(self.groups) != 0

            groups_to_release = []
            earliest_next_time = None
            current_time = time.time()

            for group, release_time in self.groups.items():
                if current_time >= (release_time - TIMER_EPSILON):
                    groups_to_release.append(group)
                else:
                    if earliest_next_time is None or release_time < earliest_next_time:
                        earliest_next_time = release_time

            assert len(groups_to_release) > 0

            # Actually releasing the groups
            for group in groups_to_release:
                del self.groups[group]

            # Relaunching timer?
            if earliest_next_time is not None:
                time_to_wait = earliest_next_time - current_time

                assert time_to_wait >= 0

                self.__spawn_timer(time_to_wait)

            self.__fire_callback()

        except BaseException as e:
            smash(self.output_queue, e)

    def teardown(self):
        self.__cancel_timer()
        self.groups = None


class Buffer(object):
    """
    Class tasked with an executor imap buffer where jobs are enqueued when
    read from the iterated stream.

    It's this class' job to emit items that can be processed by worker threads
    while respecting group parallelism constraints and throttling.

    Its #.get method can block when not suitable item can be emitted.

    Its #.put method will never block and should raise if putting the buffer
    into an incoherent state.
    """
    def __init__(self, throttled_groups, maxsize=1, parallelism=1):

        # Properties
        self.throttled_groups = throttled_groups
        self.maxsize = maxsize
        self.parallelism = parallelism

        assert isinstance(self.parallelism, int) or callable(self.parallelism)

        def throttle_callback():
            with self.condition:
                self.condition.notify_all()

        self.throttled_groups.callback(throttle_callback)

        # Threading
        self.condition = Condition()
        self.lock = Lock()

        # Containers
        self.items = OrderedDict()
        self.worked_groups = {}  # NOTE: not using a Counter here to avoid magic

    def __len__(self):
        with self.lock:
            return len(self.items)

    def is_clean(self):
        """
        This function is only used after an imap is over to ensure no
        dangling resources could be found.
        """
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

        if job.group in self.throttled_groups:
            return False

        count = self.worked_groups.get(job.group, 0)

        parallelism = self.parallelism

        if callable(self.parallelism):
            parallelism = self.parallelism(job.group)

            # No parallelism for a group basically means it is not being constrained
            if parallelism is None:
                return True

            if not isinstance(parallelism, int) or parallelism < 1:
                raise TypeError('callable "parallelism" must return positive integers')

        return count < parallelism

    def __find_suitable_job(self):
        for job in self.items.values():
            if self.__can_work(job):
                return job

        return None

    def put(self, job: Job):
        with self.lock:
            assert not self.__full()

            self.items[id(job)] = job

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

            self.throttled_groups.add(job)

        with self.condition:
            self.condition.notify_all()


class OrderedOutputBuffer(object):
    """
    Class in charge of yielding values in the same order as they were extracted
    from the iterated stream.

    Note that this requires to buffer values into memory until the next valid
    item has been processed by a worker thread.
    """

    def __init__(self):
        self.last_index = 0
        self.items = {}

    def is_clean(self):
        """
        This function is only used after an imap is over to ensure no
        dangling resource could be found.
        """
        return len(self.items) == 0

    def flush(self):
        while self.last_index in self.items:
            yield self.items.pop(self.last_index).result
            self.last_index += 1

    def output(self, job):
        if job.index == self.last_index:
            self.last_index += 1
            yield job.result
            yield from self.flush()
        else:
            self.items[job.index] = job


def validate_threadpool_kwargs(name, max_workers=None, initializer=None, initargs=None,
                               wait=None, daemonic=None):
    if max_workers is None:
        return

    if not isinstance(max_workers, int) or max_workers < 1:
        raise TypeError('"%s" should be an integer > 0' % name)

    if initializer is not None and not callable(initializer):
        raise TypeError('"initializer" is not callable')

    if initargs is not None and not isinstance(initargs, Iterable):
        raise TypeError('"initargs" is not iterable')

    if not isinstance(wait, bool):
        raise TypeError('"wait" should be boolean')

    if not isinstance(daemonic, bool):
        raise TypeError('"daemonic" should be boolean')


def validate_imap_kwargs(iterable, func, *, max_workers, key, parallelism, buffer_size,
                         throttle):

    if not isinstance(iterable, Iterable) and not is_queue(iterable):
        raise TypeError('target is not iterable nor a queue')

    if not callable(func):
        raise TypeError('worker function is not callable')

    if key is not None and not callable(key):
        raise TypeError('"key" is not callable')

    if not isinstance(parallelism, (int, float)) and not callable(parallelism):
        raise TypeError('"parallelism" is not a number nor callable')

    if isinstance(parallelism, int) and parallelism < 0:
        raise TypeError('"parallelism" is not a positive integer')

    # if parallelism > max_workers:
    #     raise TypeError('"parallelism" cannot be greater than the number of workers')

    if not isinstance(buffer_size, int) or buffer_size < 1:
        raise TypeError('"buffer_size" is not an integer > 0')

    if not isinstance(throttle, (int, float)) and not callable(throttle):
        raise TypeError('"throttle" is not a number nor callable')

    if isinstance(throttle, (int, float)) and throttle < 0:
        raise TypeError('"throttle" cannot be negative')


class ThreadPoolExecutor(object):
    """
    Quenouille custom ThreadPoolExecutor able to lazily pull items to process
    from iterated streams, all while bounding used memory and respecting
    group parallelism and throttling.

    Args:
        max_workers (int, optional): Number of threads to use.
            Defaults to min(32, os.cpu_count() + 1).
        initializer (callable, optional): Function that will be run when starting
            each worker thread. Can be useful to setup a threading local context
            for instance.
        initargs (iterable, optional): Arguments to pass to the thread initializer
            function.
        join (bool, optional): Whether to join worker threads on executor teardown.
            Defaults to True.
        daemonic (bool, optional): Whether to spawn daemon worker threads.
            Defaults to False.
    """

    def __init__(self, max_workers=None, initializer=None, initargs=tuple(),
                 wait=True, daemonic=False):

        # Validation and defaults
        validate_threadpool_kwargs(
            'max_workers',
            max_workers,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic
        )

        if max_workers is None:
            max_workers = get_default_maxworkers()

        # Properties
        self.max_workers = max_workers
        self.wait = wait
        self.daemonic = daemonic

        # Init
        self.initializer = initializer
        self.initargs = list(initargs)

        # Queues
        self.job_queue = Queue(maxsize=max_workers)
        self.output_queue = Queue()

        # Threading
        self.throttled_groups = ThrottledGroups(self.output_queue)
        self.teardown_event = Event()
        self.teardown_lock = Lock()
        self.broken_lock = Lock()
        self.imap_lock = Lock()
        self.boot_barrier = Barrier(max_workers + 1)

        # State
        self.closed = False

        # Thread pool
        self.threads = [
            Thread(
                name='Thread-quenouille-%i-%i' % (id(self), n),
                target=self.__worker,
                daemon=self.daemonic
            )
            for n in range(max_workers)
        ]

        # Actual initialization
        try:
            for thread in self.threads:
                thread.start()
        except BaseException:
            self.boot_barrier.abort()
            self.shutdown(wait=self.wait)
            raise

        try:
            self.boot_barrier.wait()
        except BrokenBarrierError:
            self.shutdown(wait=self.wait)
            raise BrokenThreadPool

    def __enter__(self):
        if self.closed:
            raise RuntimeError('cannot re-enter a closed executor')

        return self

    def __exit__(self, *args):
        self.shutdown(wait=self.wait)

    def shutdown(self, wait=True):
        if self.closed:
            return

        with self.teardown_lock:
            self.teardown_event.set()

            # Killing workers (cancel jobs and flushing end messages)
            self.__kill_workers()

            # Clearing the ouput queue since we won't be iterating anymore
            self.__clear_output_queue()

            # Clearing throttling state
            self.throttled_groups.teardown()

            # Waiting for worker threads to end
            if wait:
                for thread in self.threads:
                    if thread.is_alive():
                        thread.join()

            self.closed = True

    def __clear_output_queue(self):
        clear(self.output_queue)

    def __cancel_all_jobs(self):
        clear(self.job_queue)

    def __kill_workers(self):
        self.__cancel_all_jobs()

        n = sum(1 if t.is_alive() else 0 for t in self.threads)
        flush(self.job_queue, n, THE_END_IS_NIGH)

    def __worker(self):

        # Thread initialization
        try:
            if self.initializer is not None:
                self.initializer(*self.initargs)

            self.boot_barrier.wait()

        # NOTE: this naturally includes `BrokenBarrierError`
        except BaseException:
            self.boot_barrier.abort()
            return

        # Thread job consumer
        try:
            while not self.teardown_event.is_set():
                try:
                    job = self.job_queue.get()

                    # Signaling we must tear down the worker thread
                    if job is THE_END_IS_NIGH:
                        break

                    # Actually performing the given task
                    job()

                finally:
                    self.job_queue.task_done()

                assert self.output_queue is not None
                self.output_queue.put(job)

        except BaseException as e:
            smash(self.output_queue, e)

    def __imap(self, iterable, func, *, ordered=False, key=None, parallelism=1,
               buffer_size=DEFAULT_BUFFER_SIZE, throttle=0):

        # Cannot run in multiple threads
        if self.imap_lock.locked():
            raise RuntimeError('cannot run multiple executor methods concurrently from different threads')

        # Cannot run if already closed
        if self.closed:
            raise RuntimeError('cannot use thread pool after teardown')

        assert not self.boot_barrier.broken

        self.imap_lock.acquire()

        iterator = None
        iterable_is_queue = is_queue(iterable)

        if not iterable_is_queue:
            iterator = ThreadSafeIterator(iterable)

        # State
        self.throttled_groups.update(key, throttle)
        job_counter = count()
        end_event = Event()
        state = IterationState()
        buffer = Buffer(
            self.throttled_groups,
            maxsize=buffer_size,
            parallelism=parallelism
        )
        ordered_output_buffer = OrderedOutputBuffer()

        def enqueue():
            try:
                while not end_event.is_set():

                    # First we check to see if there is a suitable buffered job
                    job = buffer.get()

                    if job is None:

                        # Else we consume the iterator to find one
                        try:
                            if not iterable_is_queue:
                                item = next(iterator)
                            else:
                                try:
                                    item = iterable.get(block=False)
                                except Empty:
                                    if state.has_running_tasks():
                                        state.wait_for_any_task_to_finish()
                                        continue
                                    else:
                                        raise StopIteration
                        except StopIteration:
                            if not buffer.empty():
                                continue

                            should_stop = state.declare_end()

                            # Sometimes the end of the iterator lags behind
                            # the output generator
                            if should_stop:
                                self.output_queue.put_nowait(THE_END_IS_NIGH)

                            break

                        group = None

                        if key is not None:
                            group = key(item)

                        job = Job(
                            func,
                            item=item,
                            index=next(job_counter),
                            group=group
                        )

                        buffer.put(job)
                        continue

                    # Registering the job
                    buffer.register_job(job)
                    state.start_task()
                    self.job_queue.put(job)

            except BaseException as e:
                smash(self.output_queue, e)

        def cleanup(normal_exit=True):
            end_event.set()

            # Clearing the job queue to cancel next jobs
            self.__cancel_all_jobs()

            # Clearing the ouput queue since we won't be iterating anymore
            self.__clear_output_queue()

            # Sanity tests
            if normal_exit:
                assert buffer.is_clean()
                assert ordered_output_buffer.is_clean()

            # Detaching timer callback
            self.throttled_groups.detach()

            # Making sure we are getting rid of the dispatcher thread
            dispatcher.join()

            self.imap_lock.release()

        def output():
            raised = False

            try:
                while not state.should_stop() and not end_event.is_set():
                    job = self.output_queue.get()

                    if job is THE_END_IS_NIGH:
                        break

                    # Catching keyboard interrupts and other unrecoverable errors
                    if isinstance(job, BaseException):
                        raise job

                    # Unregistering job in buffer to let other threads continue working
                    buffer.unregister_job(job)

                    # Raising an error that occurred within worker function
                    # NOTE: shenanigans with tracebacks don't seem to change anything
                    if job.exc_info is not None:
                        raise job.exc_info[1].with_traceback(job.exc_info[2])

                    # Actually yielding the value to main thread
                    if ordered:
                        yield from ordered_output_buffer.output(job)
                    else:
                        yield job.result

                    # Acknowledging the job was finished
                    # NOTE: this was moved after yielding items so that the
                    # generator body may enqueue new jobs. It is possible that
                    # this has a slight perf hit if the body performs heavy work?
                    state.finish_task()

                    # Cleanup memory and avoid keeping references attached to
                    # ease up garbage collection
                    del job

            except:
                raised = True
                raise

            finally:
                cleanup(not raised)

        dispatcher = Thread(
            name='Thread-quenouille-%i-dispatcher' % id(self),
            target=enqueue,
            daemon=self.daemonic
        )
        dispatcher.start()

        return output()

    def imap_unordered(self, iterable, func, *, key=None, parallelism=1,
                       buffer_size=DEFAULT_BUFFER_SIZE, throttle=0):

        validate_imap_kwargs(
            iterable=iterable,
            func=func,
            max_workers=self.max_workers,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle
        )

        return self.__imap(
            iterable,
            func,
            ordered=False,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle
        )

    def imap(self, iterable, func, *, key=None, parallelism=1,
             buffer_size=DEFAULT_BUFFER_SIZE, throttle=0):

        validate_imap_kwargs(
            iterable=iterable,
            func=func,
            max_workers=self.max_workers,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle
        )

        return self.__imap(
            iterable,
            func,
            ordered=True,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle
        )


def imap_unordered(iterable, func, threads=None, *, key=None, parallelism=1,
                   buffer_size=DEFAULT_BUFFER_SIZE, throttle=0,
                   initializer=None, initargs=tuple(), wait=True, daemonic=False):

    validate_threadpool_kwargs(
        'threads',
        threads,
        initializer=initializer,
        initargs=initargs,
        wait=wait,
        daemonic=daemonic
    )
    validate_imap_kwargs(
        iterable=iterable,
        func=func,
        max_workers=threads,
        key=key,
        parallelism=parallelism,
        buffer_size=buffer_size,
        throttle=throttle
    )

    if isinstance(iterable, Sized):
        threads = min(
            len(iterable),
            threads or float('inf')
        )

    def generator():
        with ThreadPoolExecutor(
            max_workers=threads,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic
        ) as executor:
            yield from executor.imap_unordered(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size,
                throttle=throttle
            )

    return generator()


def imap(iterable, func, threads=None, *, key=None, parallelism=1,
         buffer_size=DEFAULT_BUFFER_SIZE, throttle=0,
         initializer=None, initargs=tuple(), wait=True, daemonic=False):

    validate_threadpool_kwargs(
        'threads',
        threads,
        initializer=initializer,
        initargs=initargs,
        wait=wait,
        daemonic=daemonic
    )
    validate_imap_kwargs(
        iterable=iterable,
        func=func,
        max_workers=threads,
        key=key,
        parallelism=parallelism,
        buffer_size=buffer_size,
        throttle=throttle
    )

    if isinstance(iterable, Sized):
        threads = min(
            len(iterable),
            threads or float('inf')
        )

    def generator():
        with ThreadPoolExecutor(
            max_workers=threads,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic
        ) as executor:
            yield from executor.imap(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size,
                throttle=throttle
            )

    return generator()


def generate_function_doc(ordered=False):
    disclaimer = 'Note that results will be yielded in an arbitrary order.'

    if ordered:
        disclaimer = 'Note that results will be yielded in same order as the input.'

    return (
        """
        Function consuming tasks from any iterable, dispatching them to a pool
        of worker threads to finally yield the produced results.

        {disclaimer}

        Args:
            iterable (iterable or queue): iterable of items to process or queue of
                items to process.
            func (callable): The task to perform with each item.
            threads (int, optional): Number of threads to use.
                Defaults to min(32, os.cpu_count() + 1).
            key (callable, optional): Function returning to which "group" a given
                item is supposed to belong. This will be used to ensure maximum
                group parallelism is respected.
            parallelism (int or callable, optional): Number of threads allowed to work
                on a same group at once. Can also be a function taking a group and
                returning its parallelism. Defaults to 1.
            buffer_size (int, optional): Max number of items the function will
                buffer into memory while attempting to find an item that can be
                passed to a worker immediately, all while respecting throttling and
                group parallelism. Defaults to 1024.
            throttle (float or callable, optional): Optional throttle time, in
                seconds, to wait before processing the next item of a given group.
                Can also be a function taking last group, item and returning next
                throttle time for this group.
            initializer (callable, optional): Function that will be run when starting
                each worker thread. Can be useful to setup a threading local context
                for instance.
            initargs (iterable, optional): Arguments to pass to the thread initializer
                function.
            wait (bool, optional): Whether to join worker threads on executor shutdown.
                Defaults to True.
            daemonic (bool, optional): Whether to spawn daemon worker threads.
                Defaults to False.

        Yields:
            any: Will yield results based on given worker function.

        """
    ).format(disclaimer=disclaimer)


imap_unordered.__doc__ = generate_function_doc()
imap.__doc__ = generate_function_doc(ordered=True)
