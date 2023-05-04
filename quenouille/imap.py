# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
from typing import (
    cast,
    Generic,
    TypeVar,
    Optional,
    Hashable,
    Callable,
    Dict,
    List,
    Any,
    Iterator,
    Iterable,
    Union,
)

import sys

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

import time
from queue import Queue, Empty
from threading import Thread, Event, Lock, Condition, Barrier, BrokenBarrierError
from collections import OrderedDict
from collections.abc import Sized
from itertools import count

from quenouille.exceptions import (
    BrokenThreadPool,
    InvalidThrottleParallelismCombination,
)
from quenouille.utils import (
    clear,
    flush,
    smash,
    queue_iter,
    is_usable_queue,
    get_default_maxworkers,
    SmartTimer,
    TimedHeapSet,
)
from quenouille.constants import THE_END_IS_NIGH, DEFAULT_BUFFER_SIZE, TIMER_EPSILON

ItemType = TypeVar("ItemType", covariant=True)


class QuenouilleQueueProtocol(Protocol[ItemType]):
    def get_nowait(self) -> ItemType:
        ...


GroupType = TypeVar("GroupType", bound=Hashable)
ResultType = TypeVar("ResultType")
ImapTarget = Union[QuenouilleQueueProtocol[ItemType], Iterable[ItemType]]
ParallelismType = Union[Callable[[GroupType], int], int]
ThrottleType = Union[
    Callable[[Optional[GroupType], ItemType, Optional[ResultType]], float], float
]


class Job(Generic[ItemType, GroupType, ResultType]):
    """
    Class representing a job to be performed by a worker thread.
    """

    __slots__ = ("func", "item", "index", "group", "result", "exc_info")

    def __init__(
        self,
        func: Callable[[ItemType], ResultType],
        item: ItemType,
        index: int,
        group: Optional[GroupType] = None,
    ):
        self.func = func  # type: Callable[[ItemType], ResultType]
        self.item = item  # type: ItemType
        self.index = index  # type: int
        self.group = group  # type: Optional[GroupType]
        self.result = None  # type: Optional[ResultType]
        self.exc_info = None

    def __call__(self) -> None:
        try:
            self.result = self.func(self.item)
        except BaseException:
            # We catch the exception before flushing downstream to be
            # sure the job can be passed down the line but it might not be
            # the best thing to do. Time will tell...
            self.exc_info = sys.exc_info()

    def __repr__(self):
        return "<{name} index={index!r} group={group!r} item={item!r}>".format(
            name=self.__class__.__name__,
            index=self.index,
            group=self.group,
            item=self.item,
        )


class IterationState(object):
    """
    Class representing an executor imap iteration state. It keeps tracks of
    counts of jobs started and finished, and is used to declare the end of
    the iterated stream so that everything can be synchronized.
    """

    def __init__(self):
        self.lock = Lock()  # type: Lock
        self.started_count = 0  # type: int
        self.finished_count = 0  # type: int
        self.finished = False  # type: bool
        self.task_is_finished = Condition()  # type: Condition

    def start_task(self) -> None:
        with self.lock:
            self.started_count += 1

    def finish_task(self) -> None:
        with self.lock:
            self.finished_count += 1

        with self.task_is_finished:
            self.task_is_finished.notify_all()

    def wait_for_any_task_to_finish(self) -> None:
        with self.task_is_finished:
            self.task_is_finished.wait()

    def has_running_tasks(self) -> bool:
        with self.lock:
            return self.started_count > self.finished_count

    def declare_end(self) -> bool:
        with self.lock:
            self.finished = True
            return self.__should_stop()

    def __should_stop(self) -> bool:
        return self.finished and self.finished_count == self.started_count

    def should_stop(self) -> bool:
        with self.lock:
            return self.__should_stop()

    def teardown(self) -> None:
        # We release all threads waiting for us
        with self.task_is_finished:
            self.task_is_finished.notify_all()

    def __repr__(self):
        return "<{name}{finished} started={started!r} done={done!r}>".format(
            name=self.__class__.__name__,
            finished=(" finished" if self.finished else ""),
            started=self.started_count,
            done=self.finished_count,
        )


class ThrottledGroups(Generic[ItemType, GroupType, ResultType]):
    """
    Class representing the groups being currently throttled. It also manages
    a timer tasked to release groups in the future.

    Note that this class does not use a lock to manage its resources because
    it is only managed either by the main thread or a Buffer instance which
    already perform the necessary actions using a lock.
    """

    def __init__(
        self,
        output_queue: Queue,
        throttle: ThrottleType[GroupType, ItemType, ResultType] = 0,
    ):
        # Properties
        self.output_queue = output_queue  # type: Queue
        self.throttle = throttle  # type: ThrottleType[GroupType, ItemType, ResultType]
        self.identity = None  # type: Optional[Any]

        # Containers
        self.groups = {}  # type: Dict[GroupType, float]

        # Threading
        self.timer = None  # type: Optional[SmartTimer]
        self.__registered_calback = None  # type: Optional[Callable[[int], None]]

    def __contains__(self, group: GroupType) -> bool:
        return group in self.groups

    def __reset(self) -> None:
        self.__cancel_timer()
        self.groups = {}

    def callback(self, fn: Callable[[int], None]) -> None:
        self.__registered_calback = fn

    def detach(self) -> None:
        self.__reset()
        self.__registered_calback = None

    def update(
        self, key, throttle: ThrottleType[GroupType, ItemType, ResultType]
    ) -> None:
        assert key is None or callable(key)

        if key is not self.identity:
            self.__reset()

        self.identity = key
        self.throttle = throttle

        assert isinstance(self.throttle, (int, float)) or callable(self.throttle)

    def add(self, job: Job[ItemType, GroupType, ResultType]) -> None:
        throttle_time = self.throttle

        if callable(self.throttle):
            throttle_time = self.throttle(job.group, job.item, job.result)

            if throttle_time is None:
                throttle_time = 0

            if not isinstance(throttle_time, (int, float)) or throttle_time < 0:
                raise TypeError('callable "throttle" must return numbers >= 0')

        if throttle_time != 0:
            group = cast(GroupType, job.group)

            if group in self:
                raise InvalidThrottleParallelismCombination(
                    "throttle cannot be > 0 for a group with parallelism > 1"
                )

            self.groups[group] = time.time() + throttle_time  # type: ignore
            self.__spawn_timer(throttle_time)  # type: ignore

    def __cancel_timer(self) -> None:
        if self.timer is None:
            return

        self.timer.cancel()

        if self.timer.is_alive():
            self.timer.join()

        self.timer = None

    def __spawn_timer(self, throttle_time: float) -> None:
        if self.timer is not None:
            if throttle_time < self.timer.remaining():
                self.__cancel_timer()
            else:
                return

        timer = SmartTimer(throttle_time, self.timer_callback)
        timer.daemon = True

        self.timer = timer
        timer.start()

    def timer_callback(self) -> None:
        try:
            self.timer = None

            # NOTE: because of timing precision issues and in some weird race
            # conditions beyond my understanding, this callback may fire
            # even when no groups remain to be throttled. In this case we just
            # let this callback be a noop.
            if len(self.groups) == 0:
                return

            groups_to_release = []
            earliest_next_time = None
            current_time = time.time()

            for group, release_time in self.groups.items():
                if current_time >= (release_time - TIMER_EPSILON):
                    groups_to_release.append(group)
                else:
                    if earliest_next_time is None or release_time < earliest_next_time:
                        earliest_next_time = release_time

            # NOTE: except for exotic race conditions that I did not foresee
            # this assertion should never fail because it is logically the same
            # as the one above, testing that groups are not empty.
            # NOTE: because of some timer precision issues that can arise
            # sometimes (in spite of the epsilon), this callback can be
            # a noop, in which case it should just respawn the timer until the
            # next suitable time.
            assert len(groups_to_release) > 0 or earliest_next_time is not None

            # Actually releasing the groups
            for group in groups_to_release:
                del self.groups[group]

            # Relaunching timer?
            if earliest_next_time is not None:
                time_to_wait = earliest_next_time - current_time

                assert time_to_wait >= 0

                self.__spawn_timer(time_to_wait)

            # NOTE: this condition is related to the explanation above
            if groups_to_release:
                assert self.__registered_calback is not None
                self.__registered_calback(len(groups_to_release))

        except BaseException as e:
            smash(self.output_queue, e)

    def teardown(self) -> None:
        self.__cancel_timer()
        del self.groups


class Buffer(Generic[ItemType, GroupType, ResultType]):
    """
    Class tasked with an executor imap buffer where jobs are enqueued when
    read from the iterated stream.

    It's this class' job to emit items that can be processed by worker threads
    while respecting group parallelism constraints and throttling.

    Its #.get method can block when not suitable item can be emitted.

    Its #.put method will never block and should raise if putting the buffer
    into an incoherent state.
    """

    def __init__(
        self,
        throttled_groups: ThrottledGroups[ItemType, GroupType, ResultType],
        maxsize: int = 1,
        parallelism: ParallelismType[GroupType] = 1,
    ):
        # Properties
        self.throttled_groups = throttled_groups  # type: ThrottledGroups
        self.maxsize = maxsize  # type: int
        self.parallelism = parallelism  # type: ParallelismType[GroupType]

        assert isinstance(self.parallelism, int) or callable(self.parallelism)

        def throttle_callback(n: int):
            with self.condition:
                self.condition.notify(n)

        self.throttled_groups.callback(throttle_callback)

        # Threading
        self.condition = Condition()  # type: Condition
        self.lock = Lock()  # type: Lock
        self.teardown_event = Event()  # type: Event

        # Containers
        self.items = (
            OrderedDict()
        )  # type: OrderedDict[int, Job[ItemType, GroupType, ResultType]]

        # NOTE: not using a Counter here to avoid magic
        self.worked_groups = {}  # type: Dict[GroupType, int]

    def __len__(self) -> int:
        with self.lock:
            return len(self.items)

    def teardown(self) -> None:
        self.teardown_event.set()

        with self.condition:
            self.condition.notify_all()

        del self.items
        del self.worked_groups

    def is_clean(self) -> bool:
        """
        This function is only used after an imap is over to ensure no
        dangling resources could be found.
        """
        with self.lock:
            return len(self.items) == 0 and len(self.worked_groups) == 0

    def __full(self) -> bool:
        count = len(self.items)
        assert count <= self.maxsize
        return count == self.maxsize

    def full(self) -> bool:
        with self.lock:
            return self.__full()

    def __empty(self) -> bool:
        count = len(self.items)
        assert count <= self.maxsize
        return count == 0

    def empty(self) -> bool:
        with self.lock:
            return self.__empty()

    def __can_work(self, job: Job[ItemType, GroupType, ResultType]) -> bool:
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

        return count < parallelism  # type: ignore

    def __find_suitable_job(self) -> Optional[Job[ItemType, GroupType, ResultType]]:
        for job in self.items.values():
            if self.__can_work(job):
                return job

        return None

    def put(self, job: Job[ItemType, GroupType, ResultType]) -> None:
        with self.lock:
            assert not self.__full()

            self.items[job.index] = job

    def get(self) -> Optional[Job[ItemType, GroupType, ResultType]]:
        while not self.teardown_event.is_set():
            self.lock.acquire()

            if len(self.items) == 0:
                self.lock.release()
                return None

            job = self.__find_suitable_job()

            if job is not None:
                del self.items[job.index]
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

    def register_job(self, job: Job[ItemType, GroupType, ResultType]) -> None:
        with self.lock:
            group = job.group

            # None group does not count
            if group is None:
                return

            if group not in self.worked_groups:
                self.worked_groups[group] = 1
            else:
                self.worked_groups[group] += 1

    def unregister_job(self, job: Job[ItemType, GroupType, ResultType]) -> None:
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
            self.condition.notify()


class OrderedOutputBuffer(Generic[ItemType, GroupType, ResultType]):
    """
    Class in charge of yielding values in the same order as they were extracted
    from the iterated stream.

    Note that this requires to buffer values into memory until the next valid
    item has been processed by a worker thread.
    """

    def __init__(self):
        self.last_index = 0  # type: int
        self.items = {}  # type: Dict[int, ResultType]

    def is_clean(self) -> bool:
        """
        This function is only used after an imap is over to ensure no
        dangling resource could be found.
        """
        return len(self.items) == 0

    def flush(self) -> Iterator[ResultType]:
        while self.last_index in self.items:
            yield self.items.pop(self.last_index)
            self.last_index += 1

    def output(self, job: Job[ItemType, GroupType, ResultType]) -> Iterator[ResultType]:
        if job.index == self.last_index:
            self.last_index += 1
            yield job.result  # type: ignore
            yield from self.flush()
        else:
            self.items[job.index] = job.result  # type: ignore


def validate_threadpool_kwargs(
    name,
    max_workers=None,
    initializer=None,
    initargs=None,
    wait=None,
    daemonic=None,
    excepthook=None,
) -> None:
    if max_workers is None:
        return

    if not isinstance(max_workers, int) or max_workers < 0:
        raise TypeError('"%s" should be an integer >= 0' % name)

    if initializer is not None and not callable(initializer):
        raise TypeError('"initializer" is not callable')

    if initargs is not None and not isinstance(initargs, Iterable):
        raise TypeError('"initargs" is not iterable')

    if not isinstance(wait, bool):
        raise TypeError('"wait" should be boolean')

    if not isinstance(daemonic, bool):
        raise TypeError('"daemonic" should be boolean')

    if not isinstance(excepthook, bool):
        raise TypeError('"excepthook" should be boolean')


def validate_imap_kwargs(
    iterable, func, *, key, parallelism, buffer_size, throttle
) -> None:
    if not isinstance(iterable, Iterable) and not is_usable_queue(iterable):
        raise TypeError("target is not iterable nor a queue")

    if not callable(func):
        raise TypeError("worker function is not callable")

    if key is not None and not callable(key):
        raise TypeError('"key" is not callable')

    if not isinstance(parallelism, (int, float)) and not callable(parallelism):
        raise TypeError('"parallelism" is not a number nor callable')

    if isinstance(parallelism, int) and parallelism < 0:
        raise TypeError('"parallelism" is not a positive integer')

    if not isinstance(buffer_size, int) or buffer_size < 0:
        raise TypeError('"buffer_size" is not an integer >= 0')

    if not isinstance(throttle, (int, float)) and not callable(throttle):
        raise TypeError('"throttle" is not a number nor callable')

    if isinstance(throttle, (int, float)) and throttle < 0:
        raise TypeError('"throttle" cannot be negative')


class ThreadPoolExecutor(object):
    """
    Quenouille custom ThreadPoolExecutor able to lazily pull items to process
    from iterated streams, all while bounding used memory and respecting
    group parallelism and throttling.

    Note that if given max_workers=0, this pool will still work, but without
    relying on threads whatsoever. This can be useful in some scenarios where
    you want to avoid duplicating code.

    Args:
        max_workers (int, optional): Number of threads to use.
            Defaults to min(32, os.cpu_count() + 1).
        initializer (callable, optional): Function that will be run when starting
            each worker thread. Can be useful to setup a threading local context
            for instance.
        initargs (iterable, optional): Arguments to pass to the thread initializer
            function.
        wait (bool, optional): Whether to join worker threads on executor teardown.
            Defaults to True.
        daemonic (bool, optional): Whether to spawn daemon worker threads.
            Defaults to False.
    """

    def __init__(
        self,
        max_workers: Optional[int] = None,
        initializer: Optional[Callable[..., None]] = None,
        initargs: Iterable[Any] = tuple(),
        wait: bool = True,
        daemonic: bool = False,
        excepthook: bool = False,
    ):
        # Validation and defaults
        validate_threadpool_kwargs(
            "max_workers",
            max_workers,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic,
            excepthook=excepthook,
        )

        if max_workers is None:
            max_workers = get_default_maxworkers()

        # Properties
        self.max_workers = max_workers  # type: int
        self.dummy = max_workers == 0  # type: bool
        self.wait = wait  # type: bool
        self.daemonic = daemonic  # type: bool

        # Init
        self.initializer = initializer
        self.initargs = list(initargs)

        # Queues
        self.job_queue = Queue(maxsize=max_workers)  # type: Queue
        self.output_queue = Queue()  # type: Queue

        # Threading
        self.throttled_groups = ThrottledGroups(
            self.output_queue
        )  # ThrottledGroups[ItemType, GroupType, ResultType]
        self.teardown_event = Event()  # type: Event
        self.teardown_lock = Lock()  # type: Lock
        self.broken_lock = Lock()  # type: Lock
        self.imap_lock = Lock()  # type Lock
        self.boot_barrier = Barrier(max_workers + 1)  # type: Barrier

        # State
        self.patching_excepthook = excepthook
        self.original_excepthook = None
        self.cleanup = None
        self.closed = False  # type: bool

        if self.dummy:
            # If pool is dummy, we run the initializer synchronously
            if self.initializer is not None:
                self.initializer(*self.initargs)
            return

        # Thread pool
        self.threads = [
            Thread(
                name="Thread-quenouille-%i-%i" % (id(self), n),
                target=self.__worker,
                daemon=self.daemonic,
            )
            for n in range(max_workers)
        ]  # type: List[Thread]

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

    @property
    def is_actually_multithreaded(self) -> bool:
        return self.dummy

    def __enter__(self):
        # NOTE: nothing needs to happens if the executor is not multithreaded
        if self.dummy:
            return self

        if self.closed:
            raise RuntimeError("cannot re-enter a closed executor")

        if not self.patching_excepthook:
            return self

        self.original_excepthook = sys.excepthook

        def executor_excepthook(exc_type, exc_value, exc_traceback):
            if self.cleanup is not None:
                self.cleanup(False)

            # NOTE: this is a subset of the shutdown routine
            self.__kill_workers()

            if self.wait:
                for thread in self.threads:
                    if thread.is_alive():
                        thread.join()

            self.closed = True

            self.original_excepthook(exc_type, exc_value, exc_traceback)  # type: ignore

        sys.excepthook = executor_excepthook

        return self

    def __exit__(self, *args) -> Optional[bool]:
        # NOTE: nothing needs to happens if the executor is not multithreaded
        if self.dummy:
            return

        if self.patching_excepthook:
            sys.excepthook = self.original_excepthook
            self.original_excepthook = None

        self.shutdown(wait=self.wait)

    def shutdown(self, wait=True) -> None:
        # NOTE: shutdown is a noop if the executor is not multithreaded
        if self.dummy:
            return

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

    def __clear_output_queue(self) -> None:
        clear(self.output_queue)

    def __cancel_all_jobs(self) -> None:
        clear(self.job_queue)

    def __kill_workers(self) -> None:
        self.__cancel_all_jobs()

        n = sum(1 if t.is_alive() else 0 for t in self.threads)
        flush(self.job_queue, n, THE_END_IS_NIGH)

    def __worker(self) -> None:
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
                    job()  # type: ignore

                finally:
                    self.job_queue.task_done()

                assert self.output_queue is not None
                self.output_queue.put(job)

        except BaseException as e:
            smash(self.output_queue, e)

    def __imap_sync(
        self,
        iterable: ImapTarget[ItemType],
        func: Callable[[ItemType], ResultType],
        *,
        key: Optional[Callable[[ItemType], GroupType]] = None,
        throttle: ThrottleType[GroupType, ItemType, ResultType] = 0
    ) -> Iterator[ResultType]:
        if is_usable_queue(iterable):
            items = queue_iter(iterable)  # type: ignore
        else:
            items = iterable

        items = cast(Iterator[ItemType], items)

        # TODO: make throttling work per group
        timers = TimedHeapSet()  # type: TimedHeapSet[GroupType]

        for item in items:
            group = None if key is None else key(item)

            if group is not None:
                # NOTE: wait_for also perform cleanup for us
                timers.wait_for(group)

            result = func(item)

            if group is not None and throttle is not None:
                if callable(throttle):
                    duration = throttle(group, item, result)
                else:
                    duration = throttle

                if duration > 0:
                    timers.add(group, duration)

            yield result

    def __imap(
        self,
        iterable: ImapTarget[ItemType],
        func: Callable[[ItemType], ResultType],
        *,
        ordered: bool = False,
        key: Optional[Callable[[ItemType], GroupType]] = None,
        parallelism: ParallelismType[GroupType] = 1,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        throttle: ThrottleType[GroupType, ItemType, ResultType] = 0
    ) -> Iterator[ResultType]:
        if self.dummy:
            return self.__imap_sync(iterable, func, key=key, throttle=throttle)

        # Cannot run in multiple threads
        if self.imap_lock.locked():
            raise RuntimeError(
                "cannot run multiple executor methods concurrently from different threads"
            )

        # Cannot run if already closed
        if self.closed:
            raise RuntimeError("cannot use thread pool after teardown")

        assert not self.boot_barrier.broken

        self.imap_lock.acquire()

        iterator = None
        iterable_is_queue = is_usable_queue(iterable)

        if not iterable_is_queue:
            iterator = iter(iterable)  # type: ignore

        # State
        self.throttled_groups.update(key, throttle)
        job_counter = count()
        end_event = Event()
        state = IterationState()
        buffer = Buffer(
            self.throttled_groups, maxsize=buffer_size, parallelism=parallelism
        )  # type: Buffer[ItemType, GroupType, ResultType]
        ordered_output_buffer = (
            OrderedOutputBuffer()
        )  # type: OrderedOutputBuffer[ItemType, GroupType, ResultType]

        # Recasting for this context
        self.job_queue = cast(
            "Queue[Union[Job[ItemType, GroupType, ResultType], object]]", self.job_queue
        )
        self.output_queue = cast(
            "Queue[Union[BaseException, Job[ItemType, GroupType, ResultType], object]]",
            self.output_queue,
        )

        # NOTE: currently buffer_size=0 does not work with throttle nor group parallelism
        def enqueue():
            try:
                while not end_event.is_set():
                    # First we check to see if there is a suitable buffered job
                    job = None if buffer_size == 0 else buffer.get()

                    if job is None:
                        # Else we consume the iterator to find one
                        try:
                            if not iterable_is_queue:
                                item = next(iterator)  # type: ignore
                            else:
                                try:
                                    item = iterable.get_nowait()  # type: ignore
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

                        job = Job(func, item=item, index=next(job_counter), group=group)

                        if buffer_size > 0:
                            buffer.put(job)
                            continue

                    # Registering the job
                    buffer.register_job(job)
                    state.start_task()
                    self.job_queue.put(job)

            except BaseException as e:
                smash(self.output_queue, e)

        def cleanup(normal_exit: bool = True) -> None:
            # NOTE: this is somewhat of a lock to avoid double firing of the
            # cleanup function in some race conditions with sys.excepthook
            self.cleanup = None

            end_event.set()

            # Clearing the job queue to cancel next jobs
            self.__cancel_all_jobs()

            # Clearing the ouput queue since we won't be iterating anymore
            self.__clear_output_queue()

            # Sanity tests
            if normal_exit:
                assert buffer.is_clean()
                assert ordered_output_buffer.is_clean()
            else:
                buffer.teardown()

            # Releasing iteration state waiters
            state.teardown()

            # Detaching timer callback
            self.throttled_groups.detach()

            # Making sure we are getting rid of the dispatcher thread
            if self.wait:
                if dispatcher.is_alive():
                    dispatcher.join()

            self.imap_lock.release()

        self.cleanup = cleanup

        def output() -> Iterator[ResultType]:
            raised = False

            try:
                while not state.should_stop() and not end_event.is_set():
                    job = self.output_queue.get()

                    if job is THE_END_IS_NIGH:
                        break

                    # Catching keyboard interrupts and other unrecoverable errors
                    if isinstance(job, BaseException):
                        raise job

                    job = cast(Job[ItemType, GroupType, ResultType], job)

                    # Unregistering job in buffer to let other threads continue working
                    buffer.unregister_job(job)

                    # Raising an error that occurred within worker function
                    # NOTE: shenanigans with tracebacks don't seem to change anything
                    if job.exc_info is not None:
                        raise job.exc_info[1].with_traceback(job.exc_info[2])  # type: ignore

                    # Actually yielding the value to main thread
                    if ordered:
                        yield from ordered_output_buffer.output(job)
                    else:
                        yield job.result  # type: ignore

                    # Acknowledging the job was finished
                    # NOTE: this was moved after yielding items so that the
                    # generator body may enqueue new jobs. It is possible that
                    # this has a slight perf hit if the body performs heavy work?
                    state.finish_task()

                    # Cleanup memory and avoid keeping references attached to
                    # ease up garbage collection
                    # NOTE: the ordered output buffer does not store any reference
                    # to jobs but only keep their results. This is therefore
                    # the last remaining reference to a job there.
                    del job

            except:
                raised = True
                raise

            finally:
                # NOTE: we use self.cleanup here to avoid a race condition
                # with sys.excepthook
                if self.cleanup:
                    self.cleanup(not raised)

        dispatcher = Thread(
            name="Thread-quenouille-%i-dispatcher" % id(self),
            target=enqueue,
            daemon=True,
        )
        dispatcher.start()

        return output()

    def imap_unordered(
        self,
        iterable: ImapTarget[ItemType],
        func: Callable[[ItemType], ResultType],
        *,
        key: Optional[Callable[[ItemType], GroupType]] = None,
        parallelism: ParallelismType[GroupType] = 1,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        throttle: ThrottleType[GroupType, ItemType, ResultType] = 0
    ) -> Iterator[ResultType]:
        validate_imap_kwargs(
            iterable=iterable,
            func=func,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle,
        )

        return self.__imap(
            iterable,
            func,
            ordered=False,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle,
        )

    def imap(
        self,
        iterable: ImapTarget[ItemType],
        func: Callable[[ItemType], ResultType],
        *,
        key: Optional[Callable[[ItemType], GroupType]] = None,
        parallelism: ParallelismType[GroupType] = 1,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        throttle: ThrottleType[GroupType, ItemType, ResultType] = 0
    ) -> Iterator[ResultType]:
        validate_imap_kwargs(
            iterable=iterable,
            func=func,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle,
        )

        return self.__imap(
            iterable,
            func,
            ordered=True,
            key=key,
            parallelism=parallelism,
            buffer_size=buffer_size,
            throttle=throttle,
        )


def imap_unordered(
    iterable: ImapTarget[ItemType],
    func: Callable[[ItemType], ResultType],
    threads: Optional[int] = None,
    *,
    key: Optional[Callable[[ItemType], GroupType]] = None,
    parallelism: ParallelismType[GroupType] = 1,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
    throttle: ThrottleType[GroupType, ItemType, ResultType] = 0,
    initializer: Optional[Callable[..., None]] = None,
    initargs: Iterable[Any] = tuple(),
    wait: bool = True,
    daemonic: bool = False,
    excepthook: bool = False
) -> Iterator[ResultType]:
    validate_threadpool_kwargs(
        "threads",
        threads,
        initializer=initializer,
        initargs=initargs,
        wait=wait,
        daemonic=daemonic,
        excepthook=excepthook,
    )
    validate_imap_kwargs(
        iterable=iterable,
        func=func,
        key=key,
        parallelism=parallelism,
        buffer_size=buffer_size,
        throttle=throttle,
    )

    # If we know the size of the iterable, and this size is less than the
    # number of desired threads, we adjust the number of threads accordingly
    if threads is not None and isinstance(iterable, Sized):
        threads = min(len(iterable), threads)

    def generator():
        with ThreadPoolExecutor(
            max_workers=threads,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic,
            excepthook=excepthook,
        ) as executor:
            yield from executor.imap_unordered(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size,
                throttle=throttle,
            )

    return generator()


def imap(
    iterable: ImapTarget[ItemType],
    func: Callable[[ItemType], ResultType],
    threads: Optional[int] = None,
    *,
    key: Optional[Callable[[ItemType], GroupType]] = None,
    parallelism: ParallelismType[GroupType] = 1,
    buffer_size: int = DEFAULT_BUFFER_SIZE,
    throttle: ThrottleType[GroupType, ItemType, ResultType] = 0,
    initializer: Optional[Callable[..., None]] = None,
    initargs: Iterable[Any] = tuple(),
    wait: bool = True,
    daemonic: bool = False,
    excepthook: bool = False
) -> Iterator[ResultType]:
    validate_threadpool_kwargs(
        "threads",
        threads,
        initializer=initializer,
        initargs=initargs,
        wait=wait,
        daemonic=daemonic,
        excepthook=excepthook,
    )
    validate_imap_kwargs(
        iterable=iterable,
        func=func,
        key=key,
        parallelism=parallelism,
        buffer_size=buffer_size,
        throttle=throttle,
    )

    # If we know the size of the iterable, and this size is less than the
    # number of desired threads, we adjust the number of threads accordingly
    if threads is not None and isinstance(iterable, Sized):
        threads = min(len(iterable), threads)

    def generator():
        with ThreadPoolExecutor(
            max_workers=threads,
            initializer=initializer,
            initargs=initargs,
            wait=wait,
            daemonic=daemonic,
            excepthook=excepthook,
        ) as executor:
            yield from executor.imap(
                iterable,
                func,
                key=key,
                parallelism=parallelism,
                buffer_size=buffer_size,
                throttle=throttle,
            )

    return generator()


def generate_function_doc(ordered: bool = False) -> str:
    disclaimer = "Note that results will be yielded in an arbitrary order."

    if ordered:
        disclaimer = "Note that results will be yielded in same order as the input."

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
