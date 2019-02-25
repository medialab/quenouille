# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
import sys
from queue import Queue
from collections import defaultdict, deque, Counter
from threading import Condition, Event, Lock, Thread, Timer
from quenouille.thread_safe_iterator import ThreadSafeIterator

# Handy constants
# -----------------------------------------------------------------------------

# Basically a year. Useful to avoid known issues with queue blocking
FOREVER = 365 * 24 * 60 * 60

# A small async sleep value
SOON = 0.0001

# A sentinel value for the output queue to know when to stop
THE_END_IS_NIGH = object()

# A sentinel value to propagate exceptions
EVERYTHING_MUST_BURN = object()

# A sentinel value for throttling purposes
THE_WAIT_IS_OVER = object()

# The infinity, and beyond
INFINITY = float('inf')


# The implementation
# -----------------------------------------------------------------------------
def generic_imap(iterable, func, threads, ordered=False, group_parallelism=INFINITY,
                 group=None, group_buffer_size=1, group_throttle=0,
                 listener=None):
    """
    Function consuming tasks from any iterable, dispatching them to a pool
    of threads and finally yielding the produced results.

    Args:
        iterable (iterable): iterable of jobs.
        func (callable): The task to perform with each job.
        threads (int): The number of threads to use.
        ordered (bool, optional): Whether to yield results in order or not.
            Defaults to `False`.
        group_parallelism (int, optional): Number of threads allowed to work
            on the same group at once. Defaults to no limit.
        group (callable, optional): Function returning a job's group.
            This argument is required if you need to restrict group parallelism.
        group_buffer_size (int, optional): Max number of jobs that the function
            will buffer into memory when waiting for a thread to be available.
            Defaults to 1.
        group_throttle (float, optional): Optional throttle time to wait
            between each task per group.
        listener (callable, optional): Function that will be called when
            some events occur to be able to track progress.

    Yields:
        any: will yield results based on the given job.

    """

    throttling = group_throttle != 0
    handling_group_parallelism = group_parallelism != INFINITY or throttling

    # Checking arguments
    if not isinstance(threads, (int, float)) or threads < 1:
        raise TypeError('quenouille/imap: `threads` should be a positive number.')

    if not callable(func):
        raise TypeError('quenouille/imap: `func` should be callable.')

    if not isinstance(group_buffer_size, (int, float)) or group_buffer_size < 1:
        raise TypeError('quenouille/imap: `group_buffer_size` should be a positive number.')

    if group_throttle != 0 and (not isinstance(group_throttle, (int, float)) or group_throttle < 0):
        raise TypeError('quenouille/imap: `group_throttle` should be >= 0.')

    if listener is not None and not callable(listener):
        raise TypeError('quenouille/imap: `listener` should be callable if provided.')

    if handling_group_parallelism and not callable(group):
        raise TypeError('quenouille/imap: `group` is not callable and is required with `group_parallelism` or `group_throttle`')
    else:
        get_group = lambda x: group(x[1])

    # Making our iterable a thread-safe iterator
    safe_iterator = ThreadSafeIterator(enumerate(iterable))

    # One queue for jobs to do & one queue to output their results
    input_queue = Queue(maxsize=threads)
    output_queue = Queue()

    # A counter on finished threads to be able to know when to end output queue
    finished_counter = 0

    # A last index counter to be able to ouput results in order if needed
    last_index = -1
    last_index_condition = Condition()

    # State
    enqueue_lock = Lock()
    listener_lock = Lock()
    yield_lock = Lock()
    timer_condition = Condition()
    worked_groups = Counter()
    buffers = defaultdict(lambda: Queue(maxsize=group_buffer_size))
    waiters = defaultdict(deque)
    timers = {}

    # Closures
    def emit(event, job):
        if listener is not None:
            with listener_lock:
                listener(event, job)

    def enqueue(last_job=None):
        """
        Function consuming the iterable to pipe next job into the input queue
        for the workers.

        Args:
            last_job (any): Last performed job. Useful to track limits etc.

        """
        nonlocal finished_counter

        # Acquiring a lock so no other thread may enter this part
        enqueue_lock.acquire()

        job = None
        last_group = None
        current_group = None

        if last_job is not None and handling_group_parallelism:
            last_group = get_group(last_job)

            if worked_groups[last_group] == 1:
                del worked_groups[last_group]
            else:
                worked_groups[last_group] -= 1

        # Checking the buffers
        if handling_group_parallelism:
            current_group, buffer = next(
                ((k, b) for k, b in buffers.items() if worked_groups[k] < group_parallelism),
                (None, None)
            )

            if buffer is not None:
                if current_group in waiters:
                    w = waiters[current_group]
                    w.popleft().set()

                    if len(w) == 0:
                        del waiters[current_group]

                    job = buffer.get(timeout=FOREVER)

                elif not buffer.empty():
                    job = buffer.get_nowait()

                    if buffer.empty():
                        del buffers[current_group]

        # Not suitable buffer found, let's consume the iterable!
        while job is None:
            job = next(safe_iterator, None)

            if job is None:
                break

            # Do we need to increment counters?
            if handling_group_parallelism:
                current_group = get_group(job)

                # Is current group full?
                if worked_groups[current_group] >= group_parallelism:
                    buffer = buffers[current_group]

                    if not buffer.full():
                        buffer.put_nowait(job)

                        job = None
                        continue

                    waiter = Event()
                    waiters[current_group].append(waiter)

                    enqueue_lock.release()

                    waiter.wait()
                    buffer.put(job, timeout=FOREVER)

                    return enqueue()
                break
            else:
                break

        # If we don't have any job left, we count towards the end
        if job is None:

            # Releasing the lock
            enqueue_lock.release()

            finished_counter += 1

            # All threads ended? Let's signal the output queue
            if finished_counter == threads:
                output_queue.put(THE_END_IS_NIGH, timeout=FOREVER)

        # We do have another job to do, let's signal the input queue
        else:
            if handling_group_parallelism:
                assert current_group is not None
                worked_groups[current_group] += 1

            # Releasing the lock
            enqueue_lock.release()

            input_queue.put((current_group, job), timeout=FOREVER)

    def release_throttled(g):
        with timer_condition:
            timers[g] = THE_WAIT_IS_OVER
            timer_condition.notify_all()

    def worker():
        """
        Function consuming the input queue, performing the task with the
        consumed job and piping the result into the output queue.
        """
        nonlocal last_index

        while True:
            g, job = input_queue.get(timeout=FOREVER)

            if job is None:
                break

            index, data = job

            # Need to throttle?
            already_throttled = False

            if throttling:

                # NOTE: we could also use deque of waiters to be more efficient?
                with timer_condition:
                    while g in timers and timers[g] is not THE_WAIT_IS_OVER:
                        timer_condition.wait()

                    already_throttled = True
                    timers[g] = True

            # Recording time
            # NOTE: we record before & after to prevent multiple threads
            # to work at once on the same group
            if throttling and not already_throttled:
                with timer_condition:
                    timers[g] = True

            # Emitting
            emit('start', data)

            # Recording time and releasing throttled threads
            if throttling:
                with timer_condition:

                    # NOTE: we could improve the precision of the timer if needed
                    timer = Timer(group_throttle, release_throttled, args=(g, ))
                    timers[g] = timer

                timer.start()

            # Performing actual work
            try:
                result = func(data)
            except BaseException:
                return output_queue.put_nowait(
                    (EVERYTHING_MUST_BURN, sys.exc_info())
                )

            if ordered:
                with last_index_condition:

                    while last_index != index - 1:
                        last_index_condition.wait()

                    last_index = index
                    last_index_condition.notify_all()

            # Piping into output queue
            output_queue.put(result, timeout=FOREVER)
            input_queue.task_done()

            # Enqueuing next
            enqueue(job)

    def boot():
        """
        Function used to boot the workflow. It is called asynchronously to
        avoid blocking issues preventing from returning the iterator first.
        """
        for _ in range(threads):
            enqueue()

    def output():
        """
        Output generator function returned to provide an iterator to the
        user.
        """

        while True:
            result = output_queue.get(timeout=FOREVER)

            # An exception was thrown!
            if isinstance(result, tuple) and result[0] is EVERYTHING_MUST_BURN:

                # Cleanup
                if throttling:
                    for timer in timers.items():
                        if isinstance(timer, Timer):
                            timer.cancel()

                _, (_, e, trace) = result
                raise e.with_traceback(trace)

            # The end is night!
            if result is THE_END_IS_NIGH:
                break

            # NOTE: not completely sure this lock is needed.
            # Better safe than sorry...
            with yield_lock:
                yield result

    # Starting the threads
    pool = [Thread(target=worker, daemon=True) for _ in range(threads)]

    for thread in pool:
        thread.start()

    next_tick = Timer(SOON, boot)
    next_tick.start()

    return output()


# Exporting specialized variants
# -----------------------------------------------------------------------------

# NOTE: not using `functool.partial/.update_wrapper` because if does not work
# with the built-in `help` function so well. I am also not using `*args` and
# `**kwargs` to make it easy on tooling...
def imap(iterable, func, threads, ordered=True, group_parallelism=INFINITY,
         group=None, group_buffer_size=1, group_throttle=0, listener=None):

    return generic_imap(
        iterable, func, threads, ordered=ordered,
        group_parallelism=group_parallelism, group=group,
        group_buffer_size=group_buffer_size, group_throttle=group_throttle,
        listener=listener)


def imap_unordered(iterable, func, threads, ordered=False, group_parallelism=INFINITY,
                   group=None, group_buffer_size=1, group_throttle=0, listener=None):

    return generic_imap(
        iterable, func, threads, ordered=ordered,
        group_parallelism=group_parallelism, group=group,
        group_buffer_size=group_buffer_size, group_throttle=group_throttle,
        listener=listener)


imap.__doc__ = generic_imap.__doc__
imap_unordered.__doc__ = generic_imap.__doc__

__all__ = ['imap', 'imap_unordered']
