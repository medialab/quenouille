# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
import math
from queue import Queue
from collections import defaultdict, Counter
from threading import Condition, Lock, Thread, Timer
from quenouille.thread_safe_iterator import ThreadSafeIterator

# TODO: can it exit/break safely?
# TODO: handle output buffer to have more latitude on ordered performance

# Handy constants
# -----------------------------------------------------------------------------

# Basically a year. Useful to avoid known issues with queue blocking
FOREVER = 365 * 24 * 60 * 60

# A small async sleep value
SOON = 0.0001

# A sentinel value for the output queue to know when to stop
THE_END_IS_NIGH = object()

# The infinity, and beyond
INFINITY = math.inf


# The implementation
# -----------------------------------------------------------------------------
def imap(iterable, func, threads, ordered=False, group_parallelism=INFINITY,
         group=None, group_buffer_size=1):
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

    Yields:
        any: will yield results based on the given job.

    """

    handling_group_parallelism = group_parallelism != INFINITY

    # Checking arguments
    if handling_group_parallelism and not callable(group):
        raise TypeError('quenouille/imap: `group` is not callable and is required with `group_parallelism`')

    # Making our iterable a thread-safe iterator
    safe_iterator = ThreadSafeIterator(enumerate(iterable))

    # One queue for jobs to do & one queue to output their results
    input_queue = Queue(maxsize=threads)
    output_queue = Queue(maxsize=threads)

    # A lock on finished threads to be able to know when to end output queue
    finished_lock = Lock()
    finished_counter = 0

    # A last index counter to be able to ouput results in order if needed
    last_index = -1
    last_index_condition = Condition()

    # State
    grouping_lock = Lock()
    worked_groups = Counter()
    buffers = defaultdict(list)
    waiters = {}

    # Closures
    def enqueue(last_job=None):
        """
        Function consuming the iterable to pipe next job into the input queue
        for the workers.

        Args:
            last_job (any): Last performed job. Useful to track limits etc.

        """
        nonlocal finished_counter

        job = None
        should_wait = False
        waiter_to_release = None

        # Consuming the iterable to find suitable job
        while True:

            # Can we use the buffer
            # with grouping_lock:

            # Let's consume the iterable
            job = next(safe_iterator, None)
            break

        # If we don't have any job left, we count towards the end
        if job is None:
            with finished_lock:
                finished_counter += 1

                # All threads ended? Let's signal the output queue
                if finished_counter == threads:
                    output_queue.put(THE_END_IS_NIGH, timeout=FOREVER)

        # We do have another job to do, let's signal the input queue
        else:
            input_queue.put(job, timeout=FOREVER)

    def worker():
        """
        Function consuming the input queue, performing the task with the
        consumed job and piping the result into the output queue.
        """
        nonlocal last_index

        while True:
            job = input_queue.get(timeout=FOREVER)

            if job is None:
                break

            index, data = job

            result = func(data)

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

            # The end is night!
            if result is THE_END_IS_NIGH:
                break

            yield result

    # Starting the threads
    pool = [Thread(target=worker, daemon=True) for _ in range(threads)]

    for thread in pool:
        thread.start()

    t = Timer(SOON, boot)
    t.start()

    return output()
