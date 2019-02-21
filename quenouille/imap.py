# =============================================================================
# Quenouille Map Function
# =============================================================================
#
# Python implementation of a complex, lazy multithreaded iterable consumer.
#
from queue import Queue
from threading import Lock, Thread, Timer
from quenouille.thread_safe_iterator import ThreadSafeIterator

# TODO: test cases when the number of workers is over the size of iterable
# TODO: handle clean exit

# Handy constants
# -----------------------------------------------------------------------------

# Basically a year. Useful to avoid known issues with queue blocking
FOREVER = 365 * 24 * 60 * 60

# A small async sleep value
SOON = 0.0001

# A sentinel value for the output queue to know when to stop
THE_END_IS_NIGH = object()


# The implementation
# -----------------------------------------------------------------------------
def imap(iterable, func, threads):
    """
    Function consuming tasks from any iterable, dispatching them to a pool
    of threads and finally yielding the produced results.

    Args:
        iterable (iterable): iterable of jobs.
        func (callable): The task to perform with each job.
        threads (int): The number of threads to use.

    Yields:
        any: will yield results based on the given job.

    """

    # Useful members
    safe_iterator = ThreadSafeIterator(iterable)

    input_queue = Queue(maxsize=threads)
    output_queue = Queue(maxsize=threads)

    finished_lock = Lock()
    finished_counter = 0

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

        # Consuming the iterable to find suitable job
        while True:

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

        while True:
            job = input_queue.get(timeout=FOREVER)

            if job is None:
                break

            result = func(job)

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
