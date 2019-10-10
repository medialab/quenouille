# =============================================================================
# Quenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
from quenouille.constants import FOREVER


def iter_queue(queue, timeout=FOREVER):
    while not queue.empty():
        yield queue.get(timeout=timeout)
        queue.task_done()
