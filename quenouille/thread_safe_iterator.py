# =============================================================================
# Quenouille Thread Safe Iterator
# =============================================================================
#
# Just an helper python class to wrap any iterable and make it thread-safe.
#
from threading import Lock


class ThreadSafeIterator(object):
    """
    The ThreadSafeIterator class. Wraps the given iterator to make it
    thread-safe.

    Args:
        iterable (iterable): target iterable to wrap

    """

    def __init__(self, iterable):
        self.__iterator = iter(iterable)
        self.lock = Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.__iterator)
