# =============================================================================
# Quenouille Custom Exceptions
# =============================================================================
#


class QuenouilleException(Exception):
    pass


# NOTE: this exception exists in concurrent.futures.thread:
# https://docs.python.org/3/library/concurrent.futures.html#exception-classes
# But I need to support python 3.6
class BrokenThreadPool(QuenouilleException):
    pass
