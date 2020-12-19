# =============================================================================
# Quenouille Constants
# =============================================================================
#
# Handy constants.
#
import threading

# Useful to avoid known issues with queue blocking
FOREVER = threading.TIMEOUT_MAX

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
