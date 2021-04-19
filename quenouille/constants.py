# =============================================================================
# Quenouille Constants
# =============================================================================
#
# Handy constants.
#
import threading

# Useful to avoid known issues with queue blocking
FOREVER = threading.TIMEOUT_MAX

# A sentinel value for the output queue to know when to stop
THE_END_IS_NIGH = object()

# Default buffer size for imap iterators
DEFAULT_BUFFER_SIZE = 1024
