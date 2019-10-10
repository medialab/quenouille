# =============================================================================
# Quenouille Constants
# =============================================================================
#
# Handy constants.
#

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
