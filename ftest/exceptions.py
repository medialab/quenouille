# =============================================================================
# Quenouille Exception Testing
# =============================================================================
#
# Testing what happens when exceptions are thrown.
#
from quenouille import imap_unordered


def crasher(i):
    if i > 7:
        raise Exception("Die!")
    return i


for result in imap_unordered(range(15), crasher, 3):
    print(result)
