# =============================================================================
# Quenouille Stack Overflow Testing
# =============================================================================
#
# Reproducing issues related to recursion & stack overflow.
#
from quenouille import imap_unordered

DATA = range(3000)


def worker(i):
    return i


for i in imap_unordered(
    DATA, worker, 25, group=lambda x: 1, group_parallelism=1, group_throttle=0.1
):
    print(i)
