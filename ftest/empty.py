import time
import threading
from quenouille import imap_unordered

def sleeper(job):
    time.sleep(job)
    return job

for i in imap_unordered(iter([]), sleeper, 5):
    print('THIS SHOULD NOT PRINT!')

print('Finished with %i threads.' % threading.active_count())
time.sleep(3)
print('Afterwards %i threads.' % threading.active_count())
