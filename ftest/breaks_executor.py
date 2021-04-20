import threading
import time
from quenouille import ThreadPoolExecutor

def print_active_threads(i=-1):
    print('Pass %s - (%i active threads)' % (i + 1, threading.active_count()))

def worker(item):
    return item

with ThreadPoolExecutor(max_workers=100) as executor:
    for i in range(1_000):
        print_active_threads(i)
        for j in executor.imap_unordered(range(1_000), worker):
            if j > 500:
                break

print_active_threads()
time.sleep(2)
print_active_threads()
