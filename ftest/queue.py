import time
from queue import Queue
from quenouille import imap_unordered, iter_queue

queue = Queue()

queue.put(2)
queue.put(1)
queue.put(3)

def worker(payload):
    time.sleep(payload[1])

    return payload

for i, t in imap_unordered(enumerate(iter_queue(queue)), worker, 1):
    print('Done waiting %i' % t)

    if i < 1:
        queue.put(4)
