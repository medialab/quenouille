import time
from queue import Queue
from quenouille import imap_unordered, iter_queue

queue = Queue()

queue.put(2)
queue.put(1)
queue.put(3)

def consume(iterator):
    for _ in iterator:
        pass

def worker(payload):
    time.sleep(payload)
    print('Done waiting %i' % payload)

consume(imap_unordered(iter_queue(queue), worker, 1))
