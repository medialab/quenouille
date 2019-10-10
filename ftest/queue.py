import time
from queue import Queue
from quenouille import imap_unordered

queue = Queue()

queue.put(2)
queue.put(1)
queue.put(3)

def consume(iterator):
    for _ in iterator:
        pass

def queue_to_iteraror(q):
    while not q.empty():
        yield q.get()

def worker(payload):
    time.sleep(payload)
    print('Done waiting %i' % payload)

consume(imap_unordered(queue_to_iteraror(queue), worker, 1))
