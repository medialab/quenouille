import time
from queue import Queue
from quenouille import imap_unordered, QueueIterator

queue = Queue()

queue.put(2)
queue.put(1)
queue.put(3)


def worker(payload):
    time.sleep(payload[1])

    return payload


iterator = QueueIterator(queue)

for i, t in imap_unordered(enumerate(iterator), worker, 1):
    with iterator:
        print("Done waiting %i" % t)

        if i < 1:
            queue.put(4)
