import time
from quenouille import ThreadPoolExecutor

key1 = lambda x: 'SAME'
key2 = lambda x: 'SAME'


def work(i):
    time.sleep(0.01)
    return i


with ThreadPoolExecutor(max_workers=4) as executor:
    for i in executor.imap(range(2), work, key=key1, throttle=5):
        print('done', i)

    print('Finished first batch', executor.throttled_groups.groups)

    for i in executor.imap(range(2), work, key=key1, throttle=5):
        print('done', i)

    print('Finished second batch', executor.throttled_groups.groups)

    for i in executor.imap(range(2), work, key=key2, throttle=5):
        print('done', i)

    print('Finished third batch', executor.throttled_groups.groups)
