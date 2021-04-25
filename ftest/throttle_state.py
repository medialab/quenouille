import time
from quenouille import ThreadPoolExecutor

key = lambda x: 'SAME'


def work(i):
    time.sleep(0.01)
    return i


with ThreadPoolExecutor(max_workers=4) as executor:
    for i in executor.imap(range(2), work, key=key, throttle=5):
        print('done', i)

    print('Finished first batch', executor.throttled_groups.groups)

    for i in executor.imap(range(2), work, key=key, throttle=5):
        print('done', i)
