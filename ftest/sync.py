from quenouille import ThreadPoolExecutor


def worker(n):
    return n


def group(item):
    return item % 2 == 0


def throttle(group, item, result):
    return 2.0


with ThreadPoolExecutor(0) as executor:
    for n in executor.imap_unordered(range(5), worker, throttle=throttle):
        print(n)
