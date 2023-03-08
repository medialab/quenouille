from quenouille import imap_unordered

def worker(item):
    return item

it = imap_unordered(range(1_000), worker, 100)

for j in it:
    if j > 500:
        raise RuntimeError
