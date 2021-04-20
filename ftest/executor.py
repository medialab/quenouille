import time
from concurrent.futures import ThreadPoolExecutor


def worker(seconds=2):
    time.sleep(seconds)


with ThreadPoolExecutor(max_workers=2) as executor:
    for i in range(10):
        print(i)
        f = executor.submit(worker)
        print(f)
