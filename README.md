[![Build Status](https://github.com/medialab/quenouille/workflows/Tests/badge.svg)](https://github.com/medialab/quenouille/actions)

# Quenouille

A library of multithreaded iterator workflows for python.

It is typically used to iterate over lazy streams without overflowing memory, all while respecting group parallelism constraints and throttling, e.g. when downloading massive amounts of urls from the web concurrently.

It is mainly used by the [minet](https://github.com/medialab/minet) python library and CLI tool to power its downloaders and crawlers.

## Installation

You can install `quenouille` with pip using the following command:

```
pip install quenouille
```

## Usage

* [imap_unordered, imap](#imap_unordered-imap)
* [ThreadPoolExecutor](#threadpoolexecutor)
* [Miscellaneous notes](#miscellaneous-notes)

### imap_unordered, imap

Function lazily consuming an iterable and applying the desired function over the yielded items in a multithreaded fashion.

This function is also able to respect group constraints regarding parallelism and throttling: for instance, if you need to download urls in a multithreaded fashion and need to ensure that you won't hit the same domain more than twice at the same time, this function can be given proper settings to ensure the desired behavior.

The same can be said if you need to make sure to wait for a certain amount of time between two hits on the same domain by using this function's throttling.

Finally note that this function comes in two flavors: `imap_unordered`, which will output processed items as soon as they are done, and `imap`, which will output items in the same order as the input, at the price of slightly worse performance and an increased memory footprint that depends on the number of items that have been processed before the next item can be yielded.

```python
import csv
from quenouille import imap_unordered

# Example fetching urls from a CSV file
with open(csv_path, 'r') as f:
  reader = csv.DictReader(f)

  urls = (line['url'] for line in reader)

  # Performing 10 requests at a time:
  for html in imap(urls, fetch, 10):
    print(html)

  # Ensuring we don't hit the same domain more that twice at a time
  for html in imap(urls, fetch, 10, key=domain_name, parallelism=2):
    print(html)

  # Waiting 5 seconds between each request on a same domain
  for html in imap(urls, fetch, 10, key=domain_name, throttle=5):
    print(html)

  # Only load 10 urls into memory when attempting to find next suitable job
  for html in imap(urls, fetch, 10, key=domain_name, throttle=5, buffer_size=10):
    print(html)
```

*Arguments*

* **iterable** *iterable*: Any python iterable.
* **func** *callable*: Function used to perform the desired tasks. The function takes any item yielded from the given iterable as sole argument. Note that since this function will be dispatched in worker threads, you should ensure it is thread-safe.
* **threads** *?int*: Maximum number of threads to use. Defaults to `min(32, os.cpu_count() + 1)`
* **key** *?callable*: Function returning to which "group" a given item is supposed to belong. This will be used to ensure maximum parallelism is respected.
* **parallelism** *?int|callable* [`1`]: Number of threads allowed to work on a same group at once. Can also be a function taking a group and returning its parallelism.
* **buffer_size** *?int* [`1024`]: Maximum number of items the function will buffer into memory while attempting to find and item that can be passed to a worker immediately, while respecting throttling and group parallelism.
* **throttle** *?int|float|callable*: Optional throttle time, in seconds, to wait before processing the next item of a given group. Can also be a function taking the current item, with its group and returning the next throttle time.

*Using a queue rather than an iterable*

If you need to add new items to process as a result of performing tasks (when designing a web crawler for instance, where each downloaded page will yield new pages to explore further down), know that the `imap` and `imap_unordered` function also accepts queues as input:

```python
from queue import Queue
from quenouille import imap

job_queue = Queue()
job_queue.put(1)

# Enqueuing new jobs in the worker
def worker(i):
  if i < 3:
    job_queue.put(i + 1)

  return i * 2

list(imap(job_queue, worker, 2))
>>> [2, 4, 6]

# Enqueuing new jobs while iterating over results
job_queue = Queue()
job_queue.put(1)

results = []

for i in imap(job_queue, worker, 2):
  if i < 5:
    job_queue.put((i / 2) + 1)

  results.append(i)

results
>>> [2, 4, 6]
```

Note that the function will only run until the queue is fully drained. So if you decide to add more items afterwards, this is not the function's concern anymore.

### ThreadPoolExecutor

If you need to run `imap` or `imap_unordered` multiple times in succession while keeping the same thread pool up and running, you can also directly use quenouille's `ThreadPoolExecutor` like so:

```python
from quenouille import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=4) as executor:
  for i in executor.imap(range(10), worker):
    print(i)

  for j in executor.imap_unordered(range(10), worker):
    print(j)
```

### Miscellaneous notes

TODO: examples and caveats, link to minet

https://yomguithereal.github.io/posts/contiguous-range-set/
