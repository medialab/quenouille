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
  * [The None group](#the-none-group)
  * [None parallelism](#none-parallelism)
  * [Parallelism > workers](#parallelism--workers)
  * [Callable parallelism guarantees](#callable-parallelism-guarantees)
  * [Parallelism vs. throttling](#parallelism-vs-throttling)
  * [Adding entropy to throttle](#adding-entropy-to-throttle)
  * [Caveats of using imap with queues](#caveats-of-using-imap-with-queues)

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

  # Throttle time depending on domain
  def throttle(group, item, result):
    if group == 'lemonde.fr':
      return 10

    return 2

  for html in imap(urls, fetch, 10, key=domain_name, throttle=throttle):
    print(html)

  # Only load 10 urls into memory when attempting to find next suitable job
  for html in imap(urls, fetch, 10, key=domain_name, throttle=5, buffer_size=10):
    print(html)
```

*Arguments*

* **iterable** *iterable*: Any python iterable.
* **func** *callable*: Function used to perform the desired tasks. The function takes an item yielded by the given iterable as single argument. Note that since this function will be dispatched in worker threads, so you should ensure it is thread-safe.
* **threads** *?int*: Maximum number of threads to use. Defaults to `min(32, os.cpu_count() + 1)`
* **key** *?callable*: Function returning to which "group" a given item is supposed to belong. This will be used to ensure maximum parallelism is respected.
* **parallelism** *?int|callable* [`1`]: Number of threads allowed to work on a same group at once. Can also be a function taking a group and returning its parallelism.
* **buffer_size** *?int* [`1024`]: Maximum number of items the function will buffer into memory while attempting to find an item that can be passed to a worker immediately, all while respecting throttling and group parallelism.
* **throttle** *?int|float|callable*: Optional throttle time, in seconds, to wait before processing the next item of a given group. Can also be a function taking last group, item and result and returning next throttle time for this group.

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

Note that `imap` will only run until the queue is fully drained. So if you decide to add more items afterwards, this is not the function's concern anymore.

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

#### The None group

The `imap` functions consider the `None` group (this can happen if your `key` function returns `None`) as special and will always consider the attached items can be processed right away without parallelism constraints nor throttle.

#### None parallelism

`parallelism` can be callable and return `None` for some groups, meaning those won't be affected by parallelism constraints, e.g. any numbers of workers will be able to work on this group at once.

#### Parallelism > workers

If you set `parallelism` to `10` and only allocate `5` threads to perform your tasks, it should be obvious that the actual parallelism will never reach `10`.

`quenouille` won't warn you of this because it might be convenient not to give this too much thought and has not much consequence, but keep this in mind.

#### Callable parallelism guarantees

If you decide to pass a function to declare a custom parallelism for each group rather than a global fixed one, know that the function will be called each time `quenouille` has to make a decision regarding the next items to enqueue so that we don't use any memory to record the information.

This means that you should guarantee that the given function is idempotent and will always return the same parallelism for a given group if you don't want to risk a deadlock.

#### Parallelism vs. throttling

If a group is being trottled, it should be obvious that `quenouille` won't perform more than one single task for this group at once, so its `parallelism` is in fact `1`, in spite of other settings.

This does not mean that `parallelism` for some groups and `throttle` for others is impossible. This can be achieved through callables.

#### Adding entropy to throttle

You can understand the callable `throttle` kwarg as "what's the minimum time the next job from this group should wait before firing up". This means that if you need to add entropy to the throttle time, you can indeed make this function work with randomness like so:

```python
from random import random

# Waiting 5 + (between 0 and 2) seconds
def throttle(group, item):
  return 5 + (2 * random())
```

#### Caveats of using imap with queues

*Typical deadlocks*

Even if `imap` can process an input queue, one should note that you should avoid to find yourself in a situation where adding to the queue might block execution if you don't want to end in a deadlock. It can be easy to footgun yourself if your queue has a `maxsize`, for instance:

```python
from queue import Queue
from quenouille import imap

job_queue = Queue(maxsize=2)
job_queue.put(1)

for i in imap(job_queue, worker):
  if i < 2:
    job_queue.put(2)
    job_queue.put(3)

    # This will put you in a deadlock because this will block
    # because of the queue `maxsize` set to 2
    job_queue.put(4)

  print(i)
```

*Design choices*

To enable you to add items to the queue in your loop body and so it can safely detect when your queue is drained without race condition, `quenouille` acknowledges that a task is finished only after what you execute in the loop body is done.

This means that sometimes it might be more performant to only add items to the queue from the worker functions rather than from the loop body.

*queue.task_done*

For now, `quenouille` does not call `queue.task_done` for you, so this remains your responsability, if you want to be able to call `queue.join` down the lane.
