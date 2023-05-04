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

* [imap, imap_unordered](#imap_unordered-imap)
* [ThreadPoolExecutor](#threadpoolexecutor)
  * [#.imap, #.imap_unordered](#executor-imap)
  * [#.shutdown](#shutdown)
* [NamedLocks](#namedlocks)
* [Miscellaneous notes](#miscellaneous-notes)
  * [The None group](#the-none-group)
  * [Parallelism > workers](#parallelism--workers)
  * [Callable parallelism guarantees](#callable-parallelism-guarantees)
  * [Parallelism vs. throttling](#parallelism-vs-throttling)
  * [Adding entropy to throttle](#adding-entropy-to-throttle)
  * [Caveats regarding exception raising](#caveats-regarding-exception-raising)
  * [Caveats of using imap with queues](#caveats-of-using-imap-with-queues)

### imap, imap_unordered

Function lazily consuming an iterable and applying the desired function over the yielded items in a multithreaded fashion.

This function is also able to respect group constraints regarding parallelism and throttling: for instance, if you need to download urls in a multithreaded fashion and need to ensure that you won't hit the same domain more than twice at the same time, this function can be given proper settings to ensure the desired behavior.

The same can be said if you need to make sure to wait for a certain amount of time between two hits on the same domain by using this function's throttling.

Finally note that this function comes in two flavors: `imap_unordered`, which will output processed items as soon as they are done, and `imap`, which will output items in the same order as the input, at the price of slightly worse performance and an increased memory footprint that depends on the number of items that have been processed before the next item can be yielded.

```python
import csv
from quenouille import imap, imap_unordered

# Reading urls lazily from a CSV file using a generator:
def urls():
  with open('urls.csv') as f:
    reader = csv.DictReader(f)

    for line in reader:
      yield line['url']

# Defining some functions
def fetch(url):
  # ... e.g. use urllib3 to download the url
  # remember this function must be threadsafe
  return html

def get_domain_name(url):
  # ... e.g. use ural to extract domain name from url
  return domain_name

# Performing 10 requests at a time:
for html in imap(urls(), fetch, 10):
  print(html)

# Ouputting results as soon as possible (in arbitrary order)
for html in imap_unordered(urls(), fetch, 10):
  print(html)

# Ensuring we don't hit the same domain more that twice at a time
for html in imap(urls(), fetch, 10, key=get_domain_name, parallelism=2):
  print(html)

# Waiting 5 seconds between each request on a same domain
for html in imap(urls(), fetch, 10, key=get_domain_name, throttle=5):
  print(html)

# Throttle time depending on domain
def throttle(group, item, result):
  if group == 'lemonde.fr':
    return 10

  return 2

for html in imap(urls(), fetch, 10, key=get_domain_name, throttle=throttle):
  print(html)

# Only load 10 urls into memory when attempting to find next suitable job
for html in imap(urls(), fetch, 10, key=get_domain_name, throttle=5, buffer_size=10):
  print(html)
```

*Arguments*

* **iterable** *(iterable)*: Any python iterable.
* **func** *(callable)*: Function used to perform the desired tasks. The function takes an item yielded by the given iterable as single argument. Note that since this function will be dispatched in worker threads, so you should ensure it is thread-safe.
* **threads** *(int, optional)*: Maximum number of threads to use. Defaults to `min(32, os.cpu_count() + 1)`. Note that it can be `0`, in which case no threads will be used and everything will run synchronously (this can be useful for debugging or to avoid duplicating code sometimes).
* **key** *(callable, optional)*: Function returning to which "group" a given item is supposed to belong. This will be used to ensure maximum parallelism is respected.
* **parallelism** *(int or callable, optional)* [`1`]: Number of threads allowed to work on a same group at once. Can also be a function taking a group and returning its parallelism.
* **buffer_size** *(int, optional)* [`1024`]: Maximum number of items the function will buffer into memory while attempting to find an item that can be passed to a worker immediately, all while respecting throttling and group parallelism.
* **throttle** *(int or float or callable, optional)*: Optional throttle time, in seconds, to wait before processing the next item of a given group. Can also be a function taking last group, item and result and returning next throttle time for this group.
* **initializer** *(callable, optional)*: Function to run at the start of each thread worker. Can be useful to setup [thread-local data](https://docs.python.org/3/library/threading.html#thread-local-data), for instance. Remember this function must be threadsafe and should not block because the thread pool will wait for each thread to be correctly booted before being able to proceed. If one of the function calls fails, the thread pool will raise a `quenouille.exceptions.BrokenThreadPool` error and terminate immediately.
* **initargs** *(iterable, optional)*: Arguments to pass to the `initializer` function.
* **wait** *(bool, optional)* [`True`]: Whether to join worker threads, i.e. wait for them to end, when shutting down the executor. Set this to `False` if you need to go on quickly without waiting for your worker threads to end when cleaning up the executor's resources. Just note that if you spawn other thread-intensive tasks or other executors afterwards in rapid succession, you might start too many threads at once.
* **daemonic** *(bool, optional)* [`False`]: whether to spawn daemonic worker. If your worker are daemonic, the interpreter will not wait for them to end when exiting. This can be useful, combined to `wait=False`, for instance, if you want your program to exit as soon as hitting ctrl+C (you might want to avoid this if your threads need to cleanup things on exit as they will be abruptly shut down).

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

# Using it as a context manager:
with ThreadPoolExecutor(max_workers=4) as executor:
  for i in executor.imap(range(10), worker):
    print(i)

  for j in executor.imap_unordered(range(10), worker):
    print(j)

# Or if you prefer shutting down the executor explicitly:
executor = ThreadPoolExecutor()
executor.imap(range(10), worker)
executor.shutdown(wait=False)
```

Note that your throttling state is kept between multiple `imap` and `imap_unordered` calls so you don't end up perform some tasks too soon. But keep in mind this state is tied to the `key` function you provide to remain consistent, so if you change the used `key`, the throttling state will be reset.

*Arguments*

* **max_workers** *(int, optional)*: Maximum number of threads to use. Defaults to `min(32, os.cpu_count() + 1)`. Note that it can be `0`, in which case no threads will be used and everything will run synchronously (this can be useful for debugging or to avoid duplicating code sometimes).
* **initializer** *(callable, optional)*: Function to run at the start of each thread worker. Can be useful to setup [thread-local data](https://docs.python.org/3/library/threading.html#thread-local-data), for instance. Remember this function must be threadsafe and should not block because the thread pool will wait for each thread to be correctly booted before being able to proceed. If one of the function calls fails, the thread pool will raise a `quenouille.exceptions.BrokenThreadPool` error and terminate immediately.
* **initargs** *(iterable, optional)*: Arguments to pass to the `initializer` function.
* **wait** *(bool, optional)* [`True`]: Whether to join worker threads, i.e. wait for them to end, when closing the executor. Set this to `False` if you need to go on quickly without waiting for your worker threads to end when cleaning up the executor's resources. Just note that if you spawn other thread-intensive tasks or other executors afterwards in rapid succession, you might start too many threads at once.
* **daemonic** *(bool, optional)* [`False`]: whether to spawn daemonic worker. If your worker are daemonic, the interpreter will not wait for them to end when exiting. This can be useful, combined to `wait=False`, for instance, if you want your program to exit as soon as hitting ctrl+C (you might want to avoid this if your threads need to cleanup things on exit as they will be abruptly shut down).

<h4 id="executor-imap">#.imap, #.imap_unordered</h4>

Basically the same as described [here](#imap_unordered-imap) with the following arguments:

* **iterable** *(iterable)*: Any python iterable.
* **func** *(callable)*: Function used to perform the desired tasks. The function takes an item yielded by the given iterable as single argument. Note that since this function will be dispatched in worker threads, so you should ensure it is thread-safe.
* **key** *(callable, optional)*: Function returning to which "group" a given item is supposed to belong. This will be used to ensure maximum parallelism is respected.
* **parallelism** *(int or callable, optional)* [`1`]: Number of threads allowed to work on a same group at once. Can also be a function taking a group and returning its parallelism.
* **buffer_size** *(int, optional)* [`1024`]: Maximum number of items the function will buffer into memory while attempting to find an item that can be passed to a worker immediately, all while respecting throttling and group parallelism.
* **throttle** *(int or float or callable, optional)*: Optional throttle time, in seconds, to wait before processing the next item of a given group. Can also be a function taking last group, item and result and returning next throttle time for this group.

#### #.shutdown

Method used to explicitly shutdown the executor.

*Arguments*

* **wait** *(bool, optional)* [`True`]: Whether to join worker threads, i.e. wait for them to end, when shutting down the executor. Set this to `False` if you need to go on quickly without waiting for your worker threads to end when cleaning up the executor's resources. Just note that if you spawn other thread-intensive tasks or other executors afterwards in rapid succession, you might start too many threads at once.

### NamedLocks

A weakref dictionary of locks useful to make some tasks based on keys threadsafe, e.g. if you need to ensure that two threads will not be writing to the same file at once.

```python
from quenouille import NamedLocks

locks = NamedLocks()

def worker(filename):
  with locks[filename]:
    with open(filename, 'a+') as f:
      f.write('hello\n')
```

### Miscellaneous notes

#### The None group

The `imap` functions consider the `None` group (this can happen if your `key` function returns `None`) as special and will always consider the attached items can be processed right away without parallelism constraints nor throttle.

Without `key`, all items are considered as belonging to the `None` group.

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
def throttle(group, item, result):
  return 5 + (2 * random())
```

#### Caveats regarding exception raising

*Deferred generator usage exception deadlocks*

If you consume a generator returned by `imap/imap_unordered` somewhere else than where you created it, you may end up in a deadlock if you raise an exception.

This is not important when using `daemonic=True` but you might stumble upon segfaults on exit because of python reasons beyond my control.

```python
# Safe
for item in imap(...):
  raise RuntimeError

# Not safe
it = imap(...)

for item in it:
  raise RuntimeError
```

If you really want to do that, because you don't want to use the `ThreadPoolExecutor` context manager, you can try using the experimental `excepthook` kwarg:

```python
# Probably safe
it = imap(..., excepthook=True)

for item in it:
  raise RuntimeError
```

#### Caveats of using imap with queues

*Typical deadlocks*

Even if `imap` can process an input queue, you should avoid to find yourself in a situation where adding to the queue might block execution if you don't want to end in a deadlock. It can be easy to footgun yourself if your queue has a `maxsize`, for instance:

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

To enable you to add items to the queue in the loop body and so it can safely detect when your queue is drained without race condition, `quenouille` acknowledges that a task is finished only after what you execute in the loop body is done.

This means that sometimes it might be more performant to only add items to the queue from the worker functions rather than from the loop body.

*queue.task_done*

For now, `quenouille` does not call `queue.task_done` for you, so this remains your responsability, if you want to be able to call `queue.join` down the line.
