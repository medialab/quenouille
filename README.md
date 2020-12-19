[![Build Status](https://github.com/medialab/quenouille/workflows/Tests/badge.svg)](https://github.com/medialab/quenouille/actions)

# Quenouille

A library of multithreaded iterator workflows for python.

## Installation

You can install `quenouille` with pip with the following command:

```
pip install quenouille
```

## Usage
* [imap](#imap)
* [imap_unordered](#imapunordered)
* [Caveats](#caveats)

### imap

Function lazily consuming an iterator and applying the desired function over the yielded items in a multithreaded fashion. The function will yield results in an order consistent with the provided iterator.

Furthermore, it's possible to tweak options regarding group parallelism if you ever need to ensure that a limited number of threads may perform their tasks over the same group, e.g. a domain name when fetching urls: you can give a function extracting the group from the current task, you can tweak the maximum number of threads working on a same group and finally you can edit a group's buffer size to let the function load more values into memory in hope of finding next ones it can process without needing to wait.

If you don't care about output order and want snappier performance, the library also exports an [`imap_unordered`](#imap_unordered) method.

```python
import csv
from quenouille import imap

# Example fetching urls from a CSV file
with open(csv_path, 'r') as f:
  reader = csv.DictReader(f)

  urls = (line['url'] for line in reader)

  # The `fetch` function remains to be implemented by the reader
  for html in imap(urls, fetch, 10):

    # Results will be yielded in lines order
    print(html)
```

*Arguments*

* **iterable** *iterable*: Any python iterable.
* **func** *callable*: Function used to perform desired tasks. The function takes any item yielded from the given iterable as sole argument. Note that since this function will be dispatched in a multithreaded environment, it should be thread-safe.
* **threads** *int*: Number of threads to use.
* **group** *?callable* [`None`]: Function taking a single item yielded by the provided iterable and returning its group.
* **group_parallelism** *?int* [`Infinity`]: Maximum number of threads that can work on the same group at once. Defaults to no limit. This option requires that you give a function as the `group` argument.
* **group_buffer_size** *?int* [`1`]: Maximum number of values that will be loaded into memory from the iterable before waiting for other relevant threads to be available.
* **group_throttle** *?float|?callable* [`0`]: throttle time to wait (in seconds) between two tasks on the same group. Can also be a function taking the group and item and returning throttle time.
* **group_throttle_entropy** *?float* [`0`]: additional random throttle time between `0` and given value. Useful to simulate erratic behavior.
* **listener** *callable* [`None`]: A function called on certain events with the name of the event and the related item.

*Events*

* **start**: Emitted when the given function actually starts to work on a yielded item.

### imap_unordered

Function lazily consuming an iterator and applying the desired function over the yielded items in a multithreaded fashion. The function will yield results in arbitrary order based on thread completion.

Furthermore, it's possible to tweak options regarding group parallelism if you ever need to ensure that a limited number of threads may perform their tasks over the same group, e.g. a domain name when fetching urls: you can give a function extracting the group from the current task, you can tweak the maximum number of threads working on a same group and finally you can edit a group's buffer size to let the function load more values into memory in hope of finding next ones it can process without needing to wait.

If output order is important to you, the library also exports an [`imap`](#imap) method.

```python
import csv
from quenouille import imap_unordered

# Example fetching urls from a CSV file
with open(csv_path, 'r') as f:
  reader = csv.DictReader(f)

  urls = (line['url'] for line in reader)

  # The `fetch` function remains to be implemented by the reader
  for html in imap_unordered(urls, fetch, 10):

    # Results will be yielded in arbitrary order as soon as tasks complete
    print(html)
```

*Arguments*

* **iterable** *iterable*: Any python iterable.
* **func** *callable*: Function used to perform desired tasks. The function takes any item yielded from the given iterable as sole argument. Note that since this function will be dispatched in a multithreaded environment, it should be thread-safe.
* **threads** *int*: Number of threads to use.
* **group** *?callable* [`None`]: Function taking a single item yielded by the provided iterable and returning its group.
* **group_parallelism** *?int* [`Infinity`]: Maximum number of threads that can work on the same group at once. Defaults to no limit. This option requires that you give a function as the `group` argument.
* **group_buffer_size** *?int* [`1`]: Maximum number of values that will be loaded into memory from the iterable before waiting for other relevant threads to be available.
* **group_throttle** *?float|?callable* [`0`]: throttle time to wait (in seconds) between two tasks on the same group. Can also be a function taking the group and item and returning throttle time.
* **group_throttle_entropy** *?float* [`0`]: additional random throttle time between `0` and given value. Useful to simulate erratic behavior.
* **listener** *callable* [`None`]: A function called on certain events with the name of the event and the related item.

*Events*

* **start**: Emitted when the given function actually starts to work on a yielded item.

### Caveats

*On having more threads than the size of the consumed iterator*

This should be safe but note that it can have a slight performance cost related to the fact that the library will allocate and terminate threads that won't be used anyway. So you should probably clamp the number of threads based upon the size of your iterator if you know it beforehand (and use a condition not to call `imap` etc. on an empty iterator, for instance).
