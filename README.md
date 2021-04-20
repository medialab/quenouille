[![Build Status](https://github.com/medialab/quenouille/workflows/Tests/badge.svg)](https://github.com/medialab/quenouille/actions)

# Quenouille

A library of multithreaded iterator workflows for python.

## Installation

You can install `quenouille` with pip with the following command:

```
pip install quenouille
```

## Usage

* [imap_unordered, imap](#imap_unordered-imap)
* [Miscellaneous notes](#miscellaneous-notes)

### imap_unordered, imap

Function lazily consuming an iterable and applying the desired function over the yielded items in a multithreaded fashion. The function will yield results in an order consistent with the provided iterable.

Furthermore, this iterable consumer is also able to respect group constraints regarding parallelism and throttling.

For instance, if you need to download urls in a multithreaded fashion and need to ensure that you won't hit the same domain name more than twice at the same time, this function can be given proper settings to ensure the desired behavior.

The same can be said if you need to make sure to wait for a certain amount of time between two hits on the same domain name by using this function's throttling.

Finally note that this function comes in two flavors: `imap_unordered`, which will output processed items as soon as they are done, and `imap`, which will output items in the same order as the input, at the price of slightly worse performance and an increased memory footprint that depends on the number of items that have been processed before the next item can be yielded.

```python
import csv
from quenouille import imap_unordered

def fetch(url):
  # The `fetch` function remains to be implemented by the reader
  (...)
  return html

# Example fetching urls from a CSV file
with open(csv_path, 'r') as f:
  reader = csv.DictReader(f)

  urls = (line['url'] for line in reader)

  # Performing 10 requests at a time:
  for html in imap(urls, fetch, 10):
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

### Miscellaneous notes

*On having more threads than the size of the consumed iterator*

This should be safe but note that it can have a slight performance cost related to the fact that the library will allocate and terminate threads that won't be used anyway. So you should probably clamp the number of threads based upon the size of your iterator if you know it beforehand (and use a condition not to call `imap` etc. on an empty iterator, for instance).


TODO: queue, executor, thread safety of the worker, examples and caveats, link to minet
