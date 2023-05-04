# =============================================================================
# Quenouille Various Utils
# =============================================================================
#
# Miscellaneous utility functions.
#
from typing import TypeVar, Generic, List, Dict, Tuple, Hashable, Iterator

import os
import time
import heapq
from threading import Lock, Timer
from weakref import WeakValueDictionary
from queue import Empty, Full, Queue

from quenouille.constants import TIMER_EPSILON

ItemType = TypeVar("ItemType")


def clear(q: "Queue") -> None:
    while True:
        try:
            q.get_nowait()
        except Empty:
            break


def flush(q: "Queue[ItemType]", n: int, msg: ItemType) -> None:
    for _ in range(n):
        try:
            q.put_nowait(msg)
        except Full:
            break


def smash(q: "Queue[ItemType]", v: ItemType) -> None:
    clear(q)
    q.put_nowait(v)


def queue_iter(q: "Queue[ItemType]") -> Iterator[ItemType]:
    while True:
        try:
            yield q.get_nowait()
        except Empty:
            break


def is_usable_queue(v) -> bool:
    # NOTE: get_nowait is the only method used by quenouille when working
    # on a queue given by the user, as of now.
    return hasattr(v, "get_nowait") and callable(v.get_nowait)


# As per: https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
def get_default_maxworkers() -> int:
    return min(32, (os.cpu_count() or 1) + 4)


class SmartTimer(Timer):
    """
    A Timer subclass able to return information about its execution time.
    """

    def __init__(self, *args, **kwargs):
        self.started_time = time.time()  # type: float
        super().__init__(*args, **kwargs)

    def elapsed(self) -> float:
        return time.time() - self.started_time

    def remaining(self) -> float:
        return self.interval - self.elapsed()


LockedItemKey = TypeVar("LockedItemKey")


class NamedLocks(Generic[LockedItemKey]):
    def __init__(self):
        self.own_lock = Lock()  # type: Lock
        self.locks = (
            WeakValueDictionary()
        )  # type: WeakValueDictionary[LockedItemKey, Lock]

    def __repr__(self):
        with self.own_lock:
            return "<{name} acquired={acquired!r}>".format(
                name=self.__class__.__name__, acquired=list(self.locks)
            )

    def __len__(self) -> int:
        with self.own_lock:
            return len(self.locks)

    def __contains__(self, key: LockedItemKey) -> bool:
        with self.own_lock:
            return key in self.locks

    def __getitem__(self, key: LockedItemKey) -> Lock:
        with self.own_lock:
            if key not in self.locks:
                lock = Lock()
                self.locks[key] = lock
                return lock

            return self.locks[key]

    def __call__(self, key: LockedItemKey) -> Lock:
        return self[key]


HeapSetItemType = TypeVar("HeapSetItemType", bound=Hashable)


class TimedHeapSet(Generic[HeapSetItemType]):
    def __init__(self):
        self.clear()

    def clear(self) -> None:
        self.heap = []  # type: List[Tuple[float, HeapSetItemType]]
        self.map = {}  # type: Dict[HeapSetItemType, float]

    def __len__(self) -> int:
        return len(self.heap)

    def __contains__(self, item: HeapSetItemType) -> bool:
        return item in self.map

    def add(self, item: HeapSetItemType, duration: float) -> None:
        next_time = time.time() + duration
        self.map[item] = next_time
        heapq.heappush(self.heap, (next_time, item))

    def wait_for(self, item: HeapSetItemType) -> None:
        # NOTE: we cleanup before just in case
        self.cleanup()

        next_time = self.map.get(item)

        if next_time is None:
            return

        time.sleep(max((next_time - time.time()), 0) + TIMER_EPSILON)
        self.cleanup()

    def cleanup(self) -> None:
        current_time = time.time()

        while self.heap and current_time >= self.heap[0][0] - TIMER_EPSILON:
            del self.map[self.heap[0][1]]
            heapq.heappop(self.heap)
