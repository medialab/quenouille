# Redesign Documents

* Using a threadpool executor
* Using a global buffer limit, not a per group one (maybe don't keep a queue per group then)
* Relying on a queue as input and wrap an iterator as a queue source
* Rework throttling with a throttled lock or by keeping next allowed time per group
* getgroup must only be called a single time
* maybe drop listening to simplify for the time being
* throttling should allow new values to get buffered in hope of finding suitable one
* handle exceptions from callable kwargs (throttle and group key)
* `group` -> `key`?
* threadsafe `can_do_job`
* namedtuple job for maintenability
