DistributedLock
==============

Using Memcached is a distributed lock.
      =========
Lightweight and fast performance guarantees.


Here's an example:

```python
lock = DistributedLock("name")

try:
    if lock.acquire(60):
        execute code...
finally:
    lock.release()
```

```python
@distributed_lock()
def test():
    execute code...
```
