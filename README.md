DistributeLock
==============

very simple and fast distribute lock.


Here's an example:

```python
lock = DistributeLock("name")

try:
    if lock.acquire(60):
        execute code...
finally:
    lock.release()
```

```python
@distribute_lock()
def test():
    execute code...
```