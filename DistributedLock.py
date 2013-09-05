# -*- coding: utf-8 -*-

from contextlib import contextmanager
import logging
import pylibmc
import time

#Memcached config
_MEMCACHED_IP = "localhost"
_MEMCACHED_PORT = "11212"
_MEMCACHED_POOL_SIZE = 10

#DistributeLock config
_RETRY_TIME = 0.01

#connect memcached for distribute lock
mc_pool = pylibmc.ClientPool()
for i in range(_MEMCACHED_POOL_SIZE):
    mc = pylibmc.Client(['%s:%s' % (_MEMCACHED_IP, _MEMCACHED_PORT)], binary=True)
    mc_pool.fill(mc, i)


class DistributedLock:
    """
    Distributed Lock.


    >> example code <<
    lock = DistributedLock("name")
    try:
        if lock.acquire(60):
            execute code...
    finally:
        lock.release()
    """
    _INIT_MESSAGE = "M1"
    _CLOSE_MESSAGE = "M2"

    def __init__(self, key, pool=mc_pool, timeout=60):
        self.key = "lock:%s" % key
        self.wait_key = key + "-wait"
        self.event_key = key + "-event"
        self.event_wait_key = key + "-event_wait"
        self.pool = pool
        self.timeout = timeout

        with self.pool.reserve() as client:
            client.set(self.key, 1, timeout)
            client.set(self.wait_key, 0, timeout)
            client.set(self.event_key, 0, timeout)
            client.set(self.event_wait_key, 0, timeout)

        logging.debug("DistributeLock init.")

    def acquire(self, blocking=True):
        """
        Acquire a lock.

        :param blocking: Decide whether to standby. Wait is True.
        """
        wait_key = self._find_wait_key_and_regist_event()
        logging.debug("wait_key: %s" % wait_key)

        success = self._wait_key(wait_key, blocking)

        return success

    def release(self):
        """
        Release the lock.
        """
        with self.pool.reserve() as client:
            inc = client.incr(self.key)
            dec = client.decr(self.event_key)
            logging.debug("release. key inc : %s,  event_key dec : %s", inc, dec)

    def initialize(self):
        """
        Initializes the lock.
        """
        with self.pool.reserve() as client:
            result = client.set(self.key, self._INIT_MESSAGE)
            logging.debug("push message : %s", result)

    def close(self):
        """
        close the lock
        """
        with self.pool.reserve() as client:
            client.set(self.key, self._CLOSE_MESSAGE)

            wait_client_count = client.get(self.event_key)
            current_count = client.incr(self.event_wait_key)

            logging.debug("wait_client : %s, current_count : %s", wait_client_count, current_count)

            if wait_client_count is None or wait_client_count == 0:
                return True

            while True:
                count = client.get(self.event_wait_key)

                if count == wait_client_count:
                    break

                time.sleep(_RETRY_TIME)

            client.delete(self.key)
            client.delete(self.wait_key)
            client.delete(self.event_key)
            client.delete(self.event_wait_key)

        return True

    def _find_wait_key_and_regist_event(self):
        """
        standby key returns and register for Event.
        """
        with self.pool.reserve() as client:
            client.incr(self.event_key)
            result = client.incr(self.wait_key)

        return result

    def _wait_key(self, wait_key, blocking):
        with self.pool.reserve() as client:
            while True:
                index = client.get(self.key)
                logging.debug("current index : %s", index)

                if index is None:
                    raise RuntimeError("Lock time out error.")

                #LockInitialization push message event
                if index == self._INIT_MESSAGE:
                    wait_key = self._initialize()

                #Close push message event
                if index == self._CLOSE_MESSAGE:
                    raise RuntimeError("Lock is close.")

                #match Current Lock
                if wait_key == index:
                    break

                #None Blocking
                if not blocking:
                    return False

                time.sleep(_RETRY_TIME)
            return True

    def _initialize(self):
        with self.pool.reserve() as client:
            wait_client_count = client.get(self.event_key)
            current_count = client.incr(self.event_wait_key)

            client.set(self.key, 1, self.timeout)
            client.set(self.wait_key, 0, self.timeout)

            logging.debug("wait_client : %s, current_count : %s", wait_client_count, current_count)
            while True:
                count = client.get(self.event_wait_key)

                if count == wait_client_count:
                    break

                time.sleep(_RETRY_TIME)

            client.set(self.event_wait_key, 0)
            return client.incr(self.wait_key)


class DistributeLockPool(dict):
    """
    Pooling the name given by the Distribute Lock can be
    """

    @contextmanager
    def reserve(self, name):
        mc = self.pop(name, None)

        if mc is None:
            mc = DistributeLock(name)
        try:
            yield mc
        finally:
            self[name] = mc


lockPool = DistributeLockPool()


def distributed_lock(timeout=60):
    """
    DistributeLock decorator

    >> example code <<

    @distributed_lock()
    def test():
        execute code...
    """

    def call(func):
        def doLock(*args, **kwargs):
            result = None
            with lockPool.reserve(func.__name__) as lock:
                try:
                    if lock.acquire(timeout):
                        result = func(*args, **kwargs)
                finally:
                    lock.release()

            return result

        return doLock

    return call