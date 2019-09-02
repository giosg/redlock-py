import unittest

from redlock import Redlock, MultipleRedlockException

TEST_LOCK_TTL_MILLIS = 1000  # Milliseconds


class TestRedlock(unittest.TestCase):

    def setUp(self):
        self.redlock = Redlock([{"host": "localhost"}])

    def test_lock(self):
        lock_name = "test_lock"
        lock = self.redlock.lock(lock_name, 100, TEST_LOCK_TTL_MILLIS)
        self.assertEqual(lock.resource, lock_name)
        self.redlock.unlock(lock)
        lock = self.redlock.lock(lock_name, 10, TEST_LOCK_TTL_MILLIS)
        self.redlock.unlock(lock)

    def test_blocked(self):
        lock_name = "test_blocked"
        lock = self.redlock.lock(lock_name, 1000, TEST_LOCK_TTL_MILLIS)

        with self.assertRaises(MultipleRedlockException):
            self.redlock.lock(lock_name, 10, TEST_LOCK_TTL_MILLIS)

        self.redlock.unlock(lock)

    def test_bad_connection_info(self):
        with self.assertRaises(Warning):
            Redlock([{"cat": "hog"}])

    def test_py3_compatible_encoding(self):
        lock_name = "test_py3_compatible_encoding"
        lock = self.redlock.lock(lock_name, 1000, TEST_LOCK_TTL_MILLIS)
        key = self.redlock.servers[0].get(lock_name)
        self.assertEqual(lock.key, int(key))

    def test_ttl_not_int_trigger_exception_value_error(self):
        lock_name = "ttl_not_int"
        with self.assertRaises(ValueError):
            self.redlock.lock(lock_name, 1000.0, 1000.0)

    def test_multiple_redlock_exception(self):
        ex1 = Exception("Redis connection error")
        ex2 = Exception("Redis command timed out")
        exc = MultipleRedlockException([ex1, ex2])
        exc_str = str(exc)
        self.assertIn('connection error', exc_str)
        self.assertIn('command timed out', exc_str)
