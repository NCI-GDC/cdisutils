import unittest
from cdisutils.pool import AsyncProcessPool


def func(x):
    return x, x*2


class TestAsyncProcessPool(unittest.TestCase):

    def test_manual_put(self):
        p = AsyncProcessPool(10)
        p.start(func)
        work = range(1000)
        parallel_results = set()

        for i in work:
            p.put(i)
        for i in work:
            parallel_results.add(p.get())
        serial_results = set(map(func, work))
        self.assertEqual(serial_results, parallel_results)

    def test_manual_put_interlocked(self):
        p = AsyncProcessPool(10)
        p.start(func)
        work = range(1000)
        parallel_results = set()
        for i in work:
            p.put(i)
            parallel_results.add(p.get())
        serial_results = set(map(func, work))
        self.assertEqual(serial_results, parallel_results)

    def test_init_args(self):
        p = AsyncProcessPool(10)
        work = range(1000)
        p.start(func, work)
        parallel_results = set()
        for i in work:
            parallel_results.add(p.get())
        serial_results = set(map(func, work))
        self.assertEqual(serial_results, parallel_results)

    def test_put_all(self):
        p = AsyncProcessPool(10)
        work = range(1000)
        p.start(func)
        p.put_all(work)
        parallel_results = set(p)
        serial_results = set(map(func, work))
        self.assertEqual(serial_results, parallel_results)

    def test_iterator(self):
        p = AsyncProcessPool(10)
        work = range(1000)
        p.start(func, work)
        parallel_results = set(p)
        serial_results = set(map(func, work))
        self.assertEqual(serial_results, parallel_results)

    def test_condensed(self):
        parallel_results = set(AsyncProcessPool(10, func, range(1000)))
        serial_results = set(map(func, range(1000)))
        self.assertEqual(serial_results, parallel_results)

    def test_started_error(self):
        p = AsyncProcessPool(10, func, range(10))
        self.assertRaises(RuntimeError, p.start, func)

    def test_overdraw_error(self):
        p = AsyncProcessPool(10, func, range(10))
        with self.assertRaises(RuntimeError):
            for i in range(20):
                p.get()
