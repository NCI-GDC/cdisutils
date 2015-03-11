from multiprocessing.pool import Pool
from multiprocessing import Process, Queue


def split(self, a, n):
    """Split a into n evenly sized chunks"""
    k, m = len(a) / n, len(a) % n
    chunks = [a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
              for i in xrange(n)]
    assert sum([len(c) for c in chunks]) == len(a)
    return chunks


class ProcessPool(object):

    def __init__(self, processes):
        self.processes = processes

    def map(self, func, sequence):
        pool = Pool(self.processes)
        try:
            results = pool.map_async(func, sequence).get(999999)
        except KeyboardInterrupt:
            pool.close()
            pool.terminate()
            pool.join()
            return

        pool.close()
        return results


def async_worker(f, q_in, q_out):
    while True:
        args, kwargs = q_in.get()
        result = f(*args, **kwargs)
        q_out.put(result)


class AsyncProcessPool(object):

    """
    This is a pool meant to async map across arbitrary input.

    """

    def __init__(self, proc_count, target=None, initial_args=None):
        self.proc_count = proc_count
        self.q_in = None
        self.q_out = None
        self.puts, self.gets = 0, 0
        self.started = False
        if target:
            self.start(target, initial_args)

    def start(self, target, initial_args=None):
        if self.started:
            raise RuntimeError('pool was already started.')

        self.q_in = Queue()
        self.q_out = Queue()
        self.processes = [
            Process(
                target=async_worker,
                args=(target, self.q_in, self.q_out),
            ) for i in range(self.proc_count)]
        for p in self.processes:
            p.daemon = True
            p.start()
        if initial_args:
            self.put_all(initial_args)
        self.started = True

    def put(self, *args, **kwargs):
        self.puts += 1
        self.q_in.put((args, kwargs))

    def put_all(self, args):
        for arg in args:
            self.put(arg)

    def __iter__(self):
        while self.gets < self.puts:
            yield self.get()

    def __next__(self):
        while self.gets > self.puts:
            yield self.get()

    def get(self, timeout=9999999):
        if self.gets >= self.puts:
            raise RuntimeError(
                'AsyncProcessPool is meant to have a one-to-one mapping of '
                'input to output.  This means that you should be calling '
                'get() the same number of times as you call put(). '
                'puts: {}\ngets: {}'.format(self.puts, self.gets))
        try:
            result = self.q_out.get(timeout=timeout)
            self.gets += 1
        except KeyboardInterrupt:
            for p in self.processes:
                p.terminate()
            raise
        return result

    def terminate(self):
        for p in self.processes:
            p.terminate()
        self.q_in = None
        self.q_out = None
