from multiprocessing.pool import Pool


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
