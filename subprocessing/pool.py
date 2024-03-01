from itertools import zip_longest
from time import sleep

from .worker import Worker

class Pool:

    def __init__(self, processes=2, module_name='subprocessing', log_level=1):
        self.n_workers = processes
        self.module_name = module_name
        self.log_level = log_level
        self.workers = [Worker(log_level=self.log_level) for _ in range(self.n_workers)]
        self.active_workers = []
        self.next_worker = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del self

    def __del__(self):
        for worker in self.workers:
            del worker

    def map(self, func, argss, kwargss=[]):
        for args, kwargs in zip_longest(argss, kwargss, fillvalue={}):
            if len(self.active_workers) == self.n_workers:
                while True:
                    for i in range(len(self.active_workers)):
                        worker = self.active_workers[i]
                        try:
                            yield worker.receive()
                            break
                        except EOFError:
                            continue
                    else:
                        sleep(0.1)
                        continue
                    break
            worker = self.workers[self.next_worker]
            self.next_worker = (self.next_worker + 1) % self.n_workers
            self.active_workers.append(worker)
            worker.submit(func, args, kwargs)
        while self.active_workers:
            worker = self.active_workers.pop(0)
            yield worker.receive_blocking()