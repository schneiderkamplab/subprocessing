from dill import dumps, loads

from .worker import Worker

class Process:
    def __init__(self, target, args=(), kwargs={}, log_level=1):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.log_level = log_level

    def start(self):
        self.worker = Worker(log_level=self.log_level)
        self.worker.submit(self.target, self.args, self.kwargs)

    def join(self):
        result = self.worker.receive()
        del self.worker
        return result

    def is_alive(self):
        return self._process.is_alive()