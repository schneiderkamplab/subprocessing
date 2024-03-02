from time import sleep

from .worker import Worker

class Process:
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, *, daemon=None, log_level=1):
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.log_level = log_level

    def __getstate__(self) -> object:
        return self.target, self.args, self.kwargs, self.log_level

    def __setstate__(self, state) -> None:
        self.target, self.args, self.kwargs, self.log_level = state

    def run(self):
        return self.target(*self.args, **self.kwargs)

    def start(self):
        self.worker = Worker(log_level=self.log_level)
        self.worker.submit(self.run, [], {})

    def join(self):
        result = self.worker.receive()
        del self.worker
        return result

    def terminate(self):
        self.worker.terminate()

    def is_alive(self):
        return self._process.is_alive()