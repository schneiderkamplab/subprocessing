from dill import dumps, loads
import os
import subprocess
import sys
import tempfile
from time import sleep

class Worker:

    def __init__(self, log_level=1):
        self.log_level = log_level
        self.tempdir = tempfile.mkdtemp()
        self.log(f"tempdir is {self.tempdir}")
        os.mkfifo(os.path.join(self.tempdir, 'stdin'))
        self.log(f"created stdin")
        os.mkfifo(os.path.join(self.tempdir, 'stdout'))
        self.log(f"created stdout")
        self.process = subprocess.Popen([sys.executable, '-m', 'subprocessing', self.tempdir, str(self.log_level)], shell=False, stdin=None, stdout=None)
        self.log(f"started process")
        self.stdin = open(os.path.join(self.tempdir, 'stdin'), 'wb')
        self.log(f"opened stdin")
        self.stdout = open(os.open(os.path.join(self.tempdir, 'stdout'), os.O_RDONLY | os.O_NONBLOCK), 'rb')
        self.log(f"opened stdout")

    def log(self, message, level=2):
        if level <= self.log_level:
            print(f"WORKER: {message}", file=sys.stderr)

    def __del__(self):
        self.terminate()
        self.stdin.close()
        self.log(f"closed stdin")
        self.stdout.close()
        self.log(f"closed stdout")

    def terminate(self):
        self.stdin.write((0).to_bytes(8, byteorder='big'))
        self.stdin.flush()
        self.log(f"terminated process")

    def submit(self, func, args, kwargs):
        pickled = dumps((func, args, kwargs))
        self.log(f"pickled to length {len(pickled)}")
        self.stdin.write(len(pickled).to_bytes(8, byteorder='big'))
        self.log(f"sent length {len(pickled)}", level=1)
        self.stdin.write(pickled)
        self.log(f"sent input of length {len(pickled)}")
        self.stdin.flush()

    def receive_blocking(self):
        while True:
            try:
                return self.receive()
            except EOFError:
                sleep(0.1)

    def receive(self):
        length_bytes = self.stdout.read(8)
        if not length_bytes:
            self.log(f"no data available")
            raise EOFError
        length = int.from_bytes(length_bytes, byteorder='big')
        self.log(f"received length {length}")
        result = self.stdout.read(length)
        self.log(f"received input of length {length}", level=1)
        return loads(result)
