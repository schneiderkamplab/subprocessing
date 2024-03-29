from dill import dumps, loads
import os
import sys

tempdir = sys.argv[1]
log_level = int(sys.argv[2])

def log(message, level=2):
    if level <= log_level:
        print(f"MAIN-{os.getpid()}: {message}", file=sys.stderr)
        sys.stderr.flush()

log("starting with log level {log_level}", level=1)
stdin = open(os.path.join(tempdir, "stdin"), 'rb')
log("opened stdin")
stdout = open(os.path.join(tempdir, "stdout"), 'wb')
log("opened stdout")

while True:
    log("waiting for input")
    length = int.from_bytes(stdin.read(8), byteorder='big')
    log("got length {length}")
    if length == 0:
        log("received termination signal")
        break
    inp = stdin.read(length)
    log(f"got input of length {len(inp)}")
    f, args, kwargs = loads(inp)
    log(f"parsed input to {f} applied to {args} and {kwargs}", level=3)
    result = f(*args, **kwargs)
    log(f"result is {result}", level=3)
    pickled = dumps(result, protocol=4)
    log(f"pickled result of length {len(pickled)}")
    stdout.write(len(pickled).to_bytes(8, byteorder='big'))
    log("wrote length")
    stdout.flush()
    log("flushed buffer")
    stdout.write(pickled)
    log("wrote pickled result")
    stdout.flush()
    log("flushed buffer")
