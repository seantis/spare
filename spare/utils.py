import os
import signal
import stat
import sys
import weakref

from boto3 import resource
from botocore.client import Config
from contextlib import contextmanager, ExitStack
from logbook import SyslogHandler
from pathlib import Path
from spare import log
from spare.errors import FileChangedDuringReadError


CLEANUP_FUNCTIONS = None
KILL_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def read_in_chunks(fileobj, chunksize):
    fileobj.seek(0)

    while True:
        chunk = fileobj.read(chunksize)

        if chunk:
            yield chunk
        else:
            break


def clean_exit(*args):
    """ Exits cleanly, running cleanup functions first. """

    return_code = 0

    for ref in CLEANUP_FUNCTIONS:
        try:
            fn = ref()

            if fn:
                fn()
        except Exception:
            return_code = 1
            log.exception("Failure during signal handling")

    sys.exit(return_code)


def on_kill(fn, signals=KILL_SIGNALS):
    """ Executes cleanup functions when the program is killed. """

    assert isinstance(fn, weakref.ref)
    global CLEANUP_FUNCTIONS

    if CLEANUP_FUNCTIONS is None:
        CLEANUP_FUNCTIONS = set()

        for signum in signals:
            signal.signal(signum, clean_exit)

    CLEANUP_FUNCTIONS.add(fn)


@contextmanager
def abort_if_file_changes_during_read(path):
    before = Path(path).stat()
    yield
    after = Path(path).stat()

    for attr in ('st_mtime_ns', 'st_size', 'st_ino', 'st_dev'):
        if getattr(before, attr) != getattr(after, attr):
            raise FileChangedDuringReadError(path)


def s3_client(endpoint, access_key, secret_key,
              connect_timeout=5, read_timeout=10, max_attempts=2):
    endpoint = '://' in endpoint and endpoint or f'https://{endpoint}'

    s3 = resource(
        service_name='s3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={'max_attempts': max_attempts}
        ))

    s3.endpoint = endpoint
    s3.access_key = access_key
    s3.secret_key = secret_key

    return s3


@contextmanager
def writable(path):
    """ Ensures the given path is writable, resetting the mode afterwards.

    Usage:

        with writable('/my/file'):
            with open('/my/file', 'w') as f:
                f.write('foobar')

    """
    if not os.path.exists(path):
        yield
    else:
        mode = stat.S_IMODE(os.stat(path).st_mode)

        if mode != mode | stat.S_IWUSR:
            os.chmod(path, mode | stat.S_IWUSR)

        yield

        os.chmod(path, mode)


# coverage is skipped here because it is tested in a subprocess
class delay_signal(object):  # pragma: no cover
    """ Blocks the handling of the given signal inside the with statement.
    Once the with statement is exited, the last received signal is replayed.

    This basically stops the user from restarting a server which is currently
    running a backup or restore operation.

    A message is sent to the syslog if this happens.

    Usage:

        with delay_signal(SIGTERM, 'doing something important'):
            pass

    """

    def __init__(self, signal, message):
        self.signal = signal
        self.message = message

    def __enter__(self):
        self.received = None
        self.previous = signal.signal(self.signal, self.handler)

    def handler(self, signal, frame):
        self.received = (signal, frame)

        try:
            with SyslogHandler('spare', level='WARNING').applicationbound():
                log.warn(f"Delaying handling of {self.signal.name}")
        except IOError:
            pass

    def __exit__(self, type, value, traceback):
        signal.signal(self.signal, self.previous)

        if self.received:
            self.previous(*self.received)


@contextmanager
def delay_signals(message, signals=KILL_SIGNALS):
    """ Delay multiple signals at once. """

    with ExitStack() as stack:
        for signum in signals:
            stack.enter_context(delay_signal(signum, message))

        yield
