import pytest
import signal

from multiprocessing import Process
from spare import utils
from spare.errors import FileChangedDuringReadError
from time import sleep


def test_abort_if_file_changed(temporary_path, loghandler):
    path = temporary_path / 'foo'
    path.touch()

    with pytest.raises(FileChangedDuringReadError):
        with utils.abort_if_file_changes_during_read(path):
            with path.open('w') as f:
                f.write('foobar')


def test_writable(temporary_path):
    path = temporary_path / 'foo'
    path.touch()
    path.chmod(0o440)

    with pytest.raises(PermissionError):
        open(path, 'w')

    with utils.writable(path):
        with open(path, 'w'):
            pass

    with pytest.raises(PermissionError):
        open(path, 'w')

    # an inexistant path is a no-op
    with utils.writable(path / 'foobar'):
        pass


def test_delay_signal():

    class Normal(Process):
        def run(self):
            for i in range(0, 100):
                sleep(0.01)

    class Delayed(Process):
        def run(self):
            with utils.delay_signal(signal.SIGTERM, 'busy'):
                for i in range(0, 100):
                    sleep(0.01)

    # the normal process exits immedately
    process = Normal()
    process.start()
    sleep(0.1)
    process.terminate()
    sleep(0.1)
    assert not process.is_alive()

    # the other process exits when it's done
    process = Delayed()
    process.start()
    sleep(0.1)
    process.terminate()
    sleep(0.1)
    assert process.is_alive()
