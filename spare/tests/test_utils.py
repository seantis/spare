import pytest

from spare import utils
from spare.errors import FileChangedDuringReadError


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
