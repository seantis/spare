import os
import pytest
import socket
import tempfile

from contextlib import suppress
from io import UnsupportedOperation
from pathlib import Path
from spare.inventory import Inventory


def test_empty_inventory(temporary_path):
    inventory = Inventory(temporary_path)
    inventory.scan()

    assert not inventory.structure
    assert not inventory.files


def test_single_file_inventory(temporary_path):
    with (temporary_path / 'foo').open('w') as f:
        f.write('foo')

    inventory = Inventory((temporary_path / 'foo'))
    inventory.scan()

    assert len(inventory.structure) == 1
    assert len(inventory.files) == 1

    assert '.' in inventory.structure
    assert list(inventory.files.values())[0][0] == '.'


def test_empty_file_inventory(temporary_path):
    (temporary_path / 'foo').touch()

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert len(inventory.structure) == 1
    assert len(inventory.files) == 0

    assert inventory.structure['foo']['type'] == 'file'
    assert inventory.structure['foo']['empty'] is True


def test_single_symlink_inventory(temporary_path):
    (temporary_path / 'foo').touch()
    (temporary_path / 'bar').symlink_to((temporary_path / 'foo'))

    inventory = Inventory((temporary_path / 'bar'))
    inventory.scan()

    assert inventory.structure['.']['type'] == 'file'


def test_nested_paths(temporary_path):
    (temporary_path / 'foo').mkdir()

    with (temporary_path / 'foo' / 'bar').open('w') as f:
        f.write('foo')

    with (temporary_path / 'bar').open('w') as f:
        f.write('foo')

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert 'foo' in inventory.structure
    assert 'foo/bar' in inventory.structure
    assert 'bar' in inventory.structure

    # two files with the same data
    assert len(inventory.files) == 1
    assert len(tuple(inventory.files.values())[0]) == 2


def test_symlinks(temporary_path):
    with (temporary_path / 'foo').open('w') as f:
        f.write('foo')

    (temporary_path / 'bar').symlink_to((temporary_path / 'foo'))

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert len(inventory.files) == 1
    assert len(inventory.structure) == 2

    assert inventory.structure['bar']['target'] == 'foo'


def test_ignore_devices():
    # if we can scan /dev without errors we handle all the special devices
    inventory = Inventory('/dev')

    # try a bunch of folders to really get all kinds of devices
    # no need to do this recursively (which might not be a great idea)
    for path in ('/dev', '/dev/block', '/dev/disk/by-uuid'):
        with suppress(FileNotFoundError, UnsupportedOperation):
            inventory.scan_directory(Path(path), recurse=False)


def test_read_outside_symlink(temporary_path):

    with tempfile.TemporaryDirectory() as d:
        (Path(d) / 'outside').touch()
        (temporary_path / 'inside').symlink_to(Path(d) / 'outside')

        inventory = Inventory(temporary_path)
        inventory.scan()

        assert inventory.structure['inside']['type'] == 'file'


def test_read_outside_symlink_to_directory():

    with tempfile.TemporaryDirectory() as planet:
        with (Path(planet) / 'earth').open('w') as f:
            f.write('earth')

        with tempfile.TemporaryDirectory() as continent:
            with (Path(continent) / 'europe').open('w') as f:
                f.write('europe')

            (Path(continent) / 'planet').symlink_to(planet)

            with tempfile.TemporaryDirectory() as country:
                with (Path(country) / 'switzerland').open('w') as f:
                    f.write('switzerland')

                (Path(country) / 'continent').symlink_to(continent)

                inventory = Inventory(country)
                inventory.scan()

    assert inventory.structure['switzerland']['type'] == 'file'
    assert inventory.structure['continent']['type'] == 'directory'
    assert inventory.structure['continent/europe']['type'] == 'file'
    assert inventory.structure['continent/planet']['type'] == 'directory'
    assert inventory.structure['continent/planet/earth']['type'] == 'file'

    values = {p for paths in inventory.files.values() for p in paths}

    assert values == {
        'switzerland',
        'continent/europe',
        'continent/planet/earth',
    }


def test_detect_infinite_symlink_loop():

    with tempfile.TemporaryDirectory() as outside:
        with tempfile.TemporaryDirectory() as inside:
            (Path(outside) / 'inside').symlink_to(inside)
            (Path(inside) / 'outside').symlink_to(outside)

            with pytest.raises(OSError):
                inventory = Inventory(inside)
                inventory.scan()


def test_ignore_broken_symlink(temporary_path):
    (temporary_path / 'foo').touch()
    (temporary_path / 'bar').symlink_to((temporary_path / 'foo'))
    (temporary_path / 'foo').unlink()

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert not inventory.structure


def test_ignore_fifo(temporary_path):
    fifo = (temporary_path / 'fifo')

    os.mkfifo(fifo)

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert not inventory.structure
    assert not inventory.files


def test_ignore_socket_file(temporary_path):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(str(temporary_path / 'socket'))

    inventory = Inventory(temporary_path)
    inventory.scan()

    assert not inventory.structure
    assert not inventory.files


def test_skip_files(temporary_path):
    (temporary_path / 'foo').mkdir()
    (temporary_path / 'foo' / 'bar').touch()
    (temporary_path / 'bar').mkdir()
    (temporary_path / 'bar' / 'bar').touch()

    inventory = Inventory(temporary_path, skip=('./foo', ))
    inventory.scan()

    assert set(inventory.structure.keys()) == {
        'bar',
        'bar/bar',
    }

    inventory = Inventory(temporary_path, skip=('./bar', ))
    inventory.scan()

    assert set(inventory.structure.keys()) == {
        'foo',
        'foo/bar',
    }

    inventory = Inventory(temporary_path, skip=('./foo/bar', ))
    inventory.scan()

    assert set(inventory.structure.keys()) == {
        'bar',
        'bar/bar',
        'foo',
    }
