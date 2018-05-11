import os
import secrets
import pytest

from pathlib import Path
from spare.backup import create, restore, validate
from spare.envoy import Envoy
from spare.errors import TargetPathNotEmpty, SnapshotMismatchError
from spare.snapshot import SnapshotCollection
from tempfile import TemporaryDirectory


def test_create(s3, temporary_path):
    envoy = Envoy(s3, 'my-bucket', 'password')

    with (temporary_path / 'foo').open('w') as f:
        f.write('foo')

    create(temporary_path, s3, 'my-bucket', 'password')
    prefixes = set(envoy.prefixes())
    assert len(prefixes) == 2

    create(temporary_path, s3, 'my-bucket', 'password')
    prefixes = set(envoy.prefixes())
    assert len(prefixes) == 2

    with (temporary_path / 'bar').open('w') as f:
        f.write('bar')

    create(temporary_path, s3, 'my-bucket', 'password')
    prefixes = set(envoy.prefixes())
    assert len(prefixes) == 3

    (temporary_path / 'bar').unlink()

    create(temporary_path, s3, 'my-bucket', 'password')
    prefixes = set(envoy.prefixes())
    assert len(prefixes) == 2


def test_create_exclude(s3, temporary_path):
    envoy = Envoy(s3, 'my-bucket', 'password')

    with (temporary_path / 'foo').open('w') as f:
        f.write('foo')

    with (temporary_path / 'bar').open('w') as f:
        f.write('bar')

    create(temporary_path, s3, 'my-bucket', 'password', skip=('./foo'))
    assert len(set(envoy.prefixes())) == 2


def test_large_file(s3):
    content = secrets.token_bytes(1024*1024)

    with TemporaryDirectory() as d:
        d = Path(d)

        with (d / 'foo').open('wb') as f:
            f.write(content)

        create(d, s3, 'my-bucket', 'password')

    with TemporaryDirectory() as d:
        d = Path(d)

        restore(d, s3, 'my-bucket', 'password')

        with (d / 'foo').open('rb') as f:
            assert f.read() == content


def test_restore(s3):

    with TemporaryDirectory() as d:
        d = Path(d)

        # normal files
        with (d / 'foo').open('w') as f:
            f.write('foo')

        with (d / 'bar').open('w') as f:
            f.write('bar')

        # symlinks
        (d / 'link-to-foo').symlink_to(d / 'foo')

        # hardlinks
        os.link(d / 'bar', d / 'hardlink-to-bar')

        # directories
        (d / 'dir').mkdir()
        (d / 'dir' / 'foo').mkdir()
        (d / 'dir' / 'foo' / 'bar').mkdir()

        # executables
        (d / 'exe').touch()
        (d / 'exe').chmod(0o755)

        create(d, s3, 'my-bucket', 'password')

    with TemporaryDirectory() as d:
        d = Path(d)

        restore(d, s3, 'my-bucket', 'password')

        # normal files
        assert (d / 'foo').open('r').read() == 'foo'
        assert (d / 'bar').open('r').read() == 'bar'

        # symlinks
        assert (d / 'link-to-foo').is_symlink()
        assert os.readlink(d / 'link-to-foo') == str(d / 'foo')
        assert (d / 'link-to-foo').open('r').read() == 'foo'

        # hardlinks
        assert (d / 'hardlink-to-bar').stat().st_ino\
            == (d / 'bar').stat().st_ino

        # directories
        assert (d / 'dir').is_dir()
        assert (d / 'dir' / 'foo').is_dir()
        assert (d / 'dir' / 'foo' / 'bar').is_dir()

        # executables
        assert (d / 'exe').stat().st_mode & 0o777 == 0o755


def test_restore_complex_hardlinks_case(s3):
    with TemporaryDirectory() as d:
        d = Path(d)

        for child in ('foo', 'bar'):
            (d / child).mkdir()

            with (d / child / 'a').open('wb') as f:
                f.write(b'foo')

            # a <- b <- c
            os.link((d / child / 'a'), (d / child / 'b'))
            os.link((d / child / 'b'), (d / child / 'c'))

            assert (d / child / 'a').stat().st_ino\
                == (d / child / 'b').stat().st_ino\
                == (d / child / 'c').stat().st_ino

        create(d, s3, 'my-bucket', 'password')

    with TemporaryDirectory() as d:
        d = Path(d)

        # we should see two hierarchies with three files sharing a single
        # inode for each directory (no overlap between directories)
        restore(d, s3, 'my-bucket', 'password')
        inodes = set()

        for child in ('foo', 'bar'):
            assert (d / child).is_dir()
            assert (d / child / 'a').open('rb').read() == b'foo'
            assert (d / child / 'b').open('rb').read() == b'foo'
            assert (d / child / 'c').open('rb').read() == b'foo'

            assert (d / child / 'a').stat().st_ino\
                == (d / child / 'b').stat().st_ino\
                == (d / child / 'c').stat().st_ino

            inode = (d / child / 'a').stat().st_ino

            assert inode not in inodes
            inodes.add(inode)


def test_restore_previous_snapshot(s3):

    with TemporaryDirectory() as d:
        d = Path(d)

        with (d / 'foo').open('w') as f:
            f.write('foo')

        create(d, s3, 'my-bucket', 'password')

        collection = SnapshotCollection(Envoy(s3, 'my-bucket', 'password'))
        collection.load()

        assert len(collection.snapshots) == 1

        with (d / 'foo').open('w') as f:
            f.write('bar')

        (d / 'bar').mkdir()

        create(d, s3, 'my-bucket', 'password', keep=2)

        collection.load()
        assert len(collection.snapshots) == 2

    with TemporaryDirectory() as d:
        d = Path(d)

        restore(d, s3, 'my-bucket', 'password', collection.snapshots[0].prefix)

        assert (d / 'foo').open('r').read() == 'foo'
        assert not (d / 'bar').exists()

    with TemporaryDirectory() as d:
        d = Path(d)

        restore(d, s3, 'my-bucket', 'password')

        assert (d / 'foo').open('r').read() == 'bar'
        assert (d / 'bar').exists()


def test_restore_to_non_empty_directory(s3, temporary_path):
    (temporary_path / 'foo').touch()
    create(temporary_path, s3, 'my-bucket', 'password')

    with pytest.raises(TargetPathNotEmpty):
        restore(temporary_path, s3, 'my-bucket', 'password')


def test_validate(s3, temporary_path):
    with TemporaryDirectory() as d:
        d = Path(d)

        with (d / 'foo').open('w') as f:
            f.write('foo')

        with (d / 'foo').open('w') as f:
            f.write('bar')

        create(d, s3, 'my-bucket', 'password')

    assert validate(s3, 'my-bucket', 'password')

    with Envoy(s3, 'my-bucket', 'password') as envoy:
        envoy.delete(next(envoy.prefixes()))

    assert not validate(s3, 'my-bucket', 'password')


def test_force_backup(s3, temporary_path):
    (temporary_path / 'foo').mkdir()
    (temporary_path / 'foo' / 'bar').touch()

    create(temporary_path / 'foo', s3, 'my-bucket', 'password')

    (temporary_path / 'foo' / 'bar').unlink()
    (temporary_path / 'foo').rmdir()

    (temporary_path / 'foo').mkdir()
    (temporary_path / 'foo' / 'bar').touch()

    # the inode has changed
    with pytest.raises(SnapshotMismatchError):
        create(temporary_path / 'foo', s3, 'my-bucket', 'password')

    create(temporary_path / 'foo', s3, 'my-bucket', 'password', force=True)
    create(temporary_path / 'foo', s3, 'my-bucket', 'password')
