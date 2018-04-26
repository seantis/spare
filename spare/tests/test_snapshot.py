import pytest

from io import BytesIO
from spare.errors import FileChangedBeforeUploadError
from spare.errors import SnapshotMismatchError
from spare.errors import PruneToZeroError
from spare.inventory import Inventory
from spare.snapshot import SnapshotCollection


def test_empty_bucket(envoy):
    collection = SnapshotCollection(envoy)
    collection.load()

    assert not collection.snapshots


def test_save(envoy):
    collection = SnapshotCollection(envoy)

    s1 = collection.create()
    s1.save()

    collection.load()
    assert len(collection.snapshots) == 1

    s2 = collection.create()
    s2.save()

    collection.load()
    assert len(collection.snapshots) == 2

    s1.meta['foo'] = 'bar'
    s1.save()

    collection.load()
    assert collection.snapshots[0].meta['foo'] == 'bar'
    assert not collection.snapshots[1].meta


def test_prune(envoy):
    collection = SnapshotCollection(envoy)

    for i in range(0, 3):
        snapshot = collection.create()
        snapshot.meta['id'] = i
        snapshot.save()

    collection.load()
    assert len(collection.snapshots) == 3

    assert collection.snapshots[0].meta['id'] == 0
    assert collection.snapshots[1].meta['id'] == 1
    assert collection.snapshots[2].meta['id'] == 2

    collection.prune(keep=4)
    collection.load()

    assert len(collection.snapshots) == 3

    collection.prune(keep=3)
    collection.load()

    assert len(collection.snapshots) == 3

    collection.prune(keep=2)
    collection.load()

    assert len(collection.snapshots) == 2
    assert collection.snapshots[0].meta['id'] == 1
    assert collection.snapshots[1].meta['id'] == 2

    collection.prune(keep=1)
    collection.load()

    assert len(collection.snapshots) == 1
    assert collection.snapshots[0].meta['id'] == 2

    collection.prune(keep=1)
    collection.load()

    assert len(collection.snapshots) == 1
    assert collection.snapshots[0].meta['id'] == 2

    with pytest.raises(PruneToZeroError):
        collection.prune(keep=0)

    # make sure the internal files are not touched
    assert envoy.locked


def test_change_before_upload(envoy, temporary_path):
    path = temporary_path / 'foo'

    with path.open('w') as f:
        f.write('foo')

    inventory = Inventory(temporary_path)
    inventory.scan()

    with path.open('w') as f:
        f.write('bar')

    with pytest.raises(FileChangedBeforeUploadError):
        snapshot = SnapshotCollection(envoy).create()
        snapshot.backup(inventory)


def test_validate(envoy, temporary_path, loghandler):
    with (temporary_path / 'foo').open('w') as f:
        f.write('foo')

    with (temporary_path / 'bar').open('w') as f:
        f.write('bar')

    inventory = Inventory(temporary_path)
    inventory.scan()

    snapshot = SnapshotCollection(envoy).create()
    snapshot.backup(inventory)

    assert snapshot.validate()

    a, b = snapshot.meta['files']

    # remove one file
    envoy.delete(a)

    # replace another
    envoy.delete(b)
    envoy.send(b, BytesIO(b'baz'))

    # remove something from the structure
    snapshot.meta['structure'].popitem()
    snapshot.save()

    assert not snapshot.validate()

    records = [r for r in loghandler.records if r.level_name == 'ERROR']
    assert len(records) == 3

    log = '\n'.join(l.message for l in records)
    assert 'but got' in log
    assert 'is missing' in log
    assert 'is unknown' in log


def test_owner_mismatch(envoy, temporary_path):
    (temporary_path / 'foo').mkdir()
    (temporary_path / 'bar').mkdir()

    foo = Inventory(temporary_path / 'foo')
    foo.scan()

    bar = Inventory(temporary_path / 'bar')
    bar.scan()

    snapshot = SnapshotCollection(envoy).create()
    snapshot.backup(foo)

    with pytest.raises(SnapshotMismatchError):
        snapshot.backup(bar)

    with pytest.raises(SnapshotMismatchError):
        snapshot = SnapshotCollection(envoy).create()
        snapshot.backup(bar)
