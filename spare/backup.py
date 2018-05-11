from spare import log
from spare.envoy import Envoy
from spare.inventory import Inventory
from spare.recovery import Recovery
from spare.snapshot import SnapshotCollection


def create(path, s3, bucket, password, keep=1, skip=None, force=False):
    inventory = Inventory(path, skip)
    inventory.scan()

    with Envoy(s3, bucket, password) as envoy:

        collection = SnapshotCollection(envoy)
        collection.load()

        snapshot = collection.create()
        snapshot.backup(inventory, force=force)

        collection.prune(keep=keep)


def restore(path, s3, bucket, password, snapshot='latest'):
    with Envoy(s3, bucket, password) as envoy:

        collection = SnapshotCollection(envoy)
        collection.load()

        snapshot = collection.get(snapshot)

        recovery = Recovery(envoy, snapshot)
        recovery.restore(target=path)


def validate(s3, bucket, password):
    with Envoy(s3, bucket, password) as envoy:

        collection = SnapshotCollection(envoy)
        collection.load()

        valid = True

        for snapshot in collection.snapshots:
            if snapshot.validate():
                log.info(f"{snapshot.prefix} is valid")
            else:
                log.error(f"{snapshot.prefix} has errors")
                valid = False

        return valid
