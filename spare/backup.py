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


def lock(s3, bucket, password):
    envoy = Envoy(s3, bucket, password)
    envoy.ensure_bucket_exists()
    envoy.ensure_bucket_is_ours()

    if not envoy.locked:
        envoy.lock()
        return True

    return False


def unlock(s3, bucket, password):
    envoy = Envoy(s3, bucket, password)
    envoy.ensure_bucket_exists()
    envoy.ensure_bucket_is_ours()

    if envoy.locked:
        envoy.unlock()
        return True

    return False
