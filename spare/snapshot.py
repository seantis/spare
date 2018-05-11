import json
import ulid

from io import BytesIO
from spare import log
from spare.errors import FileChangedBeforeUploadError
from spare.errors import PruneToZeroError
from spare.errors import SnapshotMismatchError
from spare.inventory import hash_implementation
from spare.utils import abort_if_file_changes_during_read


class SnapshotCollection(object):
    """ Manages the snapshots stored on a bucket.

    Snapshots are created using 'ulid' to automatically keep them in order
    of creation (from oldest to newest).

    Snapshots store the structure of the inventory they backed up as well
    as all the file-digests they are linked with. This ends up working
    a bit like reference counting - as long as a snapshot links to an
    uploaded file, that file is kept around.

    When snapshots are deleted/pruned, all the files which are no longer
    referenced by any snapshot get deleted.

    """

    def __init__(self, envoy):
        self.envoy = envoy
        self.clear()

    def clear(self):
        self.snapshots = []

    def load(self):
        """ Load all snapshots from the bucket. """

        self.clear()

        for prefix in sorted(self.envoy.prefixes('snapshot')):
            self.snapshots.append(Snapshot.load(self.envoy, prefix))

    def create(self):
        return Snapshot(self.envoy)

    def get(self, prefix='latest'):
        """ Gets the given snapshot by prefix or the latest one (default). """

        if prefix == 'latest':
            return self.snapshots[-1]

        return next((s for s in self.snapshots if s.prefix == prefix), None)

    def prune(self, keep=1):
        """ Prunes the snapshots and all the files associated with them. """

        # make sure we are up to date
        self.load()

        if keep < 1:
            raise PruneToZeroError(len(self.snapshots))

        if len(self.snapshots) <= keep:
            return

        # delete the meatdata of old snapshots
        old, self.snapshots = self.snapshots[:-keep], self.snapshots[-keep:]

        for snapshot in old:
            snapshot.delete()

        # delete the prefixes we don't recognise
        known = set()

        for snapshot in self.snapshots:
            known.add(snapshot.prefix)
            known.update(d for d in snapshot.meta.get('files', ()))

        for prefix in self.envoy.prefixes():
            if prefix not in known:
                self.envoy.delete(prefix)


class Snapshot(object):
    """ A single snapshot, responsible for backing up inventories. """

    def __init__(self, envoy, prefix=None, meta=None):
        self.envoy = envoy
        self.prefix = prefix or f'snapshot_{ulid.new()}'
        self.meta = meta or {}

    @classmethod
    def load(cls, envoy, prefix):
        with BytesIO() as buffer:
            envoy.recv(prefix, buffer)
            meta = json.loads(buffer.getvalue().decode('utf-8'))

        return cls(envoy, prefix, meta)

    @property
    def neighbours(self):
        collection = SnapshotCollection(self.envoy)
        collection.load()

        for snapshot in collection.snapshots:
            if snapshot.prefix != self.prefix:
                yield snapshot

    def ensure_identity_match(self, inventory):
        """ Each inventory we backup has an identity associated with it
        (hostname + path). When creating a new snapshot we ensure that
        this identity matches, because we want each hostname/path combination
        to be stored in a separate bucket.

        """
        if 'identity' in self.meta:
            if self.meta['identity'] != inventory.identity:
                raise SnapshotMismatchError(
                    expected=inventory.identity,
                    found=self.meta['identity']
                )

        for snapshot in self.neighbours:
            if snapshot.meta['identity'] != inventory.identity:
                raise SnapshotMismatchError(
                    expected=inventory.identity,
                    found=snapshot.meta['identity']
                )

    def backup(self, inventory, force=False):
        """ Backup the given inventory. """

        log.info(f"Backing up {inventory.path}")

        force or self.ensure_identity_match(inventory)
        uploaded = set(self.envoy.prefixes())

        for digest, paths in inventory.files.items():
            if digest in uploaded:
                continue

            for path in paths:
                log.info(f"Uploading {path}")

            path = inventory.absolute_path(paths[0])
            m = hash_implementation()

            with abort_if_file_changes_during_read(path):
                with open(path, 'rb') as f:
                    self.envoy.send(digest, f, before_encrypt=m.update)

            if digest != m.hexdigest():
                raise FileChangedBeforeUploadError(path)

        self.meta['files'] = inventory.files
        self.meta['structure'] = inventory.structure
        self.meta['identity'] = inventory.identity

        self.save()

        log.info(f"Completed {self.prefix}")

    def save(self):
        """ Save the snapshot metadata to the bucket. """

        if self.envoy.is_known_prefix(self.prefix):
            self.envoy.delete(self.prefix)

        with BytesIO() as buffer:
            buffer.write(json.dumps(self.meta).encode('utf-8'))
            self.envoy.send(self.prefix, buffer)

    def delete(self):
        """ Deletes the snapshot data, but *not* the associated data. The
        latter is done by the `prune` call in the `SnapshotCollection` class.

        """
        self.envoy.delete(self.prefix)

    def validate(self):
        """ Validates the snapshot data by downloading and validating the
        checksum of all the files in the envoy.

        Returns true if the snapshot is valid. Errors are added to the log.

        """

        class NullBuffer(object):
            def write(self, bytes):
                pass

        prefixes = set(self.envoy.prefixes())

        null = NullBuffer()
        success = True

        def fail(message):
            nonlocal success
            success = False
            log.error(message)

        for digest in self.meta.get('files', ()):

            if digest not in prefixes:
                fail(f"{digest} is unknown")

            else:
                m = hash_implementation()
                self.envoy.recv(digest, null, after_decrypt=m.update)

                if digest != m.hexdigest():
                    fail(f"Expected {digest} but got {m.hexdigest()}")

            for path in self.meta['files'][digest]:
                if path not in self.meta['structure']:
                    fail(f"the metadata for {path} is missing")

        return success
