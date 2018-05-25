import grp
import hashlib
import io
import os
import os.path
import platform
import pwd
import re
import stat

from cached_property import cached_property
from collections import defaultdict
from contextlib import suppress
from pathlib import Path
from spare import log
from spare.utils import abort_if_file_changes_during_read, read_in_chunks


def hash_implementation():
    """ We use blake2b with a 32 bytes digest size, which seems to be a good
    compromise between security, performance and short digest size.

    The checksum is not relevant for security, it is simply used to detect
    differences between files.

    """
    return hashlib.blake2b(digest_size=32)


def file_checksum(path):
    """ Generates the checksum for the given path. """

    with abort_if_file_changes_during_read(path):
        m = hash_implementation()

        with open(path, 'rb') as f:
            for chunk in read_in_chunks(f, io.DEFAULT_BUFFER_SIZE):
                m.update(chunk)

        return m.hexdigest()


def scandir(path, recurse=True, follow_symlinks=False):
    """ Runs os.scandir recursively with a coroutine interface.

    The coroutine interface is used to add extra paths to be scanned during
    the execution of the generator.

    """

    unscanned = {path}

    while unscanned:
        with os.scandir(unscanned.pop()) as iterator:
            for e in iterator:
                extra_path = yield e

                if extra_path:
                    unscanned.add(extra_path)

                if recurse and e.is_dir(follow_symlinks=follow_symlinks):
                    unscanned.add(e.path)


class Inventory(object):
    """ Manages metadata related to a local path.

    Every time the `scan` method is called, two dictionaries are created
    by walking the given path.

    The structure dictionary contains all the paths and metadata to the files
    found in the path. For example:

        {
            'pictures': {
                'type': 'directory',
                'user': 'denis',
                'group': 'staff',
                'mode': 41453,
                'size': 0,
                'mtime_ns': 1524208524622212344
            }
            'pictures/001.jpg': {
                'type': 'file',
                'user': 'denis',
                'group': 'staff',
                'mode': 41380,
                'size': 1024,
                'mtime_ns': 1524208524622245644
            },
            'bilder': {
                'type': 'symlink',
                'target': 'pictures',
                'user': 'denis',
                'group': 'staff',
                'mode': 41453,
                'size': 0,
                'mtime_ns': 1524208524622278944
            }
        }

    The structure is used to rebuild the directory tree when restoring a
    remote backup.

    The other dictionary, the data dictionary, contains the hashes of all
    files that we can upload remotely together with their paths. For example:

        {
            '0e5751c026e...': ['pictures/001.jpg']
        }

    As multiple files might have the same hash we can have multiple paths
    for a single hash. The data dictionary is used to figure out what we
    need to upload and what hash the file had at the time of the scan.

    It also acts as a basic way of deduplicate files. That is, files which
    have the same hash will be combined into a single upload and copied
    back when restoring.

    Note that due to the nature of our environment these scans cannot be relied
    on blindly. The structure changes and files are modified while they are
    being read or deleted before we get to process them further down the line.

    In this sense you should think of the inventory as an potentially
    inconsistent snapshot.

    """

    def __init__(self, path, skip=None):
        self.path = Path(path)
        self.structure = {}
        self.files = defaultdict(list)

        if skip:
            skip = (skip, ) if isinstance(skip, str) else skip

            paths = '|'.join(p.lstrip('./') for p in skip)
            paths = re.compile(rf'({paths})')

            self.skip = paths
        else:
            self.skip = None

    @property
    def identity(self):
        return f'{platform.node()}:{self.path}:{self.path.stat().st_ino}'

    @cached_property
    def users(self):
        return {pw.pw_uid: pw.pw_name for pw in pwd.getpwall()}

    @cached_property
    def groups(self):
        return {gr.gr_gid: gr.gr_name for gr in grp.getgrall()}

    def cache_status(self, status):
        return {
            'user': self.users[status.st_uid],
            'group': self.groups[status.st_gid],
            'mode': status.st_mode,
            'size': status.st_size,
            'mtime_ns': status.st_mtime_ns,
            'inode': status.st_ino
        }

    def relative_path(self, path):
        return Path(path).relative_to(self.path)

    def absolute_path(self, path):
        return self.path / path

    def process_dir(self, path, status):
        self.structure[str(self.relative_path(path))] = {
            'type': 'directory',
            **self.cache_status(status)
        }

    def process_symlink(self, path, status, target):
        self.structure[str(self.relative_path(path))] = {
            'type': 'symlink',
            'target': str(self.relative_path(target)),
            **self.cache_status(status)
        }

    def process_file(self, path, status):
        relative_path = str(self.relative_path(path))
        is_empty = status.st_size == 0

        self.structure[relative_path] = {
            'type': 'file',
            'empty': is_empty,
            **self.cache_status(status)
        }

        if not is_empty:
            self.files[file_checksum(path)].append(relative_path)

    def clear(self):
        self.structure.clear()
        self.files.clear()

    def scan(self):
        self.clear()

        if self.path.is_dir():
            self.scan_directory(self.path)
        else:
            self.scan_entry(self.path)

    def scan_directory(self, path, recurse=True):
        scanner = scandir(path, recurse)
        extra = None

        with suppress(StopIteration):
            while True:
                entry = scanner.send(extra)
                extra = self.scan_entry(entry)

    def scan_entry(self, entry):

        if self.skip and self.skip.match(str(self.relative_path(entry))):
            return None

        if isinstance(entry, os.DirEntry):
            status = entry.stat(follow_symlinks=False)
        else:
            status = entry.stat()

        path = entry.__fspath__()
        scan_also = None

        if stat.S_ISCHR(status.st_mode):  # pragma: no cover
            log.warn(f"Skipping character special device {path}")

        # we can't cover this on Travis currently as we don't have access to
        # a block device and can't create one without sudo
        elif stat.S_ISBLK(status.st_mode):  # pragma: no cover
            log.warn(f"Skipping block special device {path}")

        elif stat.S_ISFIFO(status.st_mode):
            log.warn(f"Skipping named pipe {path}")

        elif stat.S_ISSOCK(status.st_mode):
            log.warn(f"Skipping socket {path}")

        elif entry.is_symlink():
            target = Path(os.readlink(path))

            if not target.exists():
                log.warn(f"Skipping broken symlink {path}")

            elif self.path not in target.parents:
                if target.is_dir():
                    log.warn(f"Processing symlink {path} as a directory")
                    self.process_dir(path, status)

                    scan_also = path
                else:
                    log.warn(f"Processing symlink {path} as a file")
                    self.process_file(path, status)
            else:
                self.process_symlink(path, status, target)

        elif entry.is_dir():
            self.process_dir(path, status)

        elif entry.is_file():
            self.process_file(path, status)

        else:
            raise NotImplementedError  # pragma: no cover

        return scan_also
