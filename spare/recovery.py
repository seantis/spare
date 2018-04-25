import os
import grp
import pwd

from cached_property import cached_property
from itertools import groupby
from pathlib import Path
from spare import log, FOLLOW_SYMLINKS
from spare.errors import TargetPathNotEmpty
from spare.download import Download, DownloadManager


class Recovery(object):
    """ Responsible for the recovery of snapshots to any given path.

    Whereas our backing up procedures try to be conservative with cpu and
    memory usage, this module tries to maximise recovery speed. The assumption
    is that recovery happens on a system that is otherwise unused.

    """

    def __init__(self, envoy, snapshot):
        self.envoy = envoy
        self.snapshot = snapshot

    @cached_property
    def uids(self):
        return {pw.pw_name: pw.pw_uid for pw in pwd.getpwall()}

    @cached_property
    def gids(self):
        return {gr.gr_name: gr.gr_gid for gr in grp.getgrall()}

    def restore(self, target):
        log.info(f"Restoring {target}")

        target = Path(target)
        target.mkdir(exist_ok=True)

        for path in target.iterdir():
            raise TargetPathNotEmpty(path)

        log.info(f"Restoring folder structure of {target}")
        self.restore_structure(target)

        log.info(f"Downloading data for {target}")
        self.download_data(target)

        log.info(f"Restored {target}")

    def restore_structure(self, target):
        structure = self.snapshot.meta['structure']

        for path in sorted(structure):
            status = structure[path]
            path = target / path

            if status['type'] == 'directory':
                path.mkdir()

            elif status['type'] == 'symlink':
                path.symlink_to(target / status['target'])

            elif status['type'] == 'file':
                path.touch()

            else:
                raise NotImplementedError  # pragma: no cover

            uid = self.uids[status['user']]
            gid = self.gids[status['group']]

            os.chmod(path, status['mode'], **FOLLOW_SYMLINKS)
            os.chown(path, uid, gid, **FOLLOW_SYMLINKS)

    def download_data(self, target):
        structure = self.snapshot.meta['structure']

        def inode(path):
            return structure[path]['inode']

        def by_inode(paths):
            return groupby(sorted(paths, key=inode), key=inode)

        with DownloadManager(self.envoy) as download_manager:
            for digest, paths in self.snapshot.meta['files'].items():
                download = Download(prefix=digest, digest=digest)

                log.info(f"Downloading {paths[0]}")

                for _, paths in by_inode(paths):
                    path, *rest = (target / p for p in paths)
                    download.to(path, hardlinks=rest)

                download_manager.queue(download)
