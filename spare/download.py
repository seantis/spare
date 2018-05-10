import os
import random
import shutil

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process
from spare import log, FOLLOW_SYMLINKS
from spare.envoy import Envoy
from spare.inventory import hash_implementation
from spare.utils import s3_client, writable
from time import sleep


class Download(object):
    """ Represents a single download.

    Locally duplicated files are only stored once on the remote and we want
    to only download them once as well.

    Therefore we use a single prefix and n targets. Each target is a list of
    1-n paths. The first path is a copy of the original digest, all the other
    paths of the same target ar hardlinks to the copy of the original digest.

    For example, say we have this folder structure where all the files
    have the same content and the files in each folder have the same inode:

    / foo / a (inode 100)
    / foo / b (inode 100)
    / foo / c (inode 100)

    / bar / a (inode 200)
    / bar / b (inode 200)
    / bar / c (inode 200)

    This should result in a single download of this form:

    Download(prefix='xxx', targets=[
        ('/foo/a', '/foo/b', '/foo/c'),
        ('/bar/a', '/bar/b', '/bar/c')
    ])

    This in turn will lead to a download to /foo/a, hardlinks to /foo/a from
    /foo/b /foo/c). As well as a copy of /foo/a to /bar/a and hardlinks from
    /bar/b and /bar/c to /bar/a.

    """

    __slots__ = ('prefix', 'digest', 'targets')

    def __init__(self, prefix, digest):
        self.prefix = prefix
        self.digest = digest
        self.targets = []

    def to(self, path, hardlinks=()):
        self.targets.append((path, *hardlinks))


class DownloadProcess(Process):
    """ Runs a download manager in a separate process and queues downloads
    using a DownloadManager in multiple threads.

    """

    def __init__(self, endpoint, access_key, secret_key,
                 bucket, password, queue):

        Process.__init__(self)

        self.queue = queue
        self.s3 = s3_client(endpoint, access_key, secret_key)
        self.envoy = Envoy(self.s3, bucket, password)

    def run(self):
        with DownloadManager(self.envoy) as manager:
            while True:
                download = self.queue.get()

                if not download:
                    break

                manager.queue(download)

                # sleep randomly to ensure that multiple processes
                # read equally from the shared queue
                sleep(random.uniform(0, 0.1))


class DownloadManager(object):
    """ Takes download objects and downloads them using a threadpool executor.

    In addition the download targets are realised after the download (incl.
    copies and hardlinks).

    Since the download manager works with threads it should be used with
    a with clause:

        with DownloadManager(envoy) as manager:
            download = Download('prefix', 'digest')
            download.to(path, hardlinks)
            download.to(path, hardlinks)

            manager.queue(download)

    """

    def __init__(self, envoy):
        self.envoy = envoy
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.results = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        for future in self.results:
            future.result()

        self.executor.shutdown(wait=True)

    def queue(self, download):
        self.results.append(self.executor.submit(self.fetch, download))

    def fetch(self, download):
        m = hash_implementation()
        genesis = download.targets[0][0]

        with writable(genesis):
            with open(genesis, 'wb') as f:
                self.envoy.recv(download.prefix, f, after_decrypt=m.update)

        # the following branch is actually covered, but coverage.py does not
        # consider it as such - I've tried all the tricks in the book and it
        # will still not capture it, so 'no cover' it is
        if download.digest != m.hexdigest():  # pragma: no cover
            paths = (p for paths in download.targets for p in paths)

            for path in paths:
                log.error((
                    f"Unexpected checksum for {path}, "
                    f"expected {download.digest}, "
                    f"got {m.hexdigest()}"
                ))

        for paths in download.targets:
            clone, *links = paths

            if clone != genesis:
                shutil.copyfile(genesis, clone, **FOLLOW_SYMLINKS)
                shutil.copystat(genesis, clone, **FOLLOW_SYMLINKS)

            for link in links:
                # files are touched during structure creation, which is a
                # problem for hard links
                os.unlink(link)

                os.link(clone, link, **FOLLOW_SYMLINKS)
