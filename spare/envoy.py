import re
import secrets

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from spare.block import DEFAULT_BLOCK, AVAILABLE_BLOCKS
from spare.errors import BucketAlreadyLockedError
from spare.errors import ExistingPrefixError
from spare.errors import InvalidPrefixError
from spare.utils import read_in_chunks


class Envoy(object):
    """ Provides the bridge between an unencrypted local file and a remote file
    encrypted in chunks.

    The keys used for each chunks are of the following format:

        prefix/000000001-nonce

    For example:

        my-file/000000001-c3f543e56704af2ca4779a7d530836cc
        my-file/000000002-4489c3d7ff0e090ad1a1260efa2f5084

    As the number indicates, we are limited to 999'999'999 blocks of 1 MiB
    each, so the largest file we can accept after compression and encryption
    is 953.674 Tebibytes.

    """

    # valid prefixes are limited to a-z A-Z 0-9 and underscore. It may not
    # begin with a dot (reserved for envoy-internal files) and it must
    # be at least two characters long
    valid_prefix = re.compile(r'^[a-zA-Z0-9_]{1}[a-zA-Z0-9_\.]+$')

    def __init__(self, s3, bucket, password, block=DEFAULT_BLOCK):
        self.s3 = s3
        self.bucket = s3.Bucket(bucket)
        self.bucket_name = bucket
        self.password = password
        self.block_class = AVAILABLE_BLOCKS[DEFAULT_BLOCK]

        # you should not change these values outside of unit tests!
        self.blocksize = 1048576  # 1 MiB
        self.noncesize = 16

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, *args):
        self.unlock()

    def lock(self):
        self.ensure_bucket_exists()

        if self.locked:
            raise BucketAlreadyLockedError(self.bucket_name)

        with BytesIO() as f:
            self.bucket.upload_fileobj(f, '.lock')

    @property
    def locked(self):
        return self.is_known_prefix('.lock')

    def unlock(self):
        if self.is_known_prefix('.lock'):
            self.bucket.objects.filter(Prefix='.lock').delete()

    def ensure_bucket_exists(self):
        if not self.bucket.creation_date:
            self.bucket.create()

    def ensure_prefix_unknown(self, prefix):
        if self.is_known_prefix(prefix):
            raise ExistingPrefixError(prefix)

    def ensure_valid_prefix(self, prefix):
        if not prefix or not self.valid_prefix.match(prefix):
            raise InvalidPrefixError(prefix)

    def spawn_block(self, nonce, data):
        return self.block_class(
            password=self.password.encode('utf-8'),
            nonce=nonce.encode('utf-8'),
            data=data
        )

    def generate_nonce(self):
        return secrets.token_hex(self.noncesize)

    def extract_nonce(self, key):
        return key.split('-')[-1]

    def extract_prefix(self, key):
        return key.split('/')[0]

    def is_first_block(self, key):
        return '1-' in key

    def is_known_prefix(self, prefix):
        for obj in self.bucket.objects.filter(Prefix=prefix, MaxKeys=1):
            return True

        return False

    def keys(self, prefix=None):
        for obj in self.bucket.objects.filter(Prefix=prefix or ''):
            if not obj.key.startswith('.'):
                yield obj.key

    def prefixes(self, prefix=None):
        for key in self.keys(prefix=prefix):
            if self.is_first_block(key):
                yield self.extract_prefix(key)

    def delete(self, prefix):
        self.ensure_valid_prefix(prefix)
        self.bucket.objects.filter(Prefix=prefix).delete()

    def send(self, prefix, fileobj, before_encrypt=None):
        self.ensure_bucket_exists()
        self.ensure_valid_prefix(prefix)
        self.ensure_prefix_unknown(prefix)

        chunks = read_in_chunks(fileobj, self.blocksize)

        # uploading chunks in threads makes little difference in memory and cpu
        # usage, but it does speed up large downloads by 20%
        #
        # further up the stack threads are not much more effective, but they
        # use much more cpu/memory
        #
        # maybe having some kind of pipeline of all chunks (not just per file
        # would improve things further, but for now this seems reasonable
        # enough)
        def upload_block(name, block):
            with BytesIO(block.data) as buffer:
                self.bucket.upload_fileobj(buffer, name)

        with ThreadPoolExecutor() as executor:
            for n, chunk in enumerate(chunks, start=1):
                nonce = self.generate_nonce()
                block = self.spawn_block(nonce, chunk)

                if before_encrypt is not None:
                    before_encrypt(chunk)

                block.encrypt()
                block_name = f'{prefix}/{n:0>9}-{nonce}'

                executor.submit(upload_block, block_name, block)

    def recv(self, prefix, fileobj, after_decrypt=None):
        self.ensure_valid_prefix(prefix)

        for obj in self.bucket.objects.filter(Prefix=prefix):
            nonce = self.extract_nonce(obj.key)

            with BytesIO() as buffer:
                self.bucket.download_fileobj(obj.key, buffer)

                buffer.seek(0)
                block = self.spawn_block(nonce, buffer.read())

            block.decrypt()

            if after_decrypt is not None:
                after_decrypt(block.data)

            fileobj.write(block.data)