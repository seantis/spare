import re
import secrets

from boto3.s3.transfer import TransferConfig
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from spare.block import DEFAULT_BLOCK, AVAILABLE_BLOCKS
from spare.errors import BucketAlreadyLockedError
from spare.errors import BucketNotLockedError
from spare.errors import BucketOtherwiseUsedError
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
        self.blocksize = 1_048_576  # found to be good value through testing
        self.noncesize = 16

        # as long as an envoy is locked, we have exclusive access and can
        # therefore keep track of the known prefixes ourselves
        self._known_prefixes = None

        # disable threads as we manage our own
        self.transfer_config = TransferConfig(use_threads=False)

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, *args):
        self.unlock()

    def lock(self):
        self.ensure_bucket_exists()
        self.ensure_bucket_is_ours()

        if self.locked:
            raise BucketAlreadyLockedError(self.bucket_name)

        self._known_prefixes = set(self.prefixes())

        with BytesIO() as f:
            self.bucket.upload_fileobj(f, '.lock')
            self.on_store_prefix('.lock')

    @property
    def locked(self):
        return self.is_known_prefix('.lock')

    def unlock(self):
        if self.is_known_prefix('.lock'):
            self.bucket.objects.filter(Prefix='.lock').delete()
            self.on_delete_prefix('.lock')
            self._known_prefixes = None

    def ensure_locked(self):
        if not self.locked:
            raise BucketNotLockedError(self.bucket_name)

    def ensure_bucket_exists(self):
        if not self.bucket.creation_date:
            self.bucket.create()

            with BytesIO(b'https://github.com/seantis/spare') as f:
                self.bucket.upload_fileobj(f, '.spare')

    def ensure_bucket_is_ours(self):
        if not self.is_known_prefix('.spare'):
            raise BucketOtherwiseUsedError(self.bucket_name)

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
        if self._known_prefixes is not None:
            return prefix in self._known_prefixes

        for obj in self.bucket.objects.filter(Prefix=prefix, MaxKeys=1):
            return True

        return False

    def on_store_prefix(self, prefix):
        if self._known_prefixes is not None:
            self._known_prefixes.add(prefix)

    def on_delete_prefix(self, prefix):
        if self._known_prefixes is not None:
            self._known_prefixes.discard(prefix)

    def keys(self, prefix=None):
        for obj in self.bucket.objects.filter(Prefix=prefix or ''):
            if not obj.key.startswith('.'):
                yield obj.key

    def prefixes(self, prefix=None):
        for key in self.keys(prefix=prefix):
            if self.is_first_block(key):
                yield self.extract_prefix(key)

    def delete(self, prefix):
        self.ensure_locked()
        self.ensure_valid_prefix(prefix)
        self.bucket.objects.filter(Prefix=prefix).delete()
        self.on_delete_prefix(prefix)

    def send(self, prefix, fileobj, before_encrypt=None):
        self.ensure_locked()
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
                self.bucket.upload_fileobj(
                    buffer, name, Config=self.transfer_config)

        with ThreadPoolExecutor() as executor:
            for n, chunk in enumerate(chunks, start=1):
                nonce = self.generate_nonce()
                block = self.spawn_block(nonce, chunk)

                if before_encrypt is not None:
                    before_encrypt(chunk)

                block.encrypt()
                block_name = f'{prefix}/{n:0>9}-{nonce}'

                executor.submit(upload_block, block_name, block)

        self.on_store_prefix(prefix)

    def recv(self, prefix, fileobj, after_decrypt=None):
        self.ensure_valid_prefix(prefix)

        for obj in self.bucket.objects.filter(Prefix=prefix):
            nonce = self.extract_nonce(obj.key)

            with BytesIO() as buffer:
                self.bucket.download_fileobj(
                    obj.key, buffer, Config=self.transfer_config)

                buffer.seek(0)
                block = self.spawn_block(nonce, buffer.read())

            block.decrypt()

            if after_decrypt is not None:
                after_decrypt(block.data)

            fileobj.write(block.data)
