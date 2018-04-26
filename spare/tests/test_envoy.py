import secrets
import pytest

from io import BytesIO
from spare.block import AVAILABLE_BLOCKS
from spare.envoy import Envoy
from spare.errors import BucketAlreadyLockedError
from spare.errors import BucketNotLockedError
from spare.errors import BucketOtherwiseUsedError
from spare.errors import ExistingPrefixError
from spare.errors import InvalidPrefixError


@pytest.mark.parametrize('block', AVAILABLE_BLOCKS)
def test_single_block(s3, block):
    with Envoy(s3, 'my-bucket', 'my-password', block) as envoy:
        envoy.send('document', BytesIO(b'secret'))

        blocks = tuple(envoy.bucket.objects.filter(Prefix='document'))

        assert len(blocks) == 1
        assert blocks[0].get()['Body'].read() != b'secret'

        output = BytesIO()
        envoy.recv('document', output)

        output.seek(0)
        assert output.read() == b'secret'


@pytest.mark.parametrize('block', AVAILABLE_BLOCKS)
def test_multiple_blocks(s3, block):
    with Envoy(s3, 'my-bucket', 'my-password', block) as envoy:
        envoy.blocksize = 1024

        data = secrets.token_bytes(1024 * 2)
        envoy.send('document', BytesIO(data))

        blocks = tuple(envoy.bucket.objects.filter(Prefix='document'))

        assert len(blocks) == 2

        output = BytesIO()
        envoy.recv('document', output)

        output.seek(0)
        assert output.read() == data


def test_replace(s3):
    with Envoy(s3, 'my-bucket', 'my-password') as envoy:
        envoy.blocksize = 1024

        initial = secrets.token_bytes(1024 * 2)
        replacement = secrets.token_bytes(1024 * 1)

        envoy.send('document', BytesIO(initial))
        assert sum(1 for k in envoy.keys()) == 2

        with pytest.raises(ExistingPrefixError):
            envoy.send('document', BytesIO(replacement))
            assert sum(1 for k in envoy.keys()) == 1

        envoy.delete('document')
        envoy.send('document', BytesIO(replacement))

        output = BytesIO()
        envoy.recv('document', output)

        output.seek(0)
        assert output.read() == replacement


def test_delete(s3):
    with Envoy(s3, 'my-bucket', 'my-password') as envoy:
        envoy.send('foo', BytesIO(b'foo'))
        envoy.send('bar', BytesIO(b'bar'))
        assert sum(1 for k in envoy.keys()) == 2

        envoy.delete('bar')
        assert sum(1 for k in envoy.keys()) == 1

        with BytesIO() as output:
            envoy.recv('foo', output)
            assert output.getvalue() == b'foo'


def test_invalid_prefix(s3):
    with Envoy(s3, 'bucket', 'password') as envoy:
        # no dashes
        with pytest.raises(InvalidPrefixError):
            envoy.send('no-dashes', BytesIO())

        # at least 2 characters long
        with pytest.raises(InvalidPrefixError):
            envoy.send('x', BytesIO())

        # no dots
        with pytest.raises(InvalidPrefixError):
            envoy.send('.hidden', BytesIO())


def test_prefixes(s3):
    with Envoy(s3, 'bucket', 'password') as envoy:
        envoy.blocksize = 1024

        envoy.ensure_bucket_exists()
        assert not next(envoy.prefixes(), None)

        data = secrets.token_bytes(1024 * 2)
        envoy.send('foo', BytesIO(data))

        assert sum(1 for k in envoy.keys()) == 2
        assert tuple(envoy.prefixes()) == ('foo', )


def test_lock(s3):
    with Envoy(s3, 'bucket', 'password'):
        pass

    with Envoy(s3, 'bucket', 'password'):
        pass

    with pytest.raises(BucketAlreadyLockedError):
        with Envoy(s3, 'bucket', 'password'):
            with Envoy(s3, 'bucket', 'password'):
                pass


def test_lock_enforced(s3):
    with Envoy(s3, 'bucket', 'password'):
        pass

    with pytest.raises(BucketNotLockedError):
        Envoy(s3, 'bucket', 'password').send('foo', BytesIO())


def test_envoy_fail_on_foreign_buckets(s3):
    bucket = Envoy(s3, 'bucket', 'password').bucket
    bucket.create()
    bucket.upload_fileobj(BytesIO(b'foo'), 'bar')

    with pytest.raises(BucketOtherwiseUsedError):
        with Envoy(s3, 'bucket', 'password'):
            pass
