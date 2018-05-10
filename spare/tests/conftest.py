import os
import port_for
import pytest
import secrets
import sys
import tempfile

from mirakuru import HTTPExecutor
from pathlib import Path
from spare.envoy import Envoy
from spare.utils import s3_client
from urllib.request import urlretrieve


MINIO_RELEASES = (
    ('darwin', 'https://dl.minio.io/server/minio/release/darwin-amd64/minio'),
    ('linux', 'https://dl.minio.io/server/minio/release/linux-amd64/minio')
)


@pytest.fixture(scope='session')
def access_key():
    return secrets.token_hex(20)


@pytest.fixture(scope='session')
def secret_key():
    return secrets.token_hex(40)


@pytest.fixture(scope='session')
def endpoint(minio):
    return minio.url


@pytest.fixture(scope='function')
def s3(access_key, secret_key, endpoint, minio_path):

    # minio has some concurrency problems with locks which compound if we
    # use the default connection setup which includes lots of retries and
    # big timeouts
    s3 = s3_client(
        endpoint.geturl(),
        access_key,
        secret_key,
        connect_timeout=1,
        read_timeout=1,
        max_attempts=3
    )

    yield s3

    for bucket in s3.buckets.iterator():
        bucket.objects.all().delete()
        bucket.delete()


@pytest.fixture(scope='session')
def minio_release():
    for key, url in MINIO_RELEASES:
        if key in sys.platform:
            return url

    raise RuntimeError(f"{sys.platform} is currently not supported")


@pytest.fixture(scope='session')
def minio_binary(request, minio_release):
    path = Path(request.config.cache.makedir('minio'))
    binary = path / 'minio'

    if not binary.exists():
        urlretrieve(minio_release, binary)
        binary.chmod(0o755)

    return binary


@pytest.fixture(scope='session')
def minio_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture(scope='session')
def minio_port():
    return port_for.select_random()


@pytest.fixture(scope='session')
def minio(minio_binary, minio_path, minio_port, access_key, secret_key):
    os.environ['MINIO_ACCESS_KEY'] = access_key
    os.environ['MINIO_SECRET_KEY'] = secret_key
    os.environ['MINIO_UPDATE'] = 'off'

    address = f'127.0.0.1:{minio_port}'
    command = f'{minio_binary} server --address {address} {minio_path}'
    endpoint = f'http://{address}'

    # for some reason minio gets stuck at times if it prints to stdout
    # or if it is run as a non-shell command
    command += ' > /dev/null'

    executor = HTTPExecutor(command, url=endpoint, status=405, shell=True)
    executor.start()

    yield executor

    executor.stop()


@pytest.fixture(scope='function')
def temporary_path():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture(scope='function')
def envoy(s3):
    with Envoy(s3, 'bucket', 'password') as envoy:
        yield envoy
