from io import BytesIO
from spare.download import Download, DownloadManager


def test_digestless_download(envoy, temporary_path):
    envoy.send('foo', BytesIO(b'foo'))

    with DownloadManager(envoy) as manager:
        download = Download('foo', 'wrong-digest')
        download.to(temporary_path / 'foo')

        manager.queue(download)

    assert (temporary_path / 'foo').open('rb').read() == b'foo'


def test_wrong_digest_download(envoy, temporary_path, loghandler):
    envoy.send('foo', BytesIO(b'foo'))

    with DownloadManager(envoy) as manager:
        download = Download('foo', 'wrong-digest')
        download.to(temporary_path / 'foo')

        manager.queue(download)

    # the download will still work
    assert (temporary_path / 'foo').open('rb').read() == b'foo'

    # but we get an error int he logs
    assert "Unexpected checksum for" in loghandler.records[0].message
