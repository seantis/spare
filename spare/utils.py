from contextlib import contextmanager
from pathlib import Path
from spare.errors import FileChangedDuringReadError


def read_in_chunks(fileobj, chunksize):
    fileobj.seek(0)

    while True:
        chunk = fileobj.read(chunksize)

        if chunk:
            yield chunk
        else:
            break


@contextmanager
def abort_if_file_changes_during_read(path):
    before = Path(path).stat()
    yield
    after = Path(path).stat()

    for attr in ('st_mtime_ns', 'st_size', 'st_ino', 'st_dev'):
        if getattr(before, attr) != getattr(after, attr):
            raise FileChangedDuringReadError(path)
