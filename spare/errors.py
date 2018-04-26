class SpareError(Exception):
    pass


class FileChangedDuringReadError(SpareError):
    """ Raised when a file has changed while it was read. """

    def __init__(self, path):
        self.path = path


class FileChangedBeforeUploadError(SpareError):
    """ Raised when a file changed after it was inventoried, but before
    it was uploaded.

    """

    def __init__(self, path):
        self.path = path


class PruneToZeroError(SpareError):
    """ Raised when trying to prune a bucket in a way that would not leave
    any snapshots behind.

    """

    def __init__(self, snapshots_left):
        self.snapshots_left = snapshots_left


class InvalidPrefixError(SpareError):
    """ Raised when a given prefix is invalid. """

    def __init__(self, prefix):
        self.prefix = prefix


class ExistingPrefixError(SpareError):
    """ Raised when a given prefix exists already. """

    def __init__(self, prefix):
        self.prefix = prefix


class BucketAlreadyLockedError(SpareError):
    """ Raised when bucket is already locked. """

    def __init__(self, bucket):
        self.bucket = bucket


class TargetPathNotEmpty(SpareError):
    """ Raised when a restore target path is not empty. """

    def __init__(self, path):
        self.path = path


class BucketNotLockedError(SpareError):
    """ Raised when a bucket was expected to be locked, but was not. """

    def __init__(self, bucket):
        self.bucket = bucket


class SnapshotMismatchError(SpareError):
    """ Raised when snapshot is attempted on a bucket which has snapshots
    from a different host/path on it.

    """

    def __init__(self, expected, found):
        self.expected = expected
        self.found = found


class BucketOtherwiseUsedError(SpareError):
    """ Raised when the bucket is not managed by spare. """

    def __init__(self, bucket):
        self.bucket = bucket
