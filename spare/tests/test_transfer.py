def test_access(s3):
    assert not tuple(s3.buckets.iterator())
    s3.create_bucket(Bucket='foobar')
    assert tuple(s3.buckets.iterator())


def test_buckets_cleared_between_tests(s3):
    assert not tuple(s3.buckets.iterator())
