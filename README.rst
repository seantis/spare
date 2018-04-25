Spare
=====

One S3 backup, encrypted on the fly.

Description
-----------

This tool is used at Seantis to create backups stored on various S3 compatible
services using a very limited subset of S3 commands.

This is what you need to know:

* Spare should not be used in production yet! Use at your own risk.

* Spare stores exactly one copy of your data.

* Files are compressed using LZMA and encrypted on the client using AES-SIV.

* During upload, spare is limited to one core and less than 100MiB of memory.

* Duplicated files are stored only once.

* If you forget your password, you cannot restore your files.

CLI
---

To set the connection parameters:

    export SPARE_ENDPOINT=host
    export SPARE_ACCESS_KEY=access-key
    export SPARE_SECRET_KEY=secret-key

To backup a folder:

    spare create --password my-password --bucket my-bucket --path /my-path

To restore a backup:

    spare restore --password my-password --bucket my-bucket --path /my-path

To verify the backup (downloads everything!):

    spare verify --password my-password --bucket my-bucket --path /my-path

Run the Tests
-------------

Spare uses `Minio <https://www.minio.io>` to run tests against a real object
storage server. As a result it can take a bit for the first test run to
complete, as Minio is downloaded and stored in the pytest cache directory.

    pip install -e '.[test]'
    py.test

Build Status
------------

.. image:: https://travis-ci.org/seantis/spare.png
  :target: https://travis-ci.org/seantis/spare
  :alt: Build Status

License
-------
spare is released under the MIT license
