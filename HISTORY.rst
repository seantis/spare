Changelog
---------

0.4.0 (2019-07-29)
~~~~~~~~~~~~~~~~~~~~~

- Improves handling of SIGINT/SIGTERM.

  It should now be safe to kill Spare during backup runs. Spare will still
  block until the current object is properly uploaded, but it won't try to
  finish the snapshot.

  The uploaded files might of course be discarded, but the bucket should be
  unlocked and the uploaded files should either be fully uploaded, or not at
  all.
  [href]

0.3.0 (2019-05-28)
~~~~~~~~~~~~~~~~~~~~~

- Adds a lock/unlock commands.
  [href]

0.2.6 (2019-05-09)
~~~~~~~~~~~~~~~~~~~~~

- Fixes tests failing on Travis, again.
  [href]

0.2.5 (2019-05-09)
~~~~~~~~~~~~~~~~~~~~~

- Fixes tests failing on Travis.
  [href]

0.2.4 (2019-05-09)
~~~~~~~~~~~~~~~~~~~~~

- Stops showing errors when files go missing during operation.
  [href]

0.2.3 (2018-06-20)
~~~~~~~~~~~~~~~~~~~~~

- Fixes large snapshots getting pruned after creation.
  [href]

0.2.2 (2018-05-25)
~~~~~~~~~~~~~~~~~~~~~

- Fixes tests.
  [href]

0.2.1 (2018-05-25)
~~~~~~~~~~~~~~~~~~~~~

- Delay sigterm handling during backup and restore operations.
  [href]

- Changes '--force' into a flag.
  [href]

0.2.0 (2018-05-11)
~~~~~~~~~~~~~~~~~~~~~

- Adds the inode to the snapshot identity to ensure that a newly setup host
  doesn't overwrite existing backups.
  [href]

0.1.3 (2018-05-10)
~~~~~~~~~~~~~~~~~~~~~

- Adds the ability to exclude files from the backup.
  [href]

0.1.2 (2018-05-10)
~~~~~~~~~~~~~~~~~~~~~

- Increases the download speed during restore operations.
  [href]

- Shows a warning when a user/group could not be found during restore.
  [href]

- Lowers the timeouts and retry rates.
  [href]

0.1.1 (2018-05-04)
~~~~~~~~~~~~~~~~~~~~~

- Lowers the number of requests needed to upload data.
  [href]

0.1.0 (2018-04-26)
~~~~~~~~~~~~~~~~~~~~~

- Initial Release.
  [href]
