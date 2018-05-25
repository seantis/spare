import click
import pdb
import sys
import traceback

from logbook import StreamHandler
from signal import SIGTERM
from spare.backup import create, restore, validate
from spare.utils import delay_signal, s3_client


VALID_PATH = click.Path(exists=True, file_okay=False)


def enable_post_mortem_debugging():  # pragma: no cover

    def hook(type, value, tb):
        if hasattr(sys, 'ps1') or not sys.stderr.isatty():
            sys.__excepthook__(type, value, tb)
        else:
            traceback.print_exception(type, value, tb)
            pdb.post_mortem(tb)

    sys.excepthook = hook


@click.group()
@click.option('--pdb', help="Enable post-mortem debugging", is_flag=True)
@click.option('--verbose', help="Print log messages to stdout", is_flag=True)
@click.pass_context
def cli(ctx, pdb, verbose):  # pragma: no cover
    if pdb:
        enable_post_mortem_debugging()

    level = verbose and 'INFO' or 'WARNING'
    StreamHandler(sys.stdout, level=level).push_application()


@cli.command(name='create')
@click.option('--endpoint', envvar='SPARE_ENDPOINT', required=True)
@click.option('--access-key', envvar='SPARE_ACCESS_KEY', required=True)
@click.option('--secret-key', envvar='SPARE_SECRET_KEY', required=True)
@click.option('--password', envvar='SPARE_PASSWORD', required=True)
@click.option('--bucket', envvar='SPARE_BUCKET', required=True)
@click.option('--path', envvar='SPARE_PATH', type=VALID_PATH, required=True)
@click.option('--force', default=False, required=False, is_flag=True)
@click.option('--skip', multiple=True, required=False)
def create_cli(endpoint, access_key, secret_key, path,
               password, bucket, skip, force):
    s3 = s3_client(endpoint, access_key, secret_key)

    with delay_signal(SIGTERM, None):
        create(path, s3, bucket, password, skip=skip or None, force=force)


@cli.command(name='restore')
@click.option('--endpoint', envvar='SPARE_ENDPOINT', required=True)
@click.option('--access-key', envvar='SPARE_ACCESS_KEY', required=True)
@click.option('--secret-key', envvar='SPARE_SECRET_KEY', required=True)
@click.option('--password', envvar='SPARE_PASSWORD', required=True)
@click.option('--bucket', envvar='SPARE_BUCKET', required=True)
@click.option('--path', envvar='SPARE_PATH', type=VALID_PATH, required=True)
def restore_cli(endpoint, access_key, secret_key, path, password, bucket):
    s3 = s3_client(endpoint, access_key, secret_key)

    with delay_signal(SIGTERM, None):
        restore(path, s3, bucket, password)


@cli.command(name='validate')
@click.option('--endpoint', envvar='SPARE_ENDPOINT', required=True)
@click.option('--access-key', envvar='SPARE_ACCESS_KEY', required=True)
@click.option('--secret-key', envvar='SPARE_SECRET_KEY', required=True)
@click.option('--password', envvar='SPARE_PASSWORD', required=True)
@click.option('--bucket', envvar='SPARE_BUCKET', required=True)
def validate_cli(endpoint, access_key, secret_key, password, bucket):
    s3 = s3_client(endpoint, access_key, secret_key)
    rc = 0 if validate(s3, bucket, password) else 1

    sys.exit(rc)
