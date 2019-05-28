from click.testing import CliRunner
from spare.cli import create_cli
from spare.cli import lock_cli
from spare.cli import restore_cli
from spare.cli import unlock_cli
from spare.cli import validate_cli


def test_create_cli(endpoint, access_key, secret_key, temporary_path):
    runner = CliRunner()

    result = runner.invoke(create_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
        '--path', str(temporary_path)
    ])
    assert result.exit_code == 0

    result = runner.invoke(restore_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
        '--path', str(temporary_path)
    ])
    assert result.exit_code == 0

    result = runner.invoke(validate_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
    ])
    assert result.exit_code == 0

    # lock once
    result = runner.invoke(lock_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
    ])
    assert result.exit_code == 0

    # lock again
    result = runner.invoke(lock_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
    ])
    assert result.exit_code == 1

    # unlock
    result = runner.invoke(unlock_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
    ])
    assert result.exit_code == 0

    # unlock again
    result = runner.invoke(unlock_cli, [
        '--endpoint', endpoint.geturl(),
        '--access-key', access_key,
        '--secret-key', secret_key,
        '--password', 'foobar',
        '--bucket', 'foobar',
    ])
    assert result.exit_code == 1
