from click.testing import CliRunner
from spare.cli import create_cli, restore_cli, validate_cli


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
