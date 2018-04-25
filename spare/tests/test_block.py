import pytest
import random

from hypothesis import given, strategies
from spare.block import AVAILABLE_BLOCKS
from secrets import token_bytes


@pytest.mark.parametrize("cls", AVAILABLE_BLOCKS.values())
def test_block(cls):
    block = cls(password=b'password', nonce=token_bytes(16), data=b'foobar')

    block.encrypt()
    assert block.data != b'foobar'

    block.decrypt()
    assert block.data == b'foobar'


@pytest.mark.parametrize("cls", AVAILABLE_BLOCKS.values())
def test_large_block(cls):
    data = token_bytes(1_048_576)

    block = cls(password=b'password', nonce=token_bytes(16), data=data)

    block.encrypt()
    assert block.data != data

    block.decrypt()
    assert block.data == data


@pytest.mark.parametrize("cls", AVAILABLE_BLOCKS.values())
@given(data=strategies.binary(), password=strategies.text())
def test_block_with_random_binary_data(cls, data, password):

    block = cls(password.encode('utf-8'), token_bytes(16), data)
    block.encrypt()

    assert block.data != data

    block.decrypt()
    assert block.data == data


@pytest.mark.parametrize("cls", AVAILABLE_BLOCKS.values())
def test_empty_block(cls):
    block = cls(b'password', token_bytes(16), b'')
    block.encrypt()

    assert block.data != b''

    block.decrypt()
    assert block.data == b''


@pytest.mark.parametrize("cls", AVAILABLE_BLOCKS.values())
def test_flip_bit(cls):
    block = cls(b'password', token_bytes(16), b'foobar')
    block.encrypt()

    data = bytearray(block.data)

    bit = random.randrange(0, len(data))
    data[bit] = data[bit] ^ 1

    block.data = bytes(data)

    with pytest.raises(Exception) as e:
        block.decrypt()

    assert 'InvalidTag' in str(e) or 'IntegrityError' in str(e)
