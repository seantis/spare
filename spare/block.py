import lzma
import hashlib

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from miscreant.aead import AEAD


AVAILABLE_BLOCKS = {}
DEFAULT_BLOCK = 'aes-siv'


class Block(object):
    """ Interface to implement a block. Each block is meant to encrypt and
    decrypt data in memory.

    Blocks may share a single passsword, but they are virtually guaranteed to
    have independent nonces (randomly chosen from a pool of 16^16 values).

    Blocks should also compress/decompress data, the used encryption should
    not use any padding and it should authenticate during/after decryption.

    """

    __slots__ = ('password', 'nonce', 'data')

    def __init_subclass__(cls, id, **kwargs):
        assert id not in AVAILABLE_BLOCKS
        AVAILABLE_BLOCKS[id] = cls

        super().__init_subclass__(**kwargs)

    def __init__(self, password, nonce, data):
        """ Called with the password, nonce and data to encrypt/decrypt.

        All parameters ought to be in bytes.
        """
        raise NotImplementedError  # pragma: nocover

    def encrypt(self):
        """ Encrypts self.data in-place, doesn't return anything. """

        raise NotImplementedError  # pragma: nocover

    def decrypt(self):
        """ Decrypts self.data in-place, doesn't return anything. """

        raise NotImplementedError  # pragma: nocover


class LZMA_AES_SIV_Block(Block, id='aes-siv'):
    """ The default block implementation, using AES-SIV by miscreant.

    Blocks are compressed using LZMA before being encrypted.

    """

    __slots__ = ('password', 'nonce', 'data')

    def __init__(self, password, nonce, data):
        self.password = hashlib.sha512(password).digest()
        self.nonce = nonce
        self.data = data

    @property
    def aead(self):
        return AEAD('AES-SIV', self.password)

    def encrypt(self):
        self.data = self.aead.seal(lzma.compress(self.data), self.nonce)

    def decrypt(self):
        self.data = lzma.decompress(self.aead.open(self.data, self.nonce))


class LZMA_AES_GCM_Block(Block, id='aes-gcm'):
    """ Another block implementation, using AES-GCM via the cryptography
    module. Mainly used to prove that independent block implementations
    work as intended.

    Blocks are compressed using LZMA before being encrypted.

    """

    __slots__ = ('password', 'nonce', 'data')

    def __init__(self, password, nonce, data):
        self.password = hashlib.sha256(password).digest()

        # use the NIST recommended 96 bits for the nonce
        self.nonce = hashlib.blake2b(nonce, digest_size=12).digest()

        self.data = data

    @property
    def aesgcm(self):
        return AESGCM(self.password)

    def encrypt(self):
        self.data = lzma.compress(self.data)
        self.data = self.aesgcm.encrypt(self.nonce, self.data, None)

    def decrypt(self):
        self.data = self.aesgcm.decrypt(self.nonce, self.data, None)
        self.data = lzma.decompress(self.data)
