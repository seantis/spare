from logbook import Logger
from os import supports_follow_symlinks

log = Logger('spare')

FOLLOW_SYMLINKS = supports_follow_symlinks and {'follow_symlinks': True} or {}

__all__ = ('log', 'FOLLOW_SYMLINKS')
