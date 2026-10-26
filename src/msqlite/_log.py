"""The package logger.

Every module logs through ``log`` so that all records carry the one name ``msqlite``: a handler or level
set on that logger covers the whole package, and users never see private module names in their logs.
"""

from logging import getLogger

log = getLogger("msqlite")
