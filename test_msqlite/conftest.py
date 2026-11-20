"""Shared test setup: keep the GIL off on free-threaded builds, and enable beartype runtime type checking.

On a free-threaded build (3.14t, 3.15t) gilguard raises ``GILEnabledError`` the moment an import re-enables the
GIL, and at once if a pytest plugin already did, so a free-threaded test run cannot quietly become a GIL run.
It raises on a standard build too (the GIL is always on there), hence the build check.

beartype is dev-only. On a Python it does not yet support (0.22.9 fails to import on the 3.15 pre-release),
the tests still run, just without runtime type checking.
"""

import sysconfig
import warnings

if sysconfig.get_config_var("Py_GIL_DISABLED"):
    import gilguard  # noqa: F401  imported for its effect, before anything else this file imports

try:
    from beartype.claw import beartype_package
except ImportError as e:
    warnings.warn(f"beartype is unavailable on this Python; running tests without runtime type checking ({e})", RuntimeWarning, stacklevel=1)
else:
    beartype_package("msqlite")
    beartype_package("test_msqlite")
