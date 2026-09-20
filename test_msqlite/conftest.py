"""Enable beartype runtime type checking for the package and the tests, when beartype supports this Python.

beartype is dev-only. On a Python it does not yet support (0.22.9 fails to import on the 3.15 pre-release),
the tests still run, just without runtime type checking.
"""

import warnings

try:
    from beartype.claw import beartype_package
except ImportError as e:
    warnings.warn(f"beartype is unavailable on this Python; running tests without runtime type checking ({e})", RuntimeWarning, stacklevel=1)
else:
    beartype_package("msqlite")
    beartype_package("test_msqlite")
