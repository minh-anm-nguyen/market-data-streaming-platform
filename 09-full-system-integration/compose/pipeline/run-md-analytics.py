"""Launch P06's CLI from its source tree, so its sql/ directory is findable.

P06's ``SqlBank`` locates ``sql/`` by walking up from its own module file. That
works from a checkout and fails from an installed wheel, because the directory
sits beside the package rather than inside it:

    SQL_DIR_NOT_FOUND: could not locate the sql/ directory above
    /usr/local/lib/python3.12/site-packages/md_analytics/sqlbank.py

So the pipeline runs P06 against the read-only repository mount instead of the
installed copy. A launcher file rather than ``python -c`` for the same reason
``read-dataset-id.py`` exists: the payload would otherwise have to survive
quoting through a Python generator, a POSIX shell and ``docker run``, and it did
not the first time it was tried.

This is a workaround, and the real fix belongs in P06: ship ``sql/`` as package
data so the installed package is self-contained. Until it does, an operator who
pip-installs P06 and runs ``build`` gets a confusing error about a missing
directory rather than a clear one about an incomplete install.
"""

import sys

sys.path.insert(0, "/repo/06-clickhouse-analytics/src")

from md_analytics.cli import main  # noqa: E402  (after the path insert)

if __name__ == "__main__":
    raise SystemExit(main())
