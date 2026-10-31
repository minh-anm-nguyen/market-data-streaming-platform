"""Print the dataset_id from a P01 run report.

A three-line script rather than an inline `python -c`, because the pipeline
runner is itself generated and the payload would have to survive quoting
through a Python writer, a POSIX shell and `docker run`. It did not: an escaped
quote in the generator produced a file that would not parse. A named file is
read once and never escaped.
"""

import json
import sys

print(json.load(open(sys.argv[1], encoding="utf-8"))["dataset_id"])
