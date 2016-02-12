
import sys


def lines_from_stdin(default=None):
    default = [default] if default else []
    if sys.stdin.isatty():
        return default
    for line in sys.stdin:
        yield line
