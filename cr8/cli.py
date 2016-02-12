
import sys
import json


def lines_from_stdin(default=None):
    if sys.stdin.isatty():
        if default:
            yield default
        return
    for line in sys.stdin:
        yield line


def dicts_from_stdin():
    """ a generator producing dicts from json strings provided via stdin

    1 JSON object per line is supported:

        {"name": "n1"}
        {"name": "n2"}

    Or 1 JSON object:

        {
            "name": "n1"
        }
    """
    if sys.stdin.isatty():
        raise SystemExit('Expected json input via stdin')
    for line in sys.stdin:
        try:
            yield json.loads(line)
        except json.decoder.JSONDecodeError:
            yield json.loads(line + '\n' + sys.stdin.read())
