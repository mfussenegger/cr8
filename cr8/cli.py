
import sys
import json
import ast


def to_int(s):
    """ converts a string to an integer

    >>> to_int('1_000_000')
    1000000

    >>> to_int('1e6')
    1000000

    >>> to_int('1000')
    1000
    """
    try:
        return int(s.replace('_', ''))
    except ValueError:
        return int(ast.literal_eval(s))


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

    Or a list of JSON objects:

        [
            {"name": "n1"},
            {"name": "n2"},
        ]
    """
    if sys.stdin.isatty():
        raise SystemExit('Expected json input via stdin')
    for line in sys.stdin:
        try:
            yield json.loads(line)
        except json.decoder.JSONDecodeError:
            dicts = json.loads(line + '\n' + sys.stdin.read())
            if isinstance(dicts, list):
                yield from dicts
            else:
                yield dicts
