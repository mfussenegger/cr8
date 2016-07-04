
import sys
import json
import ast
from collections import OrderedDict
from typing import List, Union, Iterable


def to_http_uri(s: str) -> str:
    """Prefix the string with 'http://' if there is no schema."""
    if not s.startswith(('http://', 'https://')):
        return 'http://' + s
    return s


def to_hosts(hosts: Union[Iterable[str], str]) -> List[str]:
    """Convert a string of whitespace or comma separated hosts into a list of hosts.

    Hosts may also already be a list or other iterable.
    Each host will be prefixed with 'http://' if it is not already there.

    >>> to_hosts('n1:4200,n2:4200')
    ['http://n1:4200', 'http://n2:4200']

    >>> to_hosts('n1:4200 n2:4200')
    ['http://n1:4200', 'http://n2:4200']

    >>> to_hosts('https://n1:4200')
    ['https://n1:4200']

    >>> to_hosts(['http://n1:4200', 'n2:4200'])
    ['http://n1:4200', 'http://n2:4200']
    """
    if isinstance(hosts, str):
        hosts = hosts.replace(',', ' ').split()
    return [to_http_uri(i) for i in hosts]


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


def dicts_from_lines(lines):
    """ returns a generator producing dicts from json lines

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
    lines = iter(lines)
    for line in lines:
        line = line.strip()
        if not line:
            continue  # skip empty lines
        try:
            yield json.loads(line, object_pairs_hook=OrderedDict)
        except json.decoder.JSONDecodeError:
            content = line + ''.join(lines)
            dicts = json.loads(content, object_pairs_hook=OrderedDict)
            if isinstance(dicts, list):
                yield from dicts
            else:
                yield dicts


def dicts_from_stdin():
    if sys.stdin.isatty():
        raise SystemExit('Expected json input via stdin')
    yield from dicts_from_lines(sys.stdin)


dicts_from_stdin.__doc__ = dicts_from_lines.__doc__
