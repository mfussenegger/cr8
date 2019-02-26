"""misc functions that have no real home."""

import logging
import gzip
from typing import Tuple, Iterator, Any
from collections import defaultdict


def init_logging(log):
    log.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    log.addHandler(ch)


def try_len(o: Any) -> int:
    """ Return len of `o` or None if `o` doesn't support len

    >>> try_len([1, 2])
    2

    >>> try_len(print)
    >>> try_len(None)
    """
    if not o:
        return None
    try:
        return len(o)
    except TypeError:
        return None


def parse_version(version: str) -> tuple:
    """Parse a string formatted X[.Y.Z] version number into a tuple

    >>> parse_version('10.2.3')
    (10, 2, 3)

    >>> parse_version('12')
    (12, 0, 0)
    """
    if not version:
        return None
    parts = version.split('.')
    missing = 3 - len(parts)
    return tuple(int(i) for i in parts + ([0] * missing))


def parse_table(fq_table: str) -> Tuple[str, str]:
    """Parse a tablename into tuple(<schema>, <table>).

    Schema defaults to doc if the table name doesn't contain a schema.

    >>> parse_table('x.users')
    ('x', 'users')

    >>> parse_table('users')
    ('doc', 'users')
    """
    parts = fq_table.split('.')
    if len(parts) == 1:
        return 'doc', parts[0]
    elif len(parts) == 2:
        return parts[0], parts[1]
    else:
        raise ValueError


def as_bulk_queries(queries, bulk_size):
    """Group a iterable of (stmt, args) by stmt into (stmt, bulk_args).

    bulk_args will be a list of the args grouped by stmt.

    len(bulk_args) will be <= bulk_size
    """
    stmt_dict = defaultdict(list)
    for stmt, args in queries:
        bulk_args = stmt_dict[stmt]
        bulk_args.append(args)
        if len(bulk_args) == bulk_size:
            yield stmt, bulk_args
            del stmt_dict[stmt]
    for stmt, bulk_args in stmt_dict.items():
        yield stmt, bulk_args


def get_lines(filename: str) -> Iterator[str]:
    """Create an iterator that returns the lines of a utf-8 encoded file."""
    if filename.endswith('.gz'):
        with gzip.open(filename, 'r') as f:
            for line in f:
                yield line.decode('utf-8')
    else:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                yield line


def as_statements(lines: Iterator[str]) -> Iterator[str]:
    """Create an iterator that transforms lines into sql statements.

    Statements within the lines must end with ";"
    The last statement will be included even if it does not end in ';'

    >>> list(as_statements(['select * from', '-- comments are filtered', 't;']))
    ['select * from t']

    >>> list(as_statements(['a;', 'b', 'c;', 'd', ' ']))
    ['a', 'b c', 'd']
    """
    lines = (l.strip() for l in lines if l)
    lines = (l for l in lines if l and not l.startswith('--'))
    parts = []
    for line in lines:
        parts.append(line.rstrip(';'))
        if line.endswith(';'):
            yield ' '.join(parts)
            parts.clear()
    if parts:
        yield ' '.join(parts)


def break_iterable(iterable, pred):
    """Break a iterable on the item that matches the predicate into lists.

    The item that matched the predicate is not included in the result.

    >>> list(break_iterable([1, 2, 3, 4], lambda x: x == 3))
    [[1, 2], [4]]
    """
    sublist = []
    for i in iterable:
        if pred(i):
            yield sublist
            sublist = []
        else:
            sublist.append(i)
    yield sublist
