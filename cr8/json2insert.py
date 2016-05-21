#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argh
import itertools
import json
from appmetrics import metrics
from functools import partial
from tqdm import tqdm

from .cli import dicts_from_stdin, to_int
from .misc import as_bulk_queries
from .aio import asyncio, run, execute_many, measure


def to_insert(table, d):
    """ generate a insert statement using the given table and dictionary

    :param table: table name
    :type table: string
    :param d: dictionary containing the columns and values
    :type d: dict

    :return: tuple with the statement and arguments
    """

    columns = []
    args = []
    for key, val in d.items():
        columns.append(key)
        args.append(val)
    stmt = 'insert into {table} ({columns}) values ({params})'.format(
        table=table,
        columns=', '.join(columns),
        params=', '.join(['?'] * len(columns)))
    return (stmt, args)


def print_only(table):
    for d in dicts_from_stdin():
        yield to_insert(table, d)
    yield ''
    yield 'No hosts provided. Nothing inserted'


@argh.arg('table', help='table name that should be used in the statement')
@argh.arg('--bulk-size', type=to_int)
@argh.arg('hosts', help='crate hosts which will be used \
          to execute the insert statement')
@argh.arg('-c', '--concurrency', type=to_int)
@argh.wrap_errors([KeyboardInterrupt])
def json2insert(table, bulk_size=1000, concurrency=100, stats=False, *hosts):
    """ Converts the given json line (read from stdin) into an insert statement

    If hosts are specified the insert statement will be executed on those hosts.
    Otherwise the statement and the arguments are printed.
    """
    if not hosts:
        return print_only(table)

    from crate.client import connect
    stdout = sys.stderr if stats else sys.stdout
    log = partial(print, file=stdout)
    conn = connect(hosts)
    cursor = conn.cursor()
    queries = (to_insert(table, d) for d in dicts_from_stdin())
    bulk_queries = as_bulk_queries(queries, bulk_size)
    log('Executing requests async bulk_size={} concurrency={}'.format(
        bulk_size, concurrency))

    if concurrency == 1:
        all(itertools.starmap(
            cursor.executemany, tqdm(bulk_queries, unit=' requests')))
        return

    loop = asyncio.get_event_loop()
    f = partial(execute_many, loop, cursor)
    if stats:
        hist = metrics.new_histogram('durations')
        f = partial(measure, hist, f)
    run(f, bulk_queries, concurrency, loop)
    if stats:
        yield json.dumps(hist.get())


def main():
    argh.dispatch_command(json2insert)


if __name__ == '__main__':
    main()
