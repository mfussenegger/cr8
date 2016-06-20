#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import sys
import json
from functools import partial

from .cli import dicts_from_stdin, to_int, to_hosts
from .misc import as_bulk_queries
from . import aio
from .metrics import Stats


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
        columns.append('"{}"'.format(key))
        args.append(val)
    stmt = 'insert into {table} ({columns}) values ({params})'.format(
        table=table,
        columns=', '.join(columns),
        params=', '.join(['?'] * len(columns)))
    return (stmt, args)


def print_only(table):
    for d in dicts_from_stdin():
        print(to_insert(table, d))
    print('')
    print('No hosts provided. Nothing inserted')


@argh.arg('table', help='table name that should be used in the statement')
@argh.arg('-b', '--bulk-size', type=to_int)
@argh.arg('--hosts', help='crate hosts which will be used \
          to execute the insert statement', type=to_hosts)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.wrap_errors([KeyboardInterrupt])
def insert_json(table, bulk_size=1000, concurrency=25, hosts=None):
    """ Converts the given json line (read from stdin) into an insert statement

    If hosts are specified the insert statement will be executed on those hosts.
    Otherwise the statement and the arguments are printed.
    """
    if not hosts:
        return print_only(table)

    log = partial(print, file=sys.stderr)
    queries = (to_insert(table, d) for d in dicts_from_stdin())
    bulk_queries = as_bulk_queries(queries, bulk_size)
    log('Executing requests async bulk_size={} concurrency={}'.format(
        bulk_size, concurrency))

    loop = aio.asyncio.get_event_loop()
    stats = Stats()
    with aio.Client(hosts, conn_pool_limit=concurrency) as client:
        f = partial(aio.measure, stats, client.execute_many)
        aio.run(f, bulk_queries, concurrency, loop)
    yield json.dumps(stats.get(), sort_keys=True, indent=4)


def main():
    argh.dispatch_command(insert_json)


if __name__ == '__main__':
    main()
