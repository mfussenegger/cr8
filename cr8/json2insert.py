#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

import itertools
from tqdm import tqdm

from .cli import dicts_from_stdin, to_int
from .misc import aio, as_bulk_queries


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


async def do_exec_bulk_async(f, bulk_queries):
    tasks = []
    for stmt, bulk_args in tqdm(bulk_queries):
        tasks.append(aio.ensure_future(f(stmt, bulk_args)))
    print('Fired off all requests, waiting for them to complete')
    for task in tqdm(tasks, total=len(tasks), unit=' requests'):
        await task


def exec_bulk_queries_async(exec_func, bulk_queries):
    loop = aio.get_event_loop()

    async def f(stmt, bulk_args):
        await loop.run_in_executor(None, exec_func, stmt, bulk_args)

    loop.run_until_complete(do_exec_bulk_async(f, bulk_queries))


def print_only(table):
    for d in dicts_from_stdin():
        yield to_insert(table, d)
    yield ''
    yield 'No hosts provided. Nothing inserted'


@argh.arg('table', help='table name that should be used in the statement')
@argh.arg('--bulk-size', type=to_int)
@argh.arg('hosts', help='crate hosts which will be used \
          to execute the insert statement')
@argh.wrap_errors([KeyboardInterrupt])
def json2insert(table, bulk_size=1000, sequential=False, *hosts):
    """ Converts the given json line (read from stdin) into an insert statement

    If hosts are specified the insert statement will be executed on those hosts.
    Otherwise the statement and the arguments are printed.
    """
    if not hosts:
        return print_only(table)

    from crate.client import connect
    conn = connect(hosts)
    cursor = conn.cursor()
    queries = (to_insert(table, d) for d in dicts_from_stdin())
    bulk_queries = as_bulk_queries(queries, bulk_size)
    if sequential:
        yield 'Executing requests sequential with bulk_size={}'.format(bulk_size)
        all(itertools.starmap(cursor.executemany, tqdm(bulk_queries)))
    else:
        yield 'Executing requests async with bulk_size={}'.format(bulk_size)
        exec_bulk_queries_async(cursor.executemany, bulk_queries)


def main():
    argh.dispatch_command(json2insert)


if __name__ == '__main__':
    main()
