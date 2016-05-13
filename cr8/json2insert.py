#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import asyncio as aio
from tqdm import tqdm
from collections import defaultdict

from .cli import dicts_from_stdin, to_int


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


class AsyncExecutor:

    def __init__(self, cursor, table, bulk_size, loop):
        self.cursor = cursor
        self.table = table
        self.bulk_size = bulk_size
        self.stmt_dict = defaultdict(list)
        self.loop = loop

    async def execute(self, stmt, bulk_args):
        await self.loop.run_in_executor(
            None, self.cursor.executemany, stmt, bulk_args)

    async def process(self, stmt, args):
        stmt_dict = self.stmt_dict

        bulk_args = stmt_dict[stmt]
        bulk_args.append(args)
        if len(bulk_args) == self.bulk_size:
            await self.execute(stmt, bulk_args)
            del stmt_dict[stmt]

    async def after_loop(self, count):
        for stmt, bulk_args in self.stmt_dict.items():
            await self.execute(stmt, bulk_args)
        print('Inserted {count} records'.format(count=count))


async def do_inserts(table, process, after_loop):
    count = 0
    for d in tqdm(dicts_from_stdin()):
        stmt, args = to_insert(table, d)
        await process(stmt, args)
        count += 1
    await after_loop(count)


def print_only(table):
    for d in dicts_from_stdin():
        yield to_insert(table, d)
    yield ''
    yield 'No hosts provided. Nothing inserted'


def async_inserts(cursor, table, bulk_size):
    loop = aio.get_event_loop()
    e = AsyncExecutor(cursor, table, bulk_size, loop)
    print('Reading statements and inserting with bulk_size=' + str(bulk_size))
    try:
        loop.run_until_complete(do_inserts(table, e.process, e.after_loop))
    finally:
        loop.stop()
        loop.close()


def sync_inserts(cursor, table, bulk_size):
    stmt_dict = defaultdict(list)
    count = 0
    for d in tqdm(dicts_from_stdin()):
        stmt, args = to_insert(table, d)
        bulk_args = stmt_dict[stmt]
        bulk_args.append(args)
        if len(bulk_args) == bulk_size:
            cursor.executemany(stmt, bulk_args)
            del stmt_dict[stmt]
        count += 1
    for stmt, bulk_args in stmt_dict.items():
        cursor.executemany(stmt, bulk_args)
    print('Inserted {count} records'.format(count=count))


@argh.arg('table', help='table name that should be used in the statement')
@argh.arg('--bulk-size', type=to_int)
@argh.arg('hosts', help='crate hosts which will be used \
          to execute the insert statement')
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
    if sequential:
        print('Executing requests sequential with bulk_size={}'.format(bulk_size))
        sync_inserts(cursor, table, bulk_size)
    else:
        print('Executing requests async with bulk_size={}'.format(bulk_size))
        async_inserts(cursor, table, bulk_size)


def main():
    argh.dispatch_command(json2insert)


if __name__ == '__main__':
    main()
