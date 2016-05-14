#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import argh
import argparse
from pprint import pprint
from faker import Factory
from functools import partial
from collections import OrderedDict
from crate.client import connect
from concurrent.futures import ProcessPoolExecutor

from .json2insert import to_insert
from .misc import parse_table
from .aio import asyncio, consume
from .cli import to_int


PROVIDER_LIST_URL = 'http://fake-factory.readthedocs.org/en/latest/providers.html'

loop = asyncio.get_event_loop()


def retrieve_columns(cursor, schema, table):
    cursor.execute(
        'select column_name, data_type from information_schema.columns \
        where schema_name = ? and table_name = ? \
        order by ordinal_position asc', (schema, table))
    return OrderedDict({x[0]: x[1] for x in cursor.fetchall()})


def generate_row(fakers):
    return [x() for x in fakers]


def x1000(func):
    return func() * 1000


def timestamp(fake):
    # return lamda: fake.unix_time() * 1000 workaround:
    # can't use lambda or nested functions because of multiprocessing pickling
    return partial(x1000, fake.unix_time)


def from_attribute(attr):
    def func(fake):
        return getattr(fake, attr)
    return func


class DataFaker:
    _mapping = {
        ('id', 'string'): from_attribute('uuid4'),
        ('id', 'integer'): from_attribute('random_int'),
        ('id', 'long'): from_attribute('random_int')
    }

    _type_default = {
        'integer': from_attribute('random_int'),
        'long': from_attribute('random_int'),
        'timestamp': timestamp,
        'string': from_attribute('word'),
        'boolean': from_attribute('boolean')
    }

    def __init__(self):
        self.fake = Factory.create()

    def provider_for_column(self, column_name, data_type):
        provider = getattr(self.fake, column_name, None)
        if provider:
            return provider
        alternative = self._mapping.get((column_name, data_type), None)
        if not alternative:
            alternative = self._type_default[data_type]
        return alternative(self.fake)

    def provider_from_mapping(self, column_name, mapping):
        key = mapping[column_name]
        args = None
        if isinstance(key, list):
            key, args = key
        provider = getattr(self.fake, key, None)
        if not provider:
            raise KeyError('No fake provider with name "%s" found' % (key,))
        if args:
            provider = partial(provider, *args)
        return provider


def create_row_generator(columns, mapping=None):
    fake = DataFaker()
    fakers = []
    for column_name, type_name in columns.items():
        if mapping and column_name in mapping:
            fakers.append(fake.provider_from_mapping(column_name, mapping))
        else:
            fakers.append(fake.provider_for_column(column_name, type_name))
    return partial(generate_row, fakers)


def generate_bulk_args(generate_row, bulk_size):
    return [generate_row() for i in range(bulk_size)]


async def _produce_data_and_insert(q, cursor, stmt, bulk_args_fun, num_inserts):

    async def insert(stmt, args):
        f = loop.run_in_executor(None, cursor.executemany, stmt, args)
        await f

    executor = ProcessPoolExecutor()
    for i in range(num_inserts):
        args = await asyncio.ensure_future(
            loop.run_in_executor(executor, bulk_args_fun))
        task = asyncio.ensure_future(insert(stmt, args))
        await q.put(task)
    await q.put(None)


@argh.arg('fqtable', help='(fully qualified) table name. \
          Either <schema>.<table> or just <table>')
@argh.arg('hosts', help='crate hosts', type=str)
@argh.arg('num_records', help='number of records to insert', type=to_int)
@argh.arg('--bulk-size', type=to_int)
@argh.arg('--concurrency', type=to_int)
@argh.arg('--mapping-file',
          type=argparse.FileType('r'),
          help='''JSON file with a column to fake provider mapping.
In the format:
{
    "source_column1": ["provider_with_args", ["arg1", "arg"]],
    "source_column2": "provider_without_args"
}
''')
def fill_table(hosts,
               fqtable,
               num_records,
               bulk_size=1000,
               concurrency=100,
               mapping_file=None):
    """ fills a table with random data

    Insert <num_records> into <fqtable> on <hosts>.
    Each insert request will contain <bulk_size> items.

    Depending on colum names and data types of the given table an appropriate
    provider is selected which is used to generate random data.

    E.g. a column called `name` will be filled with names.

    """
    conn = connect(hosts)
    c = conn.cursor()

    schema, table = parse_table(fqtable)
    columns = retrieve_columns(c, schema, table)
    if not columns:
        sys.exit('Could not find columns for table "{}"'.format(fqtable))
    yield 'Found schema: '
    pprint(columns)
    mapping = None
    if mapping_file:
        mapping = json.load(mapping_file)
    generate_row = create_row_generator(columns, mapping)
    bulk_args_fun = partial(generate_bulk_args, generate_row, bulk_size)

    stmt = to_insert(fqtable, columns)[0]
    yield 'Using insert statement: '
    yield stmt

    bulk_size = min(num_records, bulk_size)
    num_inserts = int(num_records / bulk_size)
    yield 'Will make {} requests with a bulk size of {}'.format(
        num_inserts, bulk_size)

    yield 'Generating fake data and executing inserts'
    q = asyncio.Queue(maxsize=concurrency)
    loop.run_until_complete(asyncio.gather(
        _produce_data_and_insert(q, conn.cursor(), stmt, bulk_args_fun, num_inserts),
        consume(q, total=num_inserts)))


def main():
    argh.dispatch_command(fill_table)


if __name__ == '__main__':
    main()
