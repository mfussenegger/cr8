#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import argh
import argparse
import math
import signal
from faker import Factory
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, Optional

from cr8.insert_json import to_insert
from cr8.misc import parse_table, parse_version
from cr8.aio import asyncio, consume
from cr8.cli import to_int
from cr8.fake_providers import GeoSpatialProvider, auto_inc
from cr8 import clients, aio

loop = asyncio.get_event_loop()


SELLECT_COLS = """
select
    column_name,
    data_type,
    character_maximum_length
from
    information_schema.columns
where
    cast(is_generated as string) in ('f', 'NEVER')
    and {schema_column_name} = ?
    and table_name = ?
    and column_name not like '%]'
order by ordinal_position asc
"""


class Column(NamedTuple):
    name: str
    type_name: str
    max_len: Optional[int]


def retrieve_columns(client, schema, table):
    r = aio.run(client.execute, "select min(version['number']) from sys.nodes")
    version = parse_version(r['rows'][0][0])
    stmt = SELLECT_COLS.format(
        schema_column_name='table_schema' if version >= (0, 57, 0) else 'schema_name')
    r = aio.run(client.execute, stmt, (schema, table))
    return [Column(*row) for row in r['rows']]


def generate_row(fakers):
    return [x() for x in fakers]


def array_provider(len_provider, value_provider, dimensions):
    if dimensions == 0:
        return value_provider()
    else:
        return [array_provider(len_provider, value_provider, dimensions - 1)
                for _ in range(len_provider())]


def make_array_provider(inner_provider, dimensions):
    def setup_array_providers(fake, column):
        inner = inner_provider(fake, column)
        arr_len = partial(fake.random_int, min=0, max=50)
        return partial(array_provider, arr_len, inner, dimensions)
    return setup_array_providers


def _gen_short(f, col):
    return partial(f.random_int, min=-32768, max=32767)


def _gen_long(f, col):
    return partial(
        f.random_int, min=-9223372036854775808, max=9223372036854775807)


def _gen_bit(f, col: Column):
    def _gen():
        l = []
        for _ in range(col.max_len or 8):
            l.append(f.boolean() and '1' or '0')
        return ''.join(l)
    return _gen


class DataFaker:
    _mapping = {
        ('id', 'string'): lambda f, ctx: f.uuid4,
        ('id', 'text'): lambda f, ctx: f.uuid4,
        ('id', 'integer'): auto_inc,
        ('id', 'long'): auto_inc,
        ('id', 'bigint'): auto_inc,
    }

    _type_default = {
        'byte': lambda f, col: partial(f.random_int, min=-128, max=127),
        'char': lambda f, col: partial(f.random_int, min=-128, max=127),
        'short': _gen_short,
        'smallint': _gen_short,
        'integer': lambda f, col: partial(f.random_int, min=-2147483648, max=2147483647),
        'long': _gen_long,
        'bigint': _gen_long,
        'float': lambda f, col: f.pyfloat,
        'real': lambda f, col: f.pyfloat,
        'double': lambda f, col: f.pydecimal,
        'double precision': lambda f, col: f.pydecimal,
        'ip': lambda f, col: f.ipv4,
        'timestamp': lambda f, col: partial(
            f.date_time_between, start_date='-2y', end_date='now'),
        'timestamp with time zone': lambda f, col: partial(
            f.date_time_between, start_date='-2y', end_date='now'),
        'timestamp without time zone': lambda f, col: partial(
            f.date_time_between, start_date='-2y', end_date='now'),
        'string': lambda f, col: f.word,
        'text': lambda f, col: f.word,
        'boolean': lambda f, col: f.boolean,
        'geo_point': lambda f, col: f.geo_point,
        'geo_shape': lambda f, col: f.geo_shape,
        'object': lambda f, col: dict,
        'bit': _gen_bit,
    }

    _custom = {
        'auto_inc': auto_inc
    }

    def __init__(self):
        self.fake = Factory.create()
        self.fake.add_provider(GeoSpatialProvider)

    def _provider_for_type(self, column: Column):
        inner_type, *dim = column.type_name.split('_array')
        inner_provider = self._type_default.get(inner_type)
        if not dim or not inner_provider:
            return inner_provider
        return make_array_provider(inner_provider, len(dim))

    def provider_for_column(self, column):
        provider = getattr(self.fake, column.name, None)
        if provider:
            return provider
        custom_provider = self._custom.get(column.name)
        if custom_provider:
            return custom_provider(self.fake, column)
        alternative = self._mapping.get((column.name, column.type_name))
        if not alternative:
            alternative = self._provider_for_type(column)
            if not alternative:
                msg = 'No fake provider found for column "{col}" with type "{type}"'
                raise ValueError(msg.format(col=column.name, type=column.type_name))
        return alternative(self.fake, column)

    def provider_from_mapping(self, column: Column, mapping):
        key = mapping[column.name]
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
    for column in columns:
        if mapping and column.name in mapping:
            fakers.append(fake.provider_from_mapping(column, mapping))
        else:
            fakers.append(fake.provider_for_column(column))
    return partial(generate_row, fakers)


async def _exec_many(client, stmt, args_coro):
    return await client.execute_many(stmt, await args_coro)


def _create_bulk_args(row_fun, req_size):
    return [row_fun() for i in range(req_size)]


def _bulk_size_generator(num_records, bulk_size, active):
    """ Generate bulk_size until num_records is reached or active becomes false

    >>> gen = _bulk_size_generator(155, 50, [True])
    >>> list(gen)
    [50, 50, 50, 5]
    """
    while active and num_records > 0:
        req_size = min(num_records, bulk_size)
        num_records -= req_size
        yield req_size


async def _gen_data_and_insert(q, e, client, stmt, row_fun, size_seq):
    for size in size_seq:
        args_coro = loop.run_in_executor(e, _create_bulk_args, row_fun, size)
        task = asyncio.ensure_future(_exec_many(client, stmt, args_coro))
        await q.put(task)
    await q.put(None)


@argh.arg('--table', help='table name', required=True)
@argh.arg('--hosts', help='crate hosts', type=str)
@argh.arg('-n', '--num-records',
          help='number of records to insert',
          type=to_int,
          default=int(1e5))
@argh.arg('-b', '--bulk-size', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('--mapping-file',
          type=argparse.FileType('r'),
          help='JSON file with a column to fake provider mapping.')
@argh.wrap_errors([KeyboardInterrupt] + clients.client_errors)
def insert_fake_data(*,
                     hosts=None,
                     table=None,
                     num_records=1e5,
                     bulk_size=1000,
                     concurrency=25,
                     mapping_file=None):
    """Generate random data and insert it into a table.

    This will read the table schema and then find suitable random data providers.
    Which provider is choosen depends on the column name and data type.

    Example:

        A column named `name` will map to the `name` provider.
        A column named `x` of type int will map to `random_int` because there
        is no `x` provider.

    Available providers are listed here:
        https://faker.readthedocs.io/en/latest/providers.html

        Additional providers:
        - auto_inc:
            Returns unique incrementing numbers.
            Automatically used for columns named "id" of type int or long
        - geo_point
            Returns [<lon>, <lat>]
            Automatically used for columns of type geo_point

    Args:
        hosts: <host>:[<port>] of the Crate node
        table: The table name into which the data should be inserted.
            Either fully qualified: `<schema>.<table>` or just `<table>`
        num_records: Number of records to insert.
            Usually a number but expressions like `1e4` work as well.
        bulk_size: The bulk size of the insert statements.
        concurrency: How many operations to run concurrently.
        mapping_file: A JSON file that defines a mapping from column name to
            fake-factory provider.
            The format is as follows:
            {
                "column_name": ["provider_with_args", ["arg1", "arg"]],
                "x": ["provider_with_args", ["arg1"]],
                "y": "provider_without_args"
            }
    """
    with clients.client(hosts, concurrency=1) as client:
        schema, table_name = parse_table(table)
        columns = retrieve_columns(client, schema, table_name)
    if not columns:
        sys.exit('Could not find columns for table "{}"'.format(table))
    print('Found schema: ')
    columns_dict = {r.name: r.type_name for r in columns}
    print(json.dumps(columns_dict, sort_keys=True, indent=4))
    mapping = None
    if mapping_file:
        mapping = json.load(mapping_file)

    bulk_size = min(num_records, bulk_size)
    num_inserts = int(math.ceil(num_records / bulk_size))

    gen_row = create_row_generator(columns, mapping)

    stmt = to_insert('"{schema}"."{table_name}"'.format(**locals()), columns_dict)[0]
    print('Using insert statement: ')
    print(stmt)

    print('Will make {} requests with a bulk size of {}'.format(
        num_inserts, bulk_size))

    print('Generating fake data and executing inserts')
    q = asyncio.Queue(maxsize=concurrency)
    with clients.client(hosts, concurrency=concurrency) as client:
        active = [True]

        def stop():
            asyncio.ensure_future(q.put(None))
            active.clear()
            loop.remove_signal_handler(signal.SIGINT)
        if sys.platform != 'win32':
            loop.add_signal_handler(signal.SIGINT, stop)
        bulk_seq = _bulk_size_generator(num_records, bulk_size, active)
        with ThreadPoolExecutor() as e:
            tasks = asyncio.gather(
                _gen_data_and_insert(q, e, client, stmt, gen_row, bulk_seq),
                consume(q, total=num_inserts)
            )
            loop.run_until_complete(tasks)


def main():
    argh.dispatch_command(insert_fake_data)


if __name__ == '__main__':
    main()
