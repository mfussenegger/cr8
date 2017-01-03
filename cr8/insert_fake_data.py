#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
import argh
import argparse
import operator
import math
import signal
from faker import Factory
from functools import partial
from collections import OrderedDict
from concurrent.futures import ProcessPoolExecutor

from .insert_json import to_insert
from .misc import parse_table, parse_version
from .aio import asyncio, consume
from .cli import to_int
from .fake_providers import GeoSpatialProvider, auto_inc
from . import clients, aio

loop = asyncio.get_event_loop()


SELLECT_COLS = """
select
    column_name,
    data_type
from
    information_schema.columns
where
    is_generated = false
    and {schema_column_name} = ?
    and table_name = ?
order by ordinal_position asc
"""


def retrieve_columns(client, schema, table):
    r = aio.run(client.execute, "select min(version['number']) from sys.nodes")
    version = parse_version(r['rows'][0][0])
    stmt = SELLECT_COLS.format(
        schema_column_name='table_schema' if version >= (0, 57, 0) else 'schema_name')
    r = aio.run(client.execute, stmt, (schema, table))
    return OrderedDict(r['rows'])


def generate_row(fakers):
    return [x() for x in fakers]


def x1000(func):
    return func() * 1000


def timestamp(fake):
    # return lamda: fake.unix_time() * 1000 workaround:
    # can't use lambda or nested functions because of multiprocessing pickling
    return partial(x1000, fake.unix_time)


class DataFaker:
    _mapping = {
        ('id', 'string'): operator.attrgetter('uuid4'),
        ('id', 'integer'): auto_inc,
        ('id', 'long'): auto_inc,
    }

    _type_default = {
        'integer': operator.attrgetter('random_int'),
        'long': operator.attrgetter('random_int'),
        'float': operator.attrgetter('pyfloat'),
        'double': operator.attrgetter('pydecimal'),
        'ip': operator.attrgetter('ipv4'),
        'timestamp': timestamp,
        'string': operator.attrgetter('word'),
        'boolean': operator.attrgetter('boolean'),
        'geo_point': operator.attrgetter('geo_point'),
    }

    _custom = {
        'auto_inc': auto_inc
    }

    def __init__(self):
        self.fake = Factory.create()
        self.fake.add_provider(GeoSpatialProvider)

    def provider_for_column(self, column_name, data_type):
        provider = getattr(self.fake, column_name, None)
        if provider:
            return provider
        custom_provider = self._custom.get(column_name)
        if custom_provider:
            return custom_provider(self.fake)
        alternative = self._mapping.get((column_name, data_type))
        if not alternative:
            alternative = self._type_default.get(data_type)
            if not alternative:
                msg = 'No fake provider found for column "{col}" with type "{type}"'
                raise ValueError(msg.format(col=column_name, type=data_type))
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


async def _gen_data_and_insert(q, client, stmt, row_fun, size_seq):
    with ProcessPoolExecutor() as e:
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
          default=1e5)
@argh.arg('-b', '--bulk-size', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('--mapping-file',
          type=argparse.FileType('r'),
          help='JSON file with a column to fake provider mapping.')
@argh.wrap_errors([KeyboardInterrupt] + clients.client_errors)
def insert_fake_data(hosts=None,
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
        http://fake-factory.readthedocs.io/en/latest/providers.html

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
    print(json.dumps(columns, sort_keys=True, indent=4))
    mapping = None
    if mapping_file:
        mapping = json.load(mapping_file)

    bulk_size = min(num_records, bulk_size)
    num_inserts = int(math.ceil(num_records / bulk_size))

    gen_row = create_row_generator(columns, mapping)

    stmt = to_insert(table, columns)[0]
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
        tasks = asyncio.gather(
            _gen_data_and_insert(q, client, stmt, gen_row, bulk_seq),
            consume(q, total=num_inserts)
        )
        loop.run_until_complete(tasks)


def main():
    argh.dispatch_command(insert_fake_data)


if __name__ == '__main__':
    main()
