#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import sys
from functools import partial
from argparse import FileType

from .cli import dicts_from_lines, to_int
from .misc import as_bulk_queries
from cr8 import aio, clients
from .metrics import Stats
from .log import format_stats


def to_insert(table, d):
    """Generate an insert statement using the given table and dictionary.

    Args:
        table (str): table name
        d (dict): dictionary with column names as keys and values as values.
    Returns:
        tuple of statement and arguments

    >>> to_insert('doc.foobar', {'name': 'Marvin'})
    ('insert into doc.foobar ("name") values ($1)', ['Marvin'])
    """

    columns = []
    args = []
    for key, val in d.items():
        columns.append('"{}"'.format(key))
        args.append(val)
    stmt = 'insert into {table} ({columns}) values ({params})'.format(
        table=table,
        columns=', '.join(columns),
        params=', '.join(f'${i + 1}' for i in range(len(columns)))
    )
    return (stmt, args)


def print_only(lines, table):
    for d in dicts_from_lines(lines):
        print(to_insert(table, d))
    print('')
    print('No hosts provided. Nothing inserted')


def _instrument_prometheus(port, measure):
    try:
        from prometheus_client import start_http_server, Histogram
    except ImportError:
        print("Couldn't instrument prometheus metrics")
        return
    h = Histogram('request_latency_ms', 'Request latencies in ms')

    def alt_measure(val):
        measure(val)
        h.observe(val)

    start_http_server(port)
    print(f'Prometheus metrics: http://localhost:{port}/')
    return alt_measure


@argh.arg('--table', help='Target table', required=True)
@argh.arg('-b', '--bulk-size', type=to_int)
@argh.arg('--hosts', help='crate hosts which will be used \
          to execute the insert statement', type=str)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-i', '--infile', type=FileType('r', encoding='utf-8'), default=sys.stdin)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--prometheus-port', type=to_int)
@argh.wrap_errors([KeyboardInterrupt, BrokenPipeError] + clients.client_errors)
def insert_json(table=None,
                bulk_size=1000,
                concurrency=25,
                hosts=None,
                infile=None,
                output_fmt=None,
                prometheus_port=None):
    """Insert JSON lines from a file or stdin into a CrateDB cluster.

    If no hosts are specified the statements will be printed.

    Args:
        table: Target table name.
        bulk_size: Bulk size of the insert statements.
        concurrency: Number of operations to run concurrently.
        hosts: hostname:port pairs of the Crate nodes
    """
    if not hosts:
        return print_only(infile, table)

    queries = (to_insert(table, d) for d in dicts_from_lines(infile))
    bulk_queries = as_bulk_queries(queries, bulk_size)
    print('Executing inserts: bulk_size={} concurrency={}'.format(
        bulk_size, concurrency), file=sys.stderr)

    stats = Stats()
    if prometheus_port:
        measure = _instrument_prometheus(prometheus_port, stats.measure)
    else:
        measure = stats.measure

    with clients.client(hosts, concurrency=concurrency) as client:
        f = partial(aio.measure, measure, client.execute_many)
        try:
            aio.run_many(f, bulk_queries, concurrency)
        except clients.SqlException as e:
            raise SystemExit(str(e))
    try:
        print(format_stats(stats.get(), output_fmt))
    except KeyError:
        if not stats.sampler.values:
            raise SystemExit('No data received via stdin')
        raise


def main():
    argh.dispatch_command(insert_json)


if __name__ == '__main__':
    main()
