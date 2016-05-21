#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argh
import os
from functools import partial
from pprint import pprint
from crate.client import connect
from crate.crash.command import CrateCmd
from appmetrics import metrics
from time import time

from cr8 import aio
from .json2insert import to_insert
from .bench_spec import load_spec
from .timeit import QueryRunner
from .misc import as_bulk_queries
from .cli import dicts_from_lines
from .stats import Result


def get_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            yield line


def get_concurrency_range(concurrency):
    if isinstance(concurrency, int):
        return range(concurrency)
    elif isinstance(concurrency, list):
        return range(concurrency[0], concurrency[1], concurrency[2])
    raise ValueError('Invalid concurrency: {}'.format(concurrency))


class Executor:
    def __init__(self, cmd, spec_dir, result_hosts):
        self.cmd = cmd
        self.spec_dir = spec_dir

        if result_hosts:
            def process_result(result):
                conn = connect(result_hosts)
                cursor = conn.cursor()
                stmt, args = to_insert('benchmarks', result.__dict__)
                cursor.execute(stmt, args)
                pprint(result.__dict__)
        else:
            def process_result(result):
                pprint(result.__dict__)
        self.process_result = process_result

    def exec_instructions(self, instructions):
        filenames = instructions.statement_files
        filenames = (os.path.join(self.spec_dir, i) for i in filenames)
        lines = (line for fn in filenames for line in get_lines(fn))
        for line in lines:
            self.cmd.process(line)

        for stmt in instructions.statements:
            self.cmd.execute(stmt)

        for data_file in instructions.data_files:
            target = data_file['target']
            source = os.path.join(self.spec_dir, data_file['source'])
            dicts = dicts_from_lines(get_lines(source))
            inserts = (to_insert(target, d) for d in dicts)
            inserts = as_bulk_queries(inserts, 5000)
            loop = aio.asyncio.get_event_loop()
            f = partial(aio.execute_many, loop, self.cmd.connection.cursor())
            aio.run(f, inserts, concurrency=100, loop=loop)

    def run_load_data(self, load_data):
        target = load_data['target']
        source = os.path.join(self.spec_dir, load_data['source'])
        concurrency_range = get_concurrency_range(load_data.get('concurrency', 100))
        bulk_size = load_data.get('bulk_size', 5000)
        loop = aio.asyncio.get_event_loop()
        execute_many = partial(aio.execute_many, loop, self.cmd.connection.cursor())

        dicts = dicts_from_lines(get_lines(source))
        inserts = (to_insert(target, d) for d in dicts)
        statement = next(iter(inserts))[0]

        for concurrency in concurrency_range:
            dicts = dicts_from_lines(get_lines(source))
            inserts = (to_insert(target, d) for d in dicts)
            inserts = as_bulk_queries(inserts, bulk_size)
            hist = metrics.new_histogram(
                'durations-load-to-{}-from-{}-{}'.format(target, source, concurrency))
            f = partial(aio.measure, hist, execute_many)
            started = time()
            aio.run(f, inserts, concurrency=concurrency, loop=loop)
            ended = time()
            self.process_result(Result(
                statement=statement,
                version_info=QueryRunner.get_version_info(
                    self.cmd.connection.client.active_servers[0]),
                started=started,
                ended=ended,
                repeats=1,
                concurrency=concurrency,
                bulk_size=bulk_size,
                stats=hist.get()
            ))

    def run_queries(self, queries):
        for query in queries:
            pprint(query)
            stmt = query['statement']
            iterations = query.get('iterations', 1)
            concurrency_range = get_concurrency_range(query.get('concurrency', 1))
            for concurrency in concurrency_range:
                runner = QueryRunner(
                    stmt,
                    repeats=iterations,
                    hosts=self.cmd.connection.client.active_servers,
                    concurrency=concurrency
                )
                result = runner.run()
                self.process_result(result)


def bench(spec, benchmark_hosts, result_hosts=None):
    bench_conn = connect(benchmark_hosts)
    executor = Executor(
        cmd=CrateCmd(connection=bench_conn),
        spec_dir=os.path.dirname(spec),
        result_hosts=result_hosts,
    )
    spec = load_spec(spec)
    try:
        yield 'Running setUp'
        executor.exec_instructions(spec.setup)
        yield 'Running benchmark'
        if spec.load_data:
            executor.run_load_data(spec.load_data)
        else:
            executor.run_queries(spec.queries)
    finally:
        yield 'Running tearDown'
        executor.exec_instructions(spec.teardown)


def main():
    argh.dispatch_command(bench)


if __name__ == "__main__":
    main()
