#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argh
import json
import os
from crate.client import connect
from crate.crash.command import CrateCmd

from cr8.json2insert import async_inserts, to_insert
from cr8.bench_spec import load_spec
from cr8.timeit import timeit


def get_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            yield line


def read_dicts(filename):
    for line in get_lines(filename):
        yield json.loads(line)


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
                print(result.runtime_stats)
        else:
            def process_result(result):
                print(result.runtime_stats)
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
            dicts = read_dicts(source)
            async_inserts(dicts, self.cmd.connection.cursor(), target, 1000)

    def run_benchmark(self, queries, bench_config):
        for stmt, args in queries:
            result = next(timeit(
                hosts=self.cmd.connection.client.active_servers,
                stmt=stmt,
                repeat=bench_config.get('repeats', 100)))
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
        executor.run_benchmark(spec.queries, spec.config)
    finally:
        yield 'Running tearDown'
        executor.exec_instructions(spec.teardown)


def main():
    argh.dispatch_command(bench)


if __name__ == "__main__":
    main()
