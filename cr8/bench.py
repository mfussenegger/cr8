#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argh
import json
import os
from crate.client import connect
from crate.crash.command import CrateCmd

from cr8.json2insert import async_inserts, to_insert
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
        else:
            def process_result(result):
                pass
        self.process_result = process_result

    def exec_instructions(self, instructions):
        filenames = instructions.get('statement_files', [])
        filenames = (os.path.join(self.spec_dir, i) for i in filenames)
        lines = (line for fn in filenames for line in get_lines(fn))
        for line in lines:
            self.cmd.process(line)

        for stmt in instructions.get('statements', []):
            self.cmd.execute(stmt)

        data_files = instructions.get('data_files', [])
        for data_file in data_files:
            target = data_file['target']
            source = os.path.join(self.spec_dir, data_file['source'])
            dicts = read_dicts(source)
            async_inserts(dicts, self.cmd.connection.cursor(), target, 1000)

    def run_benchmark(self, instructions):
        result = next(timeit(
            hosts=self.cmd.connection.client.active_servers,
            stmt=instructions['statement'],
            repeat=instructions['repeats']))
        self.process_result(result)


def bench(spec, benchmark_hosts, result_hosts=None):
    bench_conn = connect(benchmark_hosts)
    executor = Executor(
        cmd=CrateCmd(connection=bench_conn),
        spec_dir=os.path.dirname(spec),
        result_hosts=result_hosts,
    )
    with open(spec, 'r', encoding='utf-8') as spec_file:
        spec = json.load(spec_file)
    try:
        yield 'Running setUp'
        executor.exec_instructions(spec.get('setUp', {}))
        yield 'Running benchmark'
        executor.run_benchmark(spec['benchmark'])
    finally:
        yield 'Running tearDown'
        executor.exec_instructions(spec.get('tearDown', {}))


def main():
    argh.dispatch_command(bench)


if __name__ == "__main__":
    main()
