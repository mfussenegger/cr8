import argh
import os
import itertools
from functools import partial
from pprint import pprint
from crate.client import connect

from cr8 import aio
from .json2insert import to_insert
from .bench_spec import load_spec
from .timeit import QueryRunner
from .misc import as_bulk_queries, as_statements, get_lines
from .cli import dicts_from_lines


def get_concurrency_range(concurrency):
    if isinstance(concurrency, int):
        return range(concurrency)
    elif isinstance(concurrency, list):
        return range(concurrency[0], concurrency[1], concurrency[2])
    raise ValueError('Invalid concurrency: {}'.format(concurrency))


class Executor:
    def __init__(self, spec_dir, benchmark_hosts, result_hosts):
        self.benchmark_hosts = benchmark_hosts
        self.spec_dir = spec_dir
        self.conn = connect(benchmark_hosts)

        if result_hosts:
            def process_result(result):
                conn = connect(result_hosts)
                cursor = conn.cursor()
                stmt, args = to_insert('benchmarks', result.__dict__)
                cursor.execute(stmt, args)
                pprint(result.__dict__)
                print('')
        else:
            def process_result(result):
                pprint(result.__dict__)
                print('')
        self.process_result = process_result

    def exec_instructions(self, instructions):
        cursor = self.conn.cursor()
        filenames = instructions.statement_files
        filenames = (os.path.join(self.spec_dir, i) for i in filenames)
        lines = (line for fn in filenames for line in get_lines(fn))
        statements = itertools.chain(as_statements(lines), instructions.statements)
        for stmt in statements:
            cursor.execute(stmt)

        for data_file in instructions.data_files:
            target = data_file['target']
            source = os.path.join(self.spec_dir, data_file['source'])
            dicts = dicts_from_lines(get_lines(source))
            inserts = (to_insert(target, d) for d in dicts)
            inserts = as_bulk_queries(inserts, 5000)
            loop = aio.asyncio.get_event_loop()
            f = partial(aio.execute_many, loop, cursor)
            aio.run(f, inserts, concurrency=100, loop=loop)
            cursor.execute('refresh table {target}'.format(target=target))

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
                    hosts=self.benchmark_hosts,
                    concurrency=concurrency
                )
                result = runner.run()
                self.process_result(result)


def bench(spec, benchmark_hosts, result_hosts=None):
    executor = Executor(
        spec_dir=os.path.dirname(spec),
        benchmark_hosts=benchmark_hosts,
        result_hosts=result_hosts,
    )
    spec = load_spec(spec)
    try:
        yield 'Running setUp'
        executor.exec_instructions(spec.setup)
        yield 'Running benchmark'
        executor.run_queries(spec.queries)
    finally:
        yield 'Running tearDown'
        executor.exec_instructions(spec.teardown)


def main():
    argh.dispatch_command(bench)


if __name__ == "__main__":
    main()
