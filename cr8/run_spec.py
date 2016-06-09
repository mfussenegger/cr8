import argh
import os
import itertools
from time import time
from functools import partial
from pprint import pprint
from crate.client import connect

from cr8 import aio
from .insert_json import to_insert
from .bench_spec import load_spec
from .timeit import QueryRunner, Result
from .misc import as_bulk_queries, as_statements, get_lines
from .metrics import Stats
from .cli import dicts_from_lines


class Executor:
    def __init__(self, spec_dir, benchmark_hosts, result_hosts):
        self.benchmark_hosts = benchmark_hosts
        self.spec_dir = spec_dir
        self.conn = connect(benchmark_hosts)
        self.loop = aio.asyncio.get_event_loop()

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

    def _to_inserts(self, data_spec):
        target = data_spec['target']
        source = os.path.join(self.spec_dir, data_spec['source'])
        dicts = dicts_from_lines(get_lines(source))
        return (to_insert(target, d) for d in dicts)

    def exec_instructions(self, instructions):
        cursor = self.conn.cursor()
        filenames = instructions.statement_files
        filenames = (os.path.join(self.spec_dir, i) for i in filenames)
        lines = (line for fn in filenames for line in get_lines(fn))
        statements = itertools.chain(as_statements(lines), instructions.statements)
        for stmt in statements:
            cursor.execute(stmt)

        loop = self.loop
        f = partial(aio.execute_many, loop, cursor)
        for data_file in instructions.data_files:
            inserts = as_bulk_queries(self._to_inserts(data_file),
                                      data_file.get('bulk_size', 5000))
            concurrency = data_file.get('concurrency', 50)
            aio.run(f, inserts, concurrency=concurrency, loop=loop)
            cursor.execute('refresh table {target}'.format(target=data_file['target']))

    def run_load_data(self, data_spec):
        inserts = self._to_inserts(data_spec)
        statement = next(iter(inserts))[0]
        bulk_size = data_spec.get('bulk_size', 5000)
        inserts = as_bulk_queries(self._to_inserts(data_spec), bulk_size)
        concurrency = data_spec.get('concurrency', 50)
        num_records = data_spec.get('num_records', None)
        if num_records:
            num_records = max(1, int(num_records / bulk_size))
        stats = Stats()
        cursor = self.conn.cursor()
        f = partial(aio.measure, stats, partial(aio.execute_many, self.loop, cursor))
        start = time()
        aio.run(f,
                inserts,
                concurrency=concurrency,
                loop=self.loop,
                num_items=num_records)
        end = time()
        self.process_result(Result(
            version_info=QueryRunner.get_version_info(self.conn.client.active_servers[0]),
            statement=statement,
            started=start,
            ended=end,
            stats=stats,
            concurrency=concurrency,
            bulk_size=bulk_size
        ))

    def run_queries(self, queries):
        for query in queries:
            pprint(query)
            stmt = query['statement']
            iterations = query.get('iterations', 1)
            concurrency = query.get('concurrency', 1)
            runner = QueryRunner(
                stmt,
                repeats=iterations,
                hosts=self.benchmark_hosts,
                concurrency=concurrency
            )
            result = runner.run()
            self.process_result(result)


def run_spec(spec, benchmark_hosts, result_hosts=None):
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
        if spec.load_data:
            for data_spec in spec.load_data:
                executor.run_load_data(data_spec)
        else:
            executor.run_queries(spec.queries)
    finally:
        yield 'Running tearDown'
        executor.exec_instructions(spec.teardown)


def main():
    argh.dispatch_command(run_spec)


if __name__ == "__main__":
    main()
