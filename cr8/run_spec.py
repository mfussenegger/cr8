import argh
import os
import itertools
from time import time
from functools import partial
from crate.client import connect

from cr8 import aio
from .insert_json import to_insert
from .bench_spec import load_spec
from .timeit import QueryRunner, Result
from .misc import as_bulk_queries, as_statements, get_lines
from .metrics import Stats
from .cli import dicts_from_lines
from . import clients


BENCHMARK_TABLE = '''
create table if not exists benchmarks (
    version_info object (strict) as (
        number string,
        hash string,
        date timestamp
    ),
    statement string,
    meta object as (
        name string
    ),
    started timestamp,
    ended timestamp,
    concurrency int,
    bulk_size int,
    runtime_stats object (strict) as (
        avg double,
        min double,
        max double,
        mean double,
        median double,
        percentile object as (
            "50" double,
            "75" double,
            "90" double,
            "99" double,
            "99_9" double
        ),
        n integer,
        variance double,
        stdev double,
        samples array(double)
    )
) clustered into 8 shards with (number_of_replicas = '1-3', column_policy='strict')
'''


def _parse_version(version: str) -> tuple:
    if not version:
        return None
    major, minor, patch = version.split('.', maxsplit=3)
    return (int(major), int(minor), int(patch))


class Executor:
    def __init__(self, spec_dir, benchmark_hosts, result_hosts, output_fmt=None):
        self.benchmark_hosts = benchmark_hosts
        self.spec_dir = spec_dir
        self.conn = connect(benchmark_hosts)
        self.client = clients.client(benchmark_hosts)
        self.output_fmt = output_fmt
        self.server_version_info = aio.run(self.client.get_server_version)
        self.server_version = _parse_version(self.server_version_info['number'])

        if result_hosts:
            table_created = []

            def process_result(result):
                with connect(result_hosts) as conn:
                    cursor = conn.cursor()
                    if not table_created:
                        cursor.execute(BENCHMARK_TABLE)
                        table_created.append(None)
                    stmt, args = to_insert('benchmarks', result.as_dict())
                    cursor.execute(stmt, args)
                print(result)
                print('')
        else:
            def process_result(result):
                print(result)
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

        for data_file in instructions.data_files:
            inserts = as_bulk_queries(self._to_inserts(data_file),
                                      data_file.get('bulk_size', 5000))
            concurrency = data_file.get('concurrency', 25)
            aio.run_many(self.client.execute_many, inserts, concurrency=concurrency)
            cursor.execute('refresh table {target}'.format(target=data_file['target']))

    def run_load_data(self, data_spec, meta=None):
        inserts = self._to_inserts(data_spec)
        statement = next(iter(inserts))[0]
        bulk_size = data_spec.get('bulk_size', 5000)
        inserts = as_bulk_queries(self._to_inserts(data_spec), bulk_size)
        concurrency = data_spec.get('concurrency', 25)
        num_records = data_spec.get('num_records', None)
        if num_records:
            num_records = max(1, int(num_records / bulk_size))
        stats = Stats()
        f = partial(aio.measure, stats, self.client.execute_many)
        start = time()
        aio.run_many(f,
                     inserts,
                     concurrency=concurrency,
                     num_items=num_records)
        end = time()
        self.process_result(Result(
            version_info=self.server_version_info,
            statement=statement,
            meta=meta,
            started=start,
            ended=end,
            stats=stats,
            concurrency=concurrency,
            bulk_size=bulk_size,
            output_fmt=self.output_fmt
        ))

    def _skip_message(self, min_version, stmt):
        msg = ('## Skipping (Version {server_version} instead of {min_version}):\n'
               '   Statement: {statement:.70}')
        msg = msg.format(
            statement=stmt,
            min_version='.'.join((str(x) for x in min_version)),
            server_version='.'.join((str(x) for x in self.server_version)))
        return msg

    def run_queries(self, queries, meta=None):
        for query in queries:
            stmt = query['statement']
            iterations = query.get('iterations', 1)
            concurrency = query.get('concurrency', 1)
            min_version = _parse_version(query.get('min_version'))
            if min_version and min_version > self.server_version:
                print(self._skip_message(min_version, stmt))
                continue
            print(('\n## Running Query:\n'
                   '   Statement: {statement:.70}\n'
                   '   Concurrency: {concurrency}\n'
                   '   Iterations: {iterations}'.format(
                       statement=str(stmt),
                       iterations=iterations,
                       concurrency=concurrency)))
            with QueryRunner(
                stmt,
                meta,
                repeats=iterations,
                hosts=self.benchmark_hosts,
                concurrency=concurrency,
                args=query.get('args'),
                bulk_args=query.get('bulk_args'),
                output_fmt=self.output_fmt
            ) as runner:
                result = runner.run()
            self.process_result(result)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.conn.close()
        self.client.close()


@argh.arg('benchmark_hosts', type=str)
@argh.arg('-of', '--output-fmt', choices=['full', 'short'], default='full')
@argh.wrap_errors([KeyboardInterrupt])
def run_spec(spec, benchmark_hosts, result_hosts=None, output_fmt=None):
    """Run a spec file, executing the statements on the benchmark_hosts.

    Short example of a spec file:

        [setup]
        statement_files = ["sql/create_table.sql"]

            [[setup.data_files]]
            target = "t"
            source = "data/t.json"

        [[queries]]
        statement = "select count(*) from t"
        iterations = 2000
        concurrency = 10

        [teardown]
        statements = ["drop table t"]

    See https://github.com/mfussenegger/cr8/tree/master/specs
    for more examples.

    Args:
        spec: path to a spec file
        benchmark_hosts: hostname[:port] pairs of Crate nodes
        result_hosts: optional hostname[:port] Crate node pairs into which the
            runtime statistics should be inserted.
        output_fmt: output format
    """
    with Executor(
        spec_dir=os.path.dirname(spec),
        benchmark_hosts=benchmark_hosts,
        result_hosts=result_hosts,
        output_fmt=output_fmt
    ) as executor:
        spec = load_spec(spec)
        try:
            print('# Running setUp')
            executor.exec_instructions(spec.setup)
            print('# Running benchmark')
            if spec.load_data:
                for data_spec in spec.load_data:
                    executor.run_load_data(data_spec, spec.meta)
            else:
                executor.run_queries(spec.queries, spec.meta)
        finally:
            print('# Running tearDown')
            executor.exec_instructions(spec.teardown)


def main():
    argh.dispatch_command(run_spec)


if __name__ == "__main__":
    main()
