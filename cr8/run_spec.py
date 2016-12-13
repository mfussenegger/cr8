import argh
import os
import itertools
from functools import partial

from cr8 import aio
from .insert_json import to_insert
from .bench_spec import load_spec
from .engine import Runner, Result, run_and_measure
from .misc import (
    as_bulk_queries,
    as_statements,
    get_lines,
    parse_version,
    try_len
)
from .cli import dicts_from_lines
from .log import Logger
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
        error_margin double,
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


def _result_to_crate(log, client):
    table_created = []

    def save_result(result):
        if not table_created:
            aio.run(client.execute, BENCHMARK_TABLE)
            table_created.append(None)
        stmt, args = to_insert('benchmarks', result.as_dict())
        aio.run(client.execute, stmt, args)
        log.result(result)

    return save_result


class Executor:
    def __init__(self, spec_dir, benchmark_hosts, result_hosts, log):
        self.benchmark_hosts = benchmark_hosts
        self.spec_dir = spec_dir
        self.client = clients.client(benchmark_hosts)
        self.result_client = clients.client(result_hosts)
        self.server_version_info = aio.run(self.client.get_server_version)
        self.server_version = parse_version(self.server_version_info['number'])
        self.log = log
        self.create_result = partial(
            Result,
            version_info=self.server_version_info
        )
        if result_hosts:
            self.process_result = _result_to_crate(self.log, self.result_client)
        else:
            self.process_result = log.result

    def _to_inserts(self, data_spec):
        target = data_spec['target']
        source = os.path.join(self.spec_dir, data_spec['source'])
        dicts = dicts_from_lines(get_lines(source))
        return (to_insert(target, d) for d in dicts)

    def exec_instructions(self, instructions):
        filenames = instructions.statement_files
        filenames = (os.path.join(self.spec_dir, i) for i in filenames)
        lines = (line for fn in filenames for line in get_lines(fn))
        statements = itertools.chain(as_statements(lines), instructions.statements)
        for stmt in statements:
            aio.run(self.client.execute, stmt)

        for data_file in instructions.data_files:
            inserts = as_bulk_queries(self._to_inserts(data_file),
                                      data_file.get('bulk_size', 5000))
            concurrency = data_file.get('concurrency', 25)
            aio.run_many(self.client.execute_many, inserts, concurrency=concurrency)
            aio.run(self.client.execute, 'refresh table {target}'.format(target=data_file['target']))

    def run_load_data(self, data_spec, meta=None):
        inserts = self._to_inserts(data_spec)
        statement = next(iter(inserts))[0]
        bulk_size = data_spec.get('bulk_size', 5000)
        inserts = as_bulk_queries(self._to_inserts(data_spec), bulk_size)
        concurrency = data_spec.get('concurrency', 25)
        num_records = data_spec.get('num_records')
        if num_records:
            num_records = max(1, int(num_records / bulk_size))
        timed_stats = run_and_measure(
            self.client.execute_many, inserts, concurrency, num_records)
        self.process_result(self.create_result(
            statement=statement,
            meta=meta,
            timed_stats=timed_stats,
            concurrency=concurrency,
            bulk_size=bulk_size,
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
            args = query.get('args')
            bulk_args = query.get('bulk_args')
            min_version = parse_version(query.get('min_version'))
            if min_version and min_version > self.server_version:
                self.log.info(self._skip_message(min_version, stmt))
                continue
            self.log.info(('\n## Running Query:\n'
                           '   Statement: {statement:.70}\n'
                           '   Concurrency: {concurrency}\n'
                           '   Iterations: {iterations}'.format(
                               statement=str(stmt),
                               iterations=iterations,
                               concurrency=concurrency)))
            with Runner(self.benchmark_hosts, concurrency) as runner:
                timed_stats = runner.run(stmt, iterations, args, bulk_args)
            self.process_result(self.create_result(
                statement=stmt,
                meta=meta,
                timed_stats=timed_stats,
                concurrency=concurrency,
                bulk_size=try_len(bulk_args)
            ))

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()
        self.result_client.close()


def do_run_spec(spec, benchmark_hosts, log, result_hosts=None, action=None):
    with Executor(
        spec_dir=os.path.dirname(spec),
        benchmark_hosts=benchmark_hosts,
        result_hosts=result_hosts,
        log=log
    ) as executor:
        spec = load_spec(spec)
        try:
            if not action or 'setup' in action:
                log.info('# Running setUp')
                executor.exec_instructions(spec.setup)
            log.info('# Running benchmark')
            if spec.load_data and (not action or 'load_data' in action):
                for data_spec in spec.load_data:
                    executor.run_load_data(data_spec, spec.meta)
            if spec.queries and (not action or 'queries' in action):
                executor.run_queries(spec.queries, spec.meta)
        finally:
            if not action or 'teardown' in action:
                log.info('# Running tearDown')
                executor.exec_instructions(spec.teardown)


@argh.arg('benchmark_hosts', type=str)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--action',
          choices=['setup', 'teardown', 'queries', 'load_data'],
          action='append')
@argh.arg('--logfile-info', help='Redirect info messages to a file')
@argh.arg('--logfile-result', help='Redirect benchmark results to a file')
@argh.wrap_errors([KeyboardInterrupt] + clients.client_errors)
def run_spec(spec,
             benchmark_hosts,
             result_hosts=None,
             output_fmt=None,
             logfile_info=None,
             logfile_result=None,
             action=None):
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
        action: Optional action to execute.
            Default is to execute all actions - setup, queries and teardown.
            If present only the specified action will be executed.
            The argument can be provided multiple times to execute more than
            one action.
    """
    with Logger(output_fmt=output_fmt,
                logfile_info=logfile_info,
                logfile_result=logfile_result) as log:
        do_run_spec(spec, benchmark_hosts, log, result_hosts, action)


def main():
    argh.dispatch_command(run_spec)


if __name__ == "__main__":
    main()
