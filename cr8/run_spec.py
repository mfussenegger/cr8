import argh
import re
import os
import itertools
import subprocess
from functools import partial
from typing import Iterable

from cr8 import aio, clients
from cr8.insert_json import to_insert
from cr8.bench_spec import load_spec
from cr8.engine import Runner, Result, run_and_measure, eval_fail_if
from cr8.misc import (
    as_bulk_queries,
    as_statements,
    get_lines,
    parse_version,
    try_len
)
from cr8.cli import dicts_from_lines
from cr8.log import Logger


BENCHMARK_TABLE = '''
create table if not exists benchmarks (
    version_info object (strict) as (
        number text,
        hash text,
        date timestamp
    ),
    name text,
    statement text,
    meta object as (
        name text
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
    def __init__(self,
                 spec_dir,
                 benchmark_hosts,
                 result_hosts,
                 log,
                 fail_if,
                 sample_mode):
        self.benchmark_hosts = benchmark_hosts
        self.sample_mode = sample_mode
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
        if fail_if:
            self.fail_if = partial(eval_fail_if, fail_if)
        else:
            self.fail_if = lambda x: None
        if result_hosts:
            self.process_result = _result_to_crate(self.log, self.result_client)
        else:
            self.process_result = log.result

    def _to_inserts(self, data_spec):
        target = data_spec['target']
        source = data_spec['source']
        if not source.startswith(('http://', 'https://')):
            source = os.path.join(self.spec_dir, source)
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
            if self.client.is_cratedb:
                aio.run(self.client.execute, f"refresh table {data_file['target']}")

        for data_cmd in instructions.data_cmds:
            process = subprocess.Popen(
                data_cmd['cmd'],
                stdout=subprocess.PIPE,
                universal_newlines=True
            )
            target = data_cmd['target']
            dicts = dicts_from_lines(process.stdout)
            inserts = as_bulk_queries(
                (to_insert(target, d) for d in dicts),
                data_cmd.get('bulk_size', 5000)
            )
            concurrency = data_cmd.get('concurrency', 25)
            aio.run_many(self.client.execute_many, inserts, concurrency=concurrency)
            if self.client.is_cratedb:
                aio.run(self.client.execute, f"refresh table {target}")

    def update_server_stats(self):
        """Triggers ANALYZE on the server to update statistics."""
        try:
            aio.run(self.client.execute, 'ANALYZE')
        except Exception:
            pass  # swallow; CrateDB 4.1.0+ is required to run ANALYZE

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

    def run_queries(self, queries: Iterable[dict], meta=None):
        for query in queries:
            stmt = query['statement']
            iterations = query.get('iterations', 1)
            warmup = query.get('warmup', 0)
            duration = query.get('duration')
            name = query.get('name')
            concurrency = query.get('concurrency', 1)
            args = query.get('args')
            bulk_args = query.get('bulk_args')
            _min_version = query.get('min_version')
            min_version = _min_version and parse_version(_min_version)
            if min_version and min_version > self.server_version:
                self.log.info(self._skip_message(min_version, stmt))
                continue
            mode_desc = 'Duration' if duration else 'Iterations'
            name_line = name and f'   Name: {name}\n' or ''
            self.log.info(
                (f'\n## Running Query:\n'
                 f'{name_line}'
                 f'   Statement:\n'
                 f'     {stmt}\n'
                 f'   Concurrency: {concurrency}\n'
                 f'   {mode_desc}: {duration or iterations}')
            )
            with Runner(self.benchmark_hosts, concurrency, self.sample_mode) as runner:
                if warmup > 0:
                    runner.warmup(stmt, warmup, concurrency, args)
                timed_stats = runner.run(
                    stmt,
                    iterations=iterations,
                    duration=duration,
                    args=args,
                    bulk_args=bulk_args
                )
            result = self.create_result(
                statement=stmt,
                meta=meta,
                timed_stats=timed_stats,
                concurrency=concurrency,
                bulk_size=try_len(bulk_args),
                name=name
            )
            self.process_result(result)
            self.fail_if(result)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()
        self.result_client.close()


def do_run_spec(spec,
                benchmark_hosts,
                *,
                log,
                sample_mode,
                result_hosts=None,
                action=None,
                fail_if=None,
                re_name=None):
    with Executor(
        spec_dir=os.path.dirname(spec),
        benchmark_hosts=benchmark_hosts,
        result_hosts=result_hosts,
        log=log,
        fail_if=fail_if,
        sample_mode=sample_mode
    ) as executor:
        spec = load_spec(spec)
        try:
            if not action or 'setup' in action:
                log.info('# Running setUp')
                executor.exec_instructions(spec.setup)
                executor.update_server_stats()
            log.info('# Running benchmark')
            if spec.load_data and (not action or 'load_data' in action):
                for data_spec in spec.load_data:
                    executor.run_load_data(data_spec, spec.meta)
            if spec.queries and (not action or 'queries' in action):
                if re_name:
                    rex = re.compile(re_name)
                    queries = (q for q in spec.queries if 'name' in q and rex.match(q['name']))
                else:
                    queries = spec.queries
                executor.run_queries(queries, spec.meta)
        finally:
            if not action or 'teardown' in action:
                log.info('# Running tearDown')
                executor.exec_instructions(spec.teardown)


@argh.arg('benchmark_hosts', type=str)
@argh.arg('-r', '--result_hosts', type=str)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--action',
          choices=['setup', 'teardown', 'queries', 'load_data'],
          action='append')
@argh.arg('--logfile-info', help='Redirect info messages to a file')
@argh.arg('--logfile-result', help='Redirect benchmark results to a file')
@argh.arg('--sample-mode', choices=('all', 'reservoir'),
          help='Method used for sampling', default='reservoir')
@argh.arg('--re-name', type=str, help='Regex used to filter queries executed by name')
@argh.wrap_errors([KeyboardInterrupt, BrokenPipeError] + clients.client_errors)
def run_spec(spec,
             benchmark_hosts,
             result_hosts=None,
             output_fmt=None,
             logfile_info=None,
             logfile_result=None,
             action=None,
             fail_if=None,
             sample_mode='reservoir',
             re_name=None):
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
        fail-if: An expression that causes cr8 to exit with a failure if it
            evaluates to true.
            The expression can contain formatting expressions for:
                - runtime_stats
                - statement
                - meta
                - concurrency
                - bulk_size
            For example:
                --fail-if "{runtime_stats.mean} > 1.34"
    """
    with Logger(output_fmt=output_fmt,
                logfile_info=logfile_info,
                logfile_result=logfile_result) as log:
        do_run_spec(
            spec=spec,
            benchmark_hosts=benchmark_hosts,
            log=log,
            result_hosts=result_hosts,
            action=action,
            fail_if=fail_if,
            sample_mode=sample_mode,
            re_name=re_name
        )


def main():
    argh.dispatch_command(run_spec)


if __name__ == "__main__":
    main()
