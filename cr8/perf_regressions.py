#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
from crate.client import connect
from crate.client.exceptions import ProgrammingError
from cr8.timeit import timeit
from cr8.json2insert import to_insert


def _get_runtimes(hosts):
    conn = connect(hosts)
    c = conn.cursor()
    c.execute("select min(runtime_stats['avg']), statement from benchmarks group by statement")
    rows = c.fetchall()
    for min_avg, statement in rows:
        c.execute("select runtime_stats['avg'] from benchmarks "
                  "where statement = ? order by ended desc limit 1", (statement,))
        yield min_avg, c.fetchall()[0][0], statement


def _query_supported(cursor, statement):
    try:
        cursor.execute(statement)
        return True
    except ProgrammingError:
        return False


@argh.arg('benchmark_hosts', type=str, nargs='+',
          help='hosts of the crate cluster which will be used to run the queries')
@argh.arg('log_host', type=str,
          help='host of the crate cluster where there is the table which contains benchmark results')
def find_perf_regressions(benchmark_hosts, log_host):
    """ finds performance regressions by running recorded queries

    Reads the benchmark table from the log_host and runs all queries again
    against the benchmark_hosts. It will compare the new runtimes with the
    previous runs and print runtime details.

    The new query runtimes are also persitet to the log_host.
    """
    runtimes = _get_runtimes(log_host)
    regressions = []

    conn = connect(benchmark_hosts)
    benchmark_cursor = conn.cursor()

    conn = connect(log_host)
    log_cursor = conn.cursor()
    for min_avg, last_avg, statement in runtimes:
        if not _query_supported(benchmark_cursor, statement):
            yield 'Skipping statement as it run into an error: {}'.format(statement)
            continue
        yield 'Running: {}'.format(statement)
        result = next(timeit(benchmark_hosts, stmt=statement, warmup=5))
        yield 'Runtime: best: {} previous: {} current: {}'.format(
            min_avg, last_avg, result.runtime_stats['avg'])
        if (result.runtime_stats['avg'] * 1.05) > min_avg:
            regressions.append((vars(result), min_avg))

        d = vars(result).copy()
        del d['client_runtimes']
        del d['server_runtimes']
        stmt, args = to_insert('benchmarks', d)
        log_cursor.execute(stmt, args)
    if any(regressions):
        raise SystemExit(json.dumps(regressions, indent=4))
    else:
        yield json.dumps(regressions, indent=4)


def main():
    argh.dispatch_command(find_perf_regressions)


if __name__ == '__main__':
    main()
