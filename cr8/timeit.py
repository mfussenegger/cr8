#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from cr8 import aio
from cr8.cli import lines_from_stdin, to_int
from cr8.misc import as_statements
from cr8.log import Logger
from cr8.clients import client_errors
from cr8.engine import Runner, Result, eval_fail_if


@argh.arg('--hosts', help='crate hosts', type=str)
@argh.arg('-s', '--stmt', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('--duration',
          type=to_int,
          help='Duration in seconds. Overwrites repeat if set.')
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--fail-if', help='An expression which causes cr8 to exit with a\
          failure if it evaluates to true')
@argh.arg('--sample-mode', choices=('all', 'reservoir'),
          help='Method used for sampling', default='reservoir')
@argh.wrap_errors([KeyboardInterrupt, BrokenPipeError] + client_errors)
def timeit(hosts=None,
           stmt=None,
           warmup=30,
           repeat=None,
           duration=None,
           concurrency=1,
           output_fmt=None,
           fail_if=None,
           sample_mode='reservoir'):
    """Run the given statement a number of times and return the runtime stats

    Args:
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
    num_lines = 0
    log = Logger(output_fmt)
    with Runner(hosts, concurrency, sample_mode) as runner:
        version_info = aio.run(runner.client.get_server_version)
        for line in as_statements(lines_from_stdin(stmt)):
            runner.warmup(line, warmup)
            timed_stats = runner.run(line, iterations=repeat, duration=duration)
            r = Result(
                version_info=version_info,
                statement=line,
                timed_stats=timed_stats,
                concurrency=concurrency
            )
            log.result(r)
            if fail_if:
                eval_fail_if(fail_if, r)
        num_lines += 1
    if num_lines == 0:
        raise SystemExit(
            'No SQL statements provided. Use --stmt or provide statements via stdin')


def main():
    argh.dispatch_command(timeit)


if __name__ == '__main__':
    main()
