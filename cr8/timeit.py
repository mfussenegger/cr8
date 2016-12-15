#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from . import aio
from .cli import lines_from_stdin, to_int
from .misc import as_statements
from .log import Logger
from .clients import client_errors
from .engine import Runner, Result


@argh.arg('--hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.wrap_errors([KeyboardInterrupt] + client_errors)
def timeit(hosts=None,
           stmt=None,
           warmup=30,
           repeat=30,
           concurrency=1,
           output_fmt=None):
    """Run the given statement a number of times and return the runtime stats
    """
    num_lines = 0
    log = Logger(output_fmt)
    with Runner(hosts, concurrency) as runner:
        version_info = aio.run(runner.client.get_server_version)
        for line in as_statements(lines_from_stdin(stmt)):
            runner.warmup(line, warmup)
            timed_stats = runner.run(line, repeat)
            log.result(Result(
                version_info=version_info,
                statement=line,
                timed_stats=timed_stats,
                concurrency=concurrency
            ))
        num_lines += 1
    if num_lines == 0:
        raise SystemExit(
            'No SQL statements provided. Use --stmt or provide statements via stdin')


def main():
    argh.dispatch_command(timeit)


if __name__ == '__main__':
    main()
