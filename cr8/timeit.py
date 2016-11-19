#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from .cli import lines_from_stdin, to_int
from .clients import client_errors
from .engine import Runner


@argh.arg('--hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-of', '--output-fmt', choices=['full', 'short'], default='full')
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
    for line in lines_from_stdin(stmt):
        with Runner(line,
                    None,
                    repeat,
                    hosts=hosts,
                    concurrency=concurrency,
                    output_fmt=output_fmt) as runner:
            runner.warmup(warmup)
            result = runner.run()
        print(result)
        num_lines += 1
    if num_lines == 0:
        raise SystemExit(
            'No SQL statements provided. Use --stmt or provide statements via stdin')


def main():
    argh.dispatch_command(timeit)


if __name__ == '__main__':
    main()
