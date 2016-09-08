#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
import itertools

from functools import partial
from time import time

from .cli import lines_from_stdin, to_int
from .clients import client
from .metrics import Stats
from . import aio


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 meta,
                 started,
                 ended,
                 stats,
                 concurrency,
                 bulk_size=None,
                 output_fmt=None):
        self.version_info = version_info
        self.statement = str(statement)
        self.meta = meta
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.runtime_stats = stats.get()
        self.concurrency = concurrency
        self.bulk_size = bulk_size

        # copy before str_func is assigned
        # because str_func shouldn't be part of the printed result
        self.d = self.__dict__.copy()
        output_fmt = output_fmt or 'full'
        if output_fmt == 'full':
            self.str_func = partial(self.as_json_string, self.d)
        elif output_fmt == 'short':
            self.str_func = partial(self.short_output, self.runtime_stats)
        else:
            raise ValueError('Invalid output format: {}'.format(output_fmt))

    @staticmethod
    def as_json_string(d):
        return json.dumps(d, sort_keys=True, indent=4)

    def as_dict(self):
        return self.d

    @staticmethod
    def format_stats(stats, output_fmt=None):
        output_fmt = output_fmt or 'full'
        if output_fmt == 'full':
            return Result.as_json_string(stats)
        else:
            return Result.short_output(stats)

    @staticmethod
    def short_output(stats):
        output = ('Runtime:\n'
                  '    mean: {mean:.3f} +/- {stdev:.3f}\n'
                  '    min:  {min:.3f}\n'
                  '    max:  {max:.3f}')
        values = dict(
            mean=stats['mean'],
            max=stats['max'],
            min=stats['min'],
            stdev=stats.get('stdev', 0.0)
        )
        if stats['n'] > 1:
            output += (
                '\n'
                'Percentile:\n'
                '    50:   {p50:.3f}\n'
                '    99.9: {p999:.3f}'
            )
            values.update(dict(
                p50=stats['percentile']['50'],
                p999=stats['percentile']['99_9']
            ))
        return output.format(**values)

    def __str__(self):
        return self.str_func()


class QueryRunner:
    def __init__(self,
                 stmt,
                 meta,
                 repeats,
                 hosts,
                 concurrency,
                 args=None,
                 bulk_args=None,
                 output_fmt=None):
        self.stmt = stmt
        self.meta = meta
        self.repeats = repeats
        self.concurrency = concurrency
        self.hosts = hosts
        self.client = client(hosts, concurrency=concurrency)
        self.bulk_args = bulk_args
        self.args = args
        self.output_fmt = output_fmt

    def warmup(self, num_warmup):
        statements = itertools.repeat((self.stmt,), num_warmup)
        aio.run_many(self.client.execute, statements, 0, num_items=num_warmup)

    def run(self):
        version_info = aio.run(self.client.get_server_version)
        started = time()
        if self.bulk_args:
            statements = itertools.repeat((self.stmt, self.bulk_args), self.repeats)
            f = self.client.execute_many
        else:
            statements = itertools.repeat((self.stmt, self.args), self.repeats)
            f = self.client.execute
        stats = Stats(min(self.repeats, 1000))
        measure = partial(aio.measure, stats, f)

        aio.run_many(
            measure, statements, self.concurrency, num_items=self.repeats)
        ended = time()

        return Result(
            statement=self.stmt,
            meta=self.meta,
            version_info=version_info,
            started=started,
            ended=ended,
            stats=stats,
            concurrency=self.concurrency,
            output_fmt=self.output_fmt
        )

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()


@argh.arg('--hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-of', '--output-fmt', choices=['full', 'short'], default='full')
def timeit(hosts=None,
           stmt=None,
           warmup=30,
           repeat=30,
           concurrency=1,
           output_fmt=None):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    for line in lines_from_stdin(stmt):
        with QueryRunner(line,
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
