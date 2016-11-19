
import json
import itertools
from functools import partial
from time import time
from collections import namedtuple

from . import aio
from .metrics import Stats
from .clients import client


TimedStats = namedtuple('TimedStats', ['started', 'ended', 'stats'])


to_jsonstr = partial(json.dumps, sort_keys=True, indent=4)


def _format_short(stats):
    output = ('Runtime (in ms):\n'
              '    mean:    {mean:.3f} ± {error_margin:.3f}')
    values = dict(
        mean=stats['mean'],
        error_margin=stats.get('error_margin', 0.0),
    )
    if stats['n'] > 1:
        output += (
            '\n'
            '    min/max: {min:.3f} → {max:.3f}\n'
            'Percentile:\n'
            '    50:   {p50:.3f} ± {stdev:.3f} (stdev)\n'
            '    95:   {p95:.3f}\n'
            '    99.9: {p999:.3f}'
        )
        percentiles = stats['percentile']
        values.update(dict(
            max=stats['max'],
            min=stats['min'],
            stdev=stats['stdev'],
            p50=percentiles['50'],
            p95=percentiles['95'],
            p999=percentiles['99_9']
        ))
    return output.format(**values)


def format_stats(stats, output_fmt=None):
    output_fmt = output_fmt or 'full'
    if output_fmt == 'full':
        return to_jsonstr(stats)
    return _format_short(stats)


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 timed_stats,
                 concurrency,
                 meta=None,
                 bulk_size=None,
                 output_fmt='full'):
        self.version_info = version_info
        self.statement = str(statement)
        self.meta = meta
        self.started = timed_stats.started
        self.ended = timed_stats.ended
        self.runtime_stats = timed_stats.stats.get()
        self.concurrency = concurrency
        self.bulk_size = bulk_size

        # copy before str_func is assigned
        # because str_func shouldn't be part of the printed result
        self.d = self.__dict__.copy()
        if output_fmt == 'full':
            self.str_func = partial(to_jsonstr, self.d)
        elif output_fmt == 'short':
            self.str_func = partial(_format_short, self.runtime_stats)
        else:
            raise ValueError('Invalid output format: {}'.format(output_fmt))

    def as_dict(self):
        return self.d

    def __str__(self):
        return self.str_func()


def run_and_measure(f, statements, concurrency, num_items=None):
    stats = Stats(min(num_items or 1000, 1000))
    measure = partial(aio.measure, stats, f)
    started = int(time() * 1000)
    aio.run_many(measure, statements, concurrency, num_items=num_items)
    ended = int(time() * 1000)
    return TimedStats(started, ended, stats)


class Runner:
    def __init__(self, hosts, concurrency):
        self.concurrency = concurrency
        self.client = client(hosts, concurrency=concurrency)

    def warmup(self, stmt, num_warmup):
        statements = itertools.repeat((stmt,), num_warmup)
        aio.run_many(self.client.execute, statements, 0, num_items=num_warmup)

    def run(self, stmt, iterations, args=None, bulk_args=None):
        if bulk_args:
            args = bulk_args
            f = self.client.execute_many
        else:
            f = self.client.execute
        statements = itertools.repeat((stmt, args), iterations)
        return run_and_measure(f, statements, self.concurrency, iterations)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()
