import itertools
from functools import partial
from time import time, perf_counter
from collections import namedtuple

from cr8 import aio
from cr8.metrics import Stats, get_sampler
from cr8.clients import client


TimedStats = namedtuple('TimedStats', ['started', 'ended', 'stats'])


class FailIf(SystemExit):
    pass


def eval_fail_if(fail_if: str, result):
    fail_if = fail_if.format(**result.as_dict())
    if eval(fail_if):
        raise FailIf("Expression failed: " + fail_if)


class DotDict(dict):

    __getattr__ = dict.__getitem__


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 timed_stats,
                 concurrency,
                 meta=None,
                 bulk_size=None):
        self.version_info = version_info
        self.statement = str(statement)
        self.meta = meta and DotDict(meta) or None
        self.started = timed_stats.started
        self.ended = timed_stats.ended
        self.runtime_stats = DotDict(timed_stats.stats.get())
        self.concurrency = concurrency
        self.bulk_size = bulk_size

    def as_dict(self):
        return self.__dict__


def run_and_measure(f, statements, concurrency, num_items=None, sampler=None):
    stats = Stats(sampler)
    measure = partial(aio.measure, stats, f)
    started = int(time() * 1000)
    aio.run_many(measure, statements, concurrency, num_items=num_items)
    ended = int(time() * 1000)
    return TimedStats(started, ended, stats)


def _generate_statements(stmt, args, iterations, duration):
    if duration is None:
        yield from itertools.repeat((stmt, args), iterations or 100)
    else:
        now = perf_counter()
        while perf_counter() - now < duration:
            yield (stmt, args)


class Runner:
    def __init__(self, hosts, concurrency, sample_mode):
        self.concurrency = concurrency
        self.client = client(hosts, concurrency=concurrency)
        self.sampler = get_sampler(sample_mode)

    def warmup(self, stmt, num_warmup):
        statements = itertools.repeat((stmt,), num_warmup)
        aio.run_many(self.client.execute, statements, 0, num_items=num_warmup)

    def run(self, stmt, *, iterations=None, duration=None, args=None, bulk_args=None):
        if bulk_args:
            args = bulk_args
            f = self.client.execute_many
        else:
            f = self.client.execute
        statements = _generate_statements(stmt, args, iterations, duration)
        return run_and_measure(
            f, statements, self.concurrency, iterations, sampler=self.sampler)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()
