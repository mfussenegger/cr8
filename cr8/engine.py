import itertools
import shutil
import subprocess
import json
import threading
import select
from concurrent.futures import Future
from functools import partial
from time import time, perf_counter
from collections import namedtuple

from cr8 import aio
from cr8.metrics import Stats, get_sampler
from cr8.clients import client, _plain_or_callable
from cr8.aio import tqdm


TimedStats = namedtuple('TimedStats', ['started', 'ended', 'stats'])


class FailIf(SystemExit):
    pass


def eval_fail_if(fail_if: str, result):
    fail_if = fail_if.format(runtime_stats=result.runtime_stats,
                             statement=result.statement,
                             meta=result.meta,
                             concurrency=result.concurrency,
                             bulk_size=result.bulk_size)
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


def create_runner(hosts, concurrency, sample_mode):
    if shutil.which('cr8hs'):
        return HsRunner(hosts, concurrency, sample_mode)
    else:
        return Runner(hosts, concurrency, sample_mode)


class OutputReader:

    def __init__(self, stats, iterations):
        self.stats = stats
        self.iterations = iterations
        self._future = Future()

    def _consume(self, proc):
        for line in tqdm(proc.stdout, total=self.iterations):
            self.stats.measure(float(line))
        self._future.set_result(None)

    def wait(self):
        self._future.result()

    def start(self, proc):
        t = threading.Thread(target=self._consume, args=(proc,))
        t.daemon = True
        t.start()


class HsRunner:

    def __init__(self, hosts, concurrency, sample_mode):
        self.client = client(hosts, concurrency=concurrency)
        self.sampler = get_sampler(sample_mode)
        self.hosts = hosts
        self.proc = None
        self.concurrency = concurrency

    def warmup(self, stmt, num_warmup):
        statements = itertools.repeat((stmt,), num_warmup)
        aio.run_many(self.client.execute, statements, 0, num_items=num_warmup)

    def run(self, stmt, *, iterations=None, duration=None, args=None, bulk_args=None):
        self.proc = subprocess.Popen(
            ['cr8hs',
             '--hosts', self.hosts,
             '--concurrency', str(self.concurrency),
             ],
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True
        )
        if bulk_args:
            args = bulk_args
            mode = 'bulk'
        else:
            mode = 'single'
        started = int(time() * 1000)
        stats = Stats(self.sampler)
        statements = _generate_statements(stmt, args, iterations, duration)
        reader = OutputReader(stats, iterations)
        reader.start(self.proc)
        for stmt, args in statements:
            self.proc.stdin.write(json.dumps({
                'mode': mode,
                'stmt': _plain_or_callable(stmt),
                'args': _plain_or_callable(args) or []
            }) + '\n')
        self.proc.stdin.write('QUIT\n')
        self.proc.stdin.flush()
        reader.wait()
        ended = int(time() * 1000)
        return TimedStats(started, ended, stats)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()
        if self.proc:
            self.proc.terminate()


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
