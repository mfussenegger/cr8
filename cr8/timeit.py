#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
import itertools

from functools import partial
from time import time
from crate.client import connect

from .cli import lines_from_stdin, to_int, to_hosts
from .metrics import Stats
from . import aio


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 started,
                 ended,
                 stats,
                 concurrency,
                 bulk_size=None):
        self.version_info = version_info
        self.statement = statement
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.runtime_stats = stats.get()
        self.concurrency = concurrency
        self.bulk_size = bulk_size

    def __str__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=4)


class QueryRunner:
    def __init__(self, stmt, repeats, hosts, concurrency, args=None, bulk_args=None):
        self.stmt = stmt
        self.repeats = repeats
        self.concurrency = concurrency
        self.loop = aio.asyncio.get_event_loop()
        self.host = next(iter(hosts))
        self.client = aio.Client(hosts, conn_pool_limit=concurrency)
        self.bulk_args = bulk_args
        self.args = args

    def warmup(self, num_warmup):
        statements = itertools.repeat((self.stmt,), num_warmup)
        aio.run(self.client.execute, statements, 0, loop=self.loop)

    def run(self):
        version_info = self.get_version_info(self.host)

        started = time()
        if self.bulk_args:
            statements = itertools.repeat((self.stmt, self.bulk_args), self.repeats)
            f = self.client.execute_many
        else:
            statements = itertools.repeat((self.stmt, self.args), self.repeats)
            f = self.client.execute
        stats = Stats(min(self.repeats, 1000))
        measure = partial(aio.measure, stats, f)

        aio.run(measure, statements, self.concurrency, loop=self.loop)
        ended = time()

        return Result(
            statement=self.stmt,
            version_info=version_info,
            started=started,
            ended=ended,
            stats=stats,
            concurrency=self.concurrency
        )

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.client.close()

    @staticmethod
    def get_version_info(server):
        with connect(server) as conn:
            c = conn.cursor()
            c.execute('select version from sys.nodes limit 1')
            version = c.fetchone()[0]
            return {
                'hash': version['build_hash'],
                'number': version['number']
            }


@argh.arg('--hosts', help='crate hosts', type=to_hosts)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
def timeit(hosts=None, stmt=None, warmup=30, repeat=30, concurrency=1):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    hosts = hosts or ['http://localhost:4200']
    for line in lines_from_stdin(stmt):
        with QueryRunner(line, repeat, hosts, concurrency) as runner:
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
