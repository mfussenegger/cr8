#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
from pprint import pprint
from urllib.request import urlopen
import itertools
import collections

from functools import partial
from time import time
from crate.client import connect

from .cli import lines_from_stdin, to_int
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
    def __init__(self, stmt, repeats, hosts, concurrency):
        self.stmt = stmt
        self.hosts = hosts
        self.repeats = repeats
        self.concurrency = concurrency
        self.conn = conn = connect(hosts)
        cursor = conn.cursor()
        self.loop = loop = aio.asyncio.get_event_loop()
        self.execute = partial(aio.execute, loop, cursor)

    def warmup(self, num_warmup):
        statements = itertools.repeat((self.stmt,), num_warmup)
        aio.run(self.execute, statements, 0, loop=self.loop)

    def run(self):
        version_info = self.get_version_info(self.conn.client.active_servers[0])

        started = time()
        statements = itertools.repeat((self.stmt,), self.repeats)
        stats = Stats(min(self.repeats, 1000))
        measure = partial(aio.measure, stats, self.execute)

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

    @staticmethod
    def get_version_info(server):
        r = urlopen(server)
        data = json.loads(r.read().decode('utf-8'))
        return {
            'number': data['version']['number'],
            'hash': data['version']['build_hash']
        }


@argh.arg('hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
@argh.arg('-c', '--concurrency', type=to_int)
def timeit(hosts, stmt=None, warmup=30, repeat=30, concurrency=1):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    for line in lines_from_stdin(stmt):
        runner = QueryRunner(line, repeat, hosts, concurrency)
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
