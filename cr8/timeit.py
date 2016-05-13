#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import argh
import requests
import statistics

from time import time
from crate.client import connect
from concurrent.futures import ThreadPoolExecutor, wait

from .cli import lines_from_stdin, to_int


executor = ThreadPoolExecutor(20)


class Result:
    def __init__(self,
                 version_info,
                 statement,
                 started,
                 ended,
                 repeats,
                 client_runtimes,
                 server_runtimes):
        self.version_info = version_info
        self.statement = statement
        # need ts in ms in crate
        self.started = int(started * 1000)
        self.ended = int(ended * 1000)
        self.repeats = repeats
        self.client_runtimes = client_runtimes
        self.server_runtimes = server_runtimes

        runtimes = self.server_runtimes
        avg = sum(runtimes) / float(len(runtimes))
        self.runtime_stats = {
            'avg': round(avg, 6),
            'min': round(min(runtimes), 6),
            'max': round(max(runtimes), 6),
            'stdev': round(statistics.stdev(runtimes), 6),
            'pvariance': round(statistics.pvariance(runtimes), 6),
        }

    def __str__(self):
        return json.dumps(self.__dict__)


class QueryRunner:
    def __init__(self, stmt, repeats, hosts):
        self.stmt = stmt
        self.hosts = hosts
        self.repeats = repeats
        self.conn = connect(hosts)

    def warmup(self, num_warmup):
        futures = []
        for i in range(num_warmup):
            c = self.conn.cursor()
            stmt = self.stmt
            futures.append(executor.submit(lambda: c.execute(stmt)))
        wait(futures)

    def run(self):
        version_info = self.__get_version_info(self.conn.client.active_servers[0])

        started = time()
        client_runtimes = []
        server_runtimes = []
        cursor = self.conn.cursor()
        for i in range(self.repeats):
            start = time()
            cursor.execute(self.stmt)
            client_runtimes.append(round(time() - start, 3))
            server_runtimes.append(cursor.duration / 1000.)
        ended = time()

        return Result(
            statement=self.stmt,
            version_info=version_info,
            started=started,
            ended=ended,
            repeats=self.repeats,
            client_runtimes=client_runtimes,
            server_runtimes=server_runtimes
        )

    def __get_version_info(self, server):
        data = requests.get(server).json()
        return {
            'number': data['version']['number'],
            'hash': data['version']['build_hash']
        }


@argh.arg('hosts', help='crate hosts', type=str)
@argh.arg('-w', '--warmup', type=to_int)
@argh.arg('-r', '--repeat', type=to_int)
def timeit(hosts, stmt=None, warmup=30, repeat=30):
    """ runs the given statement a number of times and returns the runtime stats
    """
    num_lines = 0
    for line in lines_from_stdin(stmt):
        runner = QueryRunner(line, repeat, hosts)
        runner.warmup(warmup)
        result = runner.run()
        yield result
        num_lines += 1
    if num_lines == 0:
        raise SystemExit(
            'No SQL statements provided. Use --stmt or provide statements via stdin')


def main():
    argh.dispatch_command(timeit)


if __name__ == '__main__':
    main()
