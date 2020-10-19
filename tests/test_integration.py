import os
import doctest
import subprocess
import functools
from unittest import TestCase

from cr8.run_crate import CrateNode, get_crate
from cr8.clients import client
from cr8 import aio


crate_dir = get_crate('latest-testing')
node = CrateNode(
    crate_dir=crate_dir,
    settings={
        'cluster.name': 'cr8-tests',
        'http.port': '44200-44250'
    })


def setup(*args):
    with client(node.http_url) as c:
        aio.run(
            c.execute,
            'create table x.demo (id int, name string, country string) \
            with (number_of_replicas = 0)'
        )
        aio.run(c.execute, 'create table y.demo (name text) with (number_of_replicas = 0)')
        aio.run(c.execute, 'create blob table blobtable with (number_of_replicas = 0)')


def teardown(*args):
    try:
        with client(node.http_url) as c:
            aio.run(c.execute, 'drop table x.demo')
            aio.run(c.execute, 'drop blob table blobtable')
    finally:
        node.stop()


def transform(s):
    s = s.replace('localhost:4200', node.http_url)
    s = s.replace(
        'asyncpg://localhost:5432',
        f'asyncpg://{node.addresses.psql.host}:{node.addresses.psql.port}')
    s = s.replace(
        'postgresql://crate@localhost:5432/doc',
        f'postgresql://crate@{node.addresses.psql.host}:{node.addresses.psql.port}/doc')
    return (
        r'print(sh("""%s""").stdout.decode("utf-8"))' % s) + '\n'


class Parser(doctest.DocTestParser):

    def parse(self, string, name='<string>'):
        r = super().parse(string, name)
        for s in r:
            if isinstance(s, doctest.Example):
                s.source = transform(s.source)
        return r


class SourceBuildTest(TestCase):

    def test_build_from_branch(self):
        self.assertIsNotNone(get_crate('4.1'))


def load_tests(loader, tests, ignore):
    env = os.environ.copy()
    env['CR8_NO_TQDM'] = 'True'
    node.start()
    assert node.http_host, "http_url must be available"
    tests.addTests(doctest.DocFileSuite(
        os.path.join('..', 'README.rst'),
        globs={
            'sh': functools.partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=60,
                shell=True,
                env=env
            )
        },
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
        setUp=setup,
        tearDown=teardown,
        parser=Parser()
    ))
    return tests


if __name__ == "__main__":
    import unittest
    unittest.main()
