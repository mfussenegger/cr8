import os
import doctest
import subprocess
from unittest import main
from cr8.run_crate import CrateNode, get_crate
from crate.client import connect


crate_dir = get_crate('latest-testing')
node = CrateNode(
    crate_dir=crate_dir,
    settings={
        'cluster.name': 'cr8-tests',
        'http.port': '44200-44250'
    })


def setup(*args):
    node.start()
    with connect(node.http_url) as conn:
        c = conn.cursor()
        c.execute('create table demo (name string, country string) \
                  with (number_of_replicas = 0)')
        c.execute('create blob table blobtable with (number_of_replicas = 0)')
        benchmarks_table = os.path.join(os.path.dirname(__file__),
                                        '..', 'sql', 'benchmarks_table.sql')
        with open(benchmarks_table) as f:
            c.execute(f.read().strip().rstrip(';'))


def teardown(*args):
    with connect(node.http_url) as conn:
        c = conn.cursor()
        c.execute('drop table demo')
        c.execute('drop blob table blobtable')
    node.stop()


def transform(s):
    s = s.replace('localhost:4200', node.http_url or 'localhost:44200')
    return (
        r'print(sh("""%s""", stdin=PIPE, stdout=PIPE, stderr=STDOUT, shell=True).stdout.decode("utf-8"))' % s) + '\n'


class Parser(doctest.DocTestParser):

    def parse(self, string, name='<string>'):
        r = super().parse(string, name)
        for s in r:
            if isinstance(s, doctest.Example):
                s.source = transform(s.source)
        return r


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocFileSuite(
        os.path.join('..', 'README.rst'),
        globs={
            'sh': subprocess.run,
            'STDOUT': subprocess.STDOUT,
            'PIPE': subprocess.PIPE,
        },
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
        setUp=setup,
        tearDown=teardown,
        parser=Parser()
    ))
    return tests


if __name__ == "__main__":
    main()
