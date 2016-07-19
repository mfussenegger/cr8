import os
import doctest
import subprocess
import functools
from unittest import main
from cr8.run_crate import CrateNode, get_crate
from crate.client import connect


crate_dir = get_crate('latest-nightly')
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
        c.execute('create table x.demo (id int, name string, country string) \
                  with (number_of_replicas = 0)')
        c.execute('create blob table blobtable with (number_of_replicas = 0)')


def teardown(*args):
    with connect(node.http_url) as conn:
        c = conn.cursor()
        c.execute('drop table x.demo')
        c.execute('drop blob table blobtable')
    node.stop()


def transform(s):
    s = s.replace('localhost:4200', node.http_url or 'localhost:44200')
    return (
        r'print(sh("""%s""").stdout.decode("utf-8"))' % s) + '\n'


class Parser(doctest.DocTestParser):

    def parse(self, string, name='<string>'):
        r = super().parse(string, name)
        for s in r:
            if isinstance(s, doctest.Example):
                s.source = transform(s.source)
        return r


def load_tests(loader, tests, ignore):
    env = os.environ.copy()
    env['CR8_NO_TQDM'] = 'True'
    tests.addTests(doctest.DocFileSuite(
        os.path.join('..', 'README.rst'),
        globs={
            'sh': functools.partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=30,
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
    main()
