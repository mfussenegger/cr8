from cr8.clients import client
from cr8 import aio
from cr8.run_crate import get_crate, CrateNode


crate_dir = get_crate('latest-testing')
node = CrateNode(
    crate_dir=crate_dir,
    settings={
        'cluster.name': 'cr8-tests',
        'http.port': '44200-44250',
        'cluster.routing.allocation.disk.threshold_enabled': 'false',
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
    with client(node.http_url) as c:
        aio.run(c.execute, 'drop table x.demo')
        aio.run(c.execute, 'drop table y.demo')
        aio.run(c.execute, 'drop blob table blobtable')


def translate(s):
    """
    Translate canonical database addresses to match the ones provided by the test layer.
    """
    s = s.replace('localhost:4200', node.http_url)
    s = s.replace(
        'asyncpg://localhost:5432',
        f'asyncpg://{node.addresses.psql.host}:{node.addresses.psql.port}')
    s = s.replace(
        'postgresql://crate@localhost:5432/doc',
        f'postgresql://crate@{node.addresses.psql.host}:{node.addresses.psql.port}/doc')
    return s


def transform(s):
    """
    Transform all commands parsed from doctests.
    """
    s = translate(s)
    return (
        r'print(sh("""%s""").stdout.decode("utf-8"))' % s) + '\n'
