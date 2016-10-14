import json
import aiohttp
import itertools
import calendar
import time
from datetime import datetime, date
from typing import List, Union, Iterable
from decimal import Decimal
from urllib.parse import urlparse, parse_qs

try:
    import aiopg
except ImportError:
    aiopg = None


class CrateJsonEncoder(json.JSONEncoder):

    epoch = datetime(1970, 1, 1)

    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, datetime):
            delta = o - self.epoch
            return int(delta.total_seconds() * 1000)
        if isinstance(o, date):
            return calendar.timegm(o.timetuple()) * 1000
        return json.JSONEncoder.default(self, o)


class SqlException(Exception):

    def __init__(self, message):
        self.message = message


client_errors = [
    SqlException,
    aiohttp.ClientError
]


def _to_http_uri(s: str) -> str:
    """Prefix the string with 'http://' if there is no schema."""
    if not s.startswith(('http://', 'https://')):
        return 'http://' + s
    return s


def _to_http_hosts(hosts: Union[Iterable[str], str]) -> List[str]:
    """Convert a string of whitespace or comma separated hosts into a list of hosts.

    Hosts may also already be a list or other iterable.
    Each host will be prefixed with 'http://' if it is not already there.

    >>> _to_http_hosts('n1:4200,n2:4200')
    ['http://n1:4200', 'http://n2:4200']

    >>> _to_http_hosts('n1:4200 n2:4200')
    ['http://n1:4200', 'http://n2:4200']

    >>> _to_http_hosts('https://n1:4200')
    ['https://n1:4200']

    >>> _to_http_hosts(['http://n1:4200', 'n2:4200'])
    ['http://n1:4200', 'http://n2:4200']
    """
    if isinstance(hosts, str):
        hosts = hosts.replace(',', ' ').split()
    return [_to_http_uri(i) for i in hosts]


async def _exec(session, url, data):
    async with session.post(url, data=data) as resp:
        r = await resp.json()
        if 'error' in r:
            raise SqlException(r['error']['message'])
        return r


def _plain_or_callable(obj):
    """Returns the value of the called object of obj is a callable,
    otherwise the plain object.
    Returns None if obj is None.

    >>> obj = None
    >>> _plain_or_callable(obj)

    >>> stmt = 'select * from sys.nodes'
    >>> _plain_or_callable(stmt)
    'select * from sys.nodes'

    >>> def _args():
    ...     return [1, 'name']
    >>> _plain_or_callable(_args)
    [1, 'name']

    >>> class BulkArgsGenerator:
    ...     def __call__(self):
    ...         return [[1, 'foo'], [2, 'bar'], [3, 'foobar']]
    >>> _plain_or_callable(BulkArgsGenerator())
    [[1, 'foo'], [2, 'bar'], [3, 'foobar']]
    """
    return obj() if callable(obj) else obj


def _date_or_none(d: str) -> str:
    """Return a date as if, if valid, otherwise None

    >>> _date_or_none('2017-02-27')
    '2017-02-27'

    >>> _date_or_none('NA')
    """
    try:
        datetime.strptime(d, '%Y-%m-%d')
        return d
    except ValueError:
        return None


def _to_dsn(hosts):
    """Convert a host URI into a dsn for aiopg.

    >>> _to_dsn('aiopg://myhostname:4242/mydb')
    'dbname=mydb host=myhostname port=4242'

    >>> _to_dsn('aiopg://myhostname:4242')
    'dbname=doc host=myhostname port=4242'

    >>> _to_dsn('aiopg://myhostname:4242/doc?sslmode=require')
    'dbname=doc host=myhostname port=4242 sslmode=require'

    >>> _to_dsn('myhostname')
    Traceback (most recent call last):
        ...
    ValueError: Port is missing
    """
    p = urlparse(hosts)
    try:
        host, port = p.netloc.split(':', maxsplit=1)
    except ValueError:
        raise ValueError('Port is missing')
    dsn = 'dbname={dbname} host={host} port={port}'.format(
        dbname=p.path[1:] if p.path else 'doc',
        host=host,
        port=port)
    if p.query:
        dsn += ' ' + ' '.join(k + '=' + v[0] for k, v in parse_qs(p.query).items())
    return dsn


class AiopgClient:
    def __init__(self, hosts, pool_size=25):
        self.dsn = _to_dsn(hosts)
        self.pool_size = pool_size
        self._pool = None

    async def _get_pool(self):
        if not self._pool:
            self._pool = await aiopg.create_pool(
                self.dsn,
                timeout=10,
                maxsize=self.pool_size,
                enable_hstore=False,
                enable_uuid=False
            )
        return self._pool

    async def execute(self, stmt, args=None):
        start = time.time()
        pool = await self._get_pool()
        with (await pool.cursor()) as cur:
            await cur.execute(stmt, args)
            rows = await cur.fetchall()
            return {
                'duration': (time.time() - start) * 1000.,
                'rows': rows
            }

    async def execute_many(self, stmt, bulk_args):
        start = time.time()
        pool = await self._get_pool()
        with (await pool.cursor()) as cur:
            await cur.executemany(stmt, bulk_args)
            rows = await cur.fetchall()
            return {
                'duration': (time.time() - start) * 1000.,
                'rows': rows
            }

    async def get_server_version(self):
        pool = await self._get_pool()
        with (await pool.cursor()) as cur:
            await cur.execute('select version from sys.nodes')
            for (version,) in cur:
                return {
                    'hash': version['build_hash'],
                    'number': version['number']
                }

    def close(self):
        if self._pool:
            self._pool.close()

    def __enter__(self):
        return self

    def __exit__(self, *exs):
        self.close()


class HttpClient:
    def __init__(self, hosts, conn_pool_limit=25):
        self.hosts = hosts
        self.urls = itertools.cycle([i + '/_sql' for i in hosts])
        self._conn_pool_limit = conn_pool_limit
        self._session = None

    async def get_session(self):
        if not self._session:
            conn = aiohttp.TCPConnector(limit=self._conn_pool_limit)
            self._session = aiohttp.ClientSession(connector=conn)
        return self._session

    async def execute(self, stmt, args=None):
        payload = {'stmt': _plain_or_callable(stmt)}
        if args:
            payload['args'] = _plain_or_callable(args)
        session = await self.get_session()
        return await _exec(session, next(self.urls), json.dumps(payload, cls=CrateJsonEncoder))

    async def execute_many(self, stmt, bulk_args):
        data = json.dumps(dict(
            stmt=_plain_or_callable(stmt),
            bulk_args=_plain_or_callable(bulk_args)
        ), cls=CrateJsonEncoder)
        session = await self.get_session()
        return await _exec(session, next(self.urls), data)

    async def get_server_version(self):
        session = await self.get_session()
        async with session.get(self.hosts[0] + '/') as resp:
            r = await resp.json()
            version = r['version']
            return {
                'hash': version['build_hash'],
                'number': version['number'],
                'date': _date_or_none(version['build_timestamp'][:10])
            }

    def close(self):
        if self._session:
            self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def client(hosts, concurrency=25):
    hosts = hosts or 'localhost:4200'
    if hosts.startswith('aiopg://'):
        if not aiopg:
            raise ValueError('Cannot use "aiopg" scheme if aiopg is not available')
        return AiopgClient(hosts, pool_size=concurrency)
    return HttpClient(_to_http_hosts(hosts), conn_pool_limit=concurrency)
