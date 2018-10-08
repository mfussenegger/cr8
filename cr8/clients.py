import json
import aiohttp
import itertools
import calendar
import types
import time
from urllib.parse import urlparse, parse_qs
from datetime import datetime, date
from typing import List, Union, Iterable
from decimal import Decimal
from urllib.parse import urlparse, parse_qs
from cr8.aio import asyncio  # import via aio for uvloop setup

try:
    import asyncpg
except ImportError:
    asyncpg = None


HTTP_DEFAULT_HDRS = {'Content-Type': 'application/json'}


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
if asyncpg:
    client_errors.append(asyncpg.exceptions.PostgresError)


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
    async with session.post(url,
                            data=data,
                            headers=HTTP_DEFAULT_HDRS,
                            timeout=None) as resp:
        if resp.status == 401:
            t = await resp.text()
            raise SqlException(t)
        r = await resp.json()
        if 'error' in r:
            raise SqlException(
                r['error']['message'] + ' occurred using: ' + str(data))
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

    >>> _plain_or_callable((x for x in range(10)))
    0

    >>> class BulkArgsGenerator:
    ...     def __call__(self):
    ...         return [[1, 'foo'], [2, 'bar'], [3, 'foobar']]
    >>> _plain_or_callable(BulkArgsGenerator())
    [[1, 'foo'], [2, 'bar'], [3, 'foobar']]
    """
    if callable(obj):
        return obj()
    elif isinstance(obj, types.GeneratorType):
        return next(obj)
    else:
        return obj


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
    'postgres://crate@myhostname:4242/mydb'

    >>> _to_dsn('aiopg://myhostname:4242')
    'postgres://crate@myhostname:4242/doc'

    >>> _to_dsn('aiopg://hoschi:pw@myhostname:4242/doc?sslmode=require')
    'postgres://hoschi:pw@myhostname:4242/doc?sslmode=require'

    >>> _to_dsn('aiopg://myhostname')
    'postgres://crate@myhostname:5432/doc'
    """
    p = urlparse(hosts)
    try:
        user_and_pw, netloc = p.netloc.split('@', maxsplit=1)
    except ValueError:
        netloc = p.netloc
        user_and_pw = 'crate'
    try:
        host, port = netloc.split(':', maxsplit=1)
    except ValueError:
        host = netloc
        port = 5432
    dbname = p.path[1:] if p.path else 'doc'
    dsn = f'postgres://{user_and_pw}@{host}:{port}/{dbname}'
    if p.query:
        dsn += '?' + '&'.join(k + '=' + v[0] for k, v in parse_qs(p.query).items())
    return dsn


def _to_boolean(v):
    if str(v).lower() in ("true"):
        return True
    elif str(v).lower() in ("false"):
        return False
    else:
        raise ValueError('not a boolean value')


def _verify_ssl_from_first(hosts):
    """Check if SSL validation parameter is passed in URI

    >>> _verify_ssl_from_first(['https://myhost:4200/?verify_ssl=false'])
    False

    >>> _verify_ssl_from_first(['https://myhost:4200/'])
    True

    >>> _verify_ssl_from_first([
    ...      'https://h1:4200/?verify_ssl=False',
    ...      'https://h2:4200/?verify_ssl=True'
    ... ])
    False
    """
    for host in hosts:
        query = parse_qs(urlparse(host).query)
        if 'verify_ssl' in query:
            return _to_boolean(query['verify_ssl'][0])
    return True


class AsyncpgClient:
    def __init__(self, hosts, pool_size=25):
        self.dsn = _to_dsn(hosts)
        self.pool_size = pool_size
        self._pool = None

    async def _get_pool(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=self.pool_size,
                max_size=self.pool_size
            )
        return self._pool

    async def execute(self, stmt, args=None):
        start = time.time()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            if args:
                rows = await conn.fetch(stmt, *args)
            else:
                rows = await conn.fetch(stmt)
            return {
                'duration': (time.time() - start) * 1000.,
                'rows': rows
            }

    async def execute_many(self, stmt, bulk_args):
        start = time.time()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(stmt, bulk_args)
            return {
                'duration': (time.time() - start) * 1000.,
                'rows': []
            }

    async def get_server_version(self):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            for (version,) in await conn.fetch('select version from sys.nodes'):
                version = json.loads(version)
                return {
                    'hash': version['build_hash'],
                    'number': version['number']
                }

    async def _close_pool(self):
        if self._pool:
            await self._pool.close()
            self._pool = None

    def close(self):
        asyncio.get_event_loop().run_until_complete(self._close_pool())

    def __enter__(self):
        return self

    def __exit__(self, *exs):
        self.close()


class HttpClient:
    def __init__(self, hosts, conn_pool_limit=25):
        self.hosts = hosts
        self.urls = itertools.cycle([i + '/_sql' for i in hosts])
        verify_ssl = _verify_ssl_from_first(self.hosts)
        conn = aiohttp.TCPConnector(limit=conn_pool_limit, verify_ssl=verify_ssl)
        self._session = aiohttp.ClientSession(connector=conn)

    async def execute(self, stmt, args=None):
        payload = {'stmt': _plain_or_callable(stmt)}
        if args:
            payload['args'] = _plain_or_callable(args)
        return await _exec(
            self._session,
            next(self.urls),
            json.dumps(payload, cls=CrateJsonEncoder)
        )

    async def execute_many(self, stmt, bulk_args):
        data = json.dumps(dict(
            stmt=_plain_or_callable(stmt),
            bulk_args=_plain_or_callable(bulk_args)
        ), cls=CrateJsonEncoder)
        return await _exec(self._session, next(self.urls), data)

    async def get_server_version(self):
        async with self._session.get(self.hosts[0] + '/') as resp:
            r = await resp.json()
            version = r['version']
            return {
                'hash': version['build_hash'],
                'number': version['number'],
                'date': _date_or_none(version['build_timestamp'][:10])
            }

    async def _close(self):
        if self._session:
            await self._session.close()

    def close(self):
        asyncio.get_event_loop().run_until_complete(self._close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def client(hosts, concurrency=25):
    hosts = hosts or 'localhost:4200'
    if hosts.startswith('asyncpg://'):
        if not asyncpg:
            raise ValueError('Cannot use "asyncpg" scheme if asyncpg is not available')
        return AsyncpgClient(hosts, pool_size=concurrency)
    return HttpClient(_to_http_hosts(hosts), conn_pool_limit=concurrency)
