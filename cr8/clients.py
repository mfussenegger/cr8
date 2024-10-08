import json

import aiohttp
import itertools
import calendar
import types
import time
import contextlib
from urllib.parse import urlparse, parse_qs, urlunparse
from datetime import datetime, date
from typing import List, Union, Iterable, Dict, Optional, Any
from decimal import Decimal
from cr8.aio import asyncio  # import via aio for uvloop setup

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore

try:
    import simdjson  # type: ignore
    dumps = simdjson.dumps
except ImportError:
    dumps = json.dumps


HTTP_DEFAULT_HDRS = {'Content-Type': 'application/json'}


class CrateJsonEncoder(json.JSONEncoder):

    epoch = datetime(1970, 1, 1)

    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, datetime):
            delta = o.replace(tzinfo=None) - self.epoch
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

    >>> _to_http_hosts('https://n1:4200/?verify_ssl=false')
    ['https://n1:4200/?verify_ssl=false']

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


def _date_or_none(d: str) -> Optional[str]:
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


def _to_dsn(hosts: str) -> str:
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
        port = "5432"
    dbname = p.path[1:] if p.path else 'doc'
    dsn = f'postgres://{user_and_pw}@{host}:{port}/{dbname}'
    if p.query:
        dsn += '?' + '&'.join(k + '=' + v[0] for k, v in parse_qs(p.query).items())
    return dsn


def _to_boolean(v: str) -> bool:
    if str(v).lower() in ("true"):
        return True
    elif str(v).lower() in ("false"):
        return False
    else:
        raise ValueError('not a boolean value')


def _verify_ssl_from_first(hosts: List[str]) -> bool:
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
    def __init__(self, hosts, pool_size=25, session_settings=None):
        self.dsn = _to_dsn(hosts)
        self.pool_size = pool_size
        self._pool = None
        self.is_cratedb = True
        self.session_settings = session_settings or {}

    async def _get_pool(self):

        async def set_session_settings(conn):
            for setting, value in self.session_settings.items():
                await conn.execute(f'set {setting}={value}')

        if not self._pool:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=self.pool_size,
                max_size=self.pool_size,
                init=set_session_settings
            )
        return self._pool

    async def execute(self, stmt, args=None):
        start = time.perf_counter()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            if args:
                rows = await conn.fetch(stmt, *args)
            else:
                rows = await conn.fetch(stmt)
            return {
                'duration': (time.perf_counter() - start) * 1000.,
                'rows': rows
            }

    async def execute_many(self, stmt, bulk_args):
        start = time.perf_counter()
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(stmt, bulk_args)
            return {
                'duration': (time.perf_counter() - start) * 1000.,
                'rows': []
            }

    async def get_server_version(self):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            try:
                for (version,) in await conn.fetch('select version from sys.nodes'):
                    version = json.loads(version)
                    return {
                        'hash': version['build_hash'],
                        'number': version['number']
                    }
            except asyncpg.exceptions.UndefinedTableError:
                self.is_cratedb = False
                version = await conn.fetchval("select current_setting('server_version_num')")
                return {
                    'hash': None,
                    'number': version
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


def _append_sql(host: str) -> str:
    """ Append `/_sql` to the host, dropping any query parameters.

    >>> _append_sql('http://n1:4200')
    'http://n1:4200/_sql'

    >>> _append_sql('https://n1:4200/?verify_ssl=false')
    'https://n1:4200/_sql'

    >>> _append_sql('https://crate@n1:4200/?verify_ssl=false')
    'https://crate@n1:4200/_sql'

    """
    p = list(urlparse(host))
    p[2] = '_sql'
    p[3] = None
    p[4] = None
    return urlunparse(tuple(p))


class HttpClient:
    def __init__(self,
                 hosts: List[str],
                 conn_pool_limit=25,
                 session_settings: Optional[Dict[str, Any]] = None):
        self.hosts = hosts
        self.urls = itertools.cycle(list(map(_append_sql, hosts)))
        self.conn_pool_limit = conn_pool_limit
        self.is_cratedb = True
        self._pools: Dict[str, asyncio.Queue] = {}
        self.session_settings = session_settings or {}

    @contextlib.asynccontextmanager
    async def _session(self, url):
        pool = self._pools.get(url)
        if not pool:
            pool = asyncio.Queue(maxsize=self.conn_pool_limit)
            self._pools[url] = pool
            _connector_params = {
                'limit': 1,
                'verify_ssl': _verify_ssl_from_first(self.hosts)
            }
            for n in range(0, self.conn_pool_limit):
                tcp_connector = aiohttp.TCPConnector(**_connector_params)
                session = aiohttp.ClientSession(connector=tcp_connector)
                for setting, value in self.session_settings.items():
                    payload = {'stmt': f'set {setting}={value}'}
                    await _exec(
                        session,
                        url,
                        dumps(payload, cls=CrateJsonEncoder)
                    )
                pool.put_nowait(session)

        session = await pool.get()
        try:
            yield session
        finally:
            await pool.put(session)

    async def execute(self, stmt, args=None):
        payload = {'stmt': _plain_or_callable(stmt)}
        if args:
            payload['args'] = _plain_or_callable(args)
        url = next(self.urls)
        async with self._session(url) as session:
            result = await _exec(
                session,
                url,
                dumps(payload, cls=CrateJsonEncoder)
            )
        return result

    async def execute_many(self, stmt, bulk_args):
        data = dumps(dict(
            stmt=_plain_or_callable(stmt),
            bulk_args=_plain_or_callable(bulk_args)
        ), cls=CrateJsonEncoder)
        url = next(self.urls)
        async with self._session(url) as session:
            result = await _exec(session, url, data)
        return result

    async def get_server_version(self):
        urlparts = urlparse(self.hosts[0])
        url = urlunparse((urlparts.scheme, urlparts.netloc, '/', '', '', ''))
        async with self._session(url) as session:
            async with session.get(url) as resp:
                r = await resp.json()
                version = r['version']
                result = {
                    'hash': version['build_hash'],
                    'number': version['number'],
                    'date': _date_or_none(version['build_timestamp'][:10])
                }
                return result

    async def _close(self):
        pools = self._pools
        self._pools = {}
        for url, pool in pools.items():
            for i in range(0, self.conn_pool_limit):
                session = await pool.get()
                await session.close()
        pools.clear()

    def close(self):
        asyncio.get_event_loop().run_until_complete(self._close())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def client(hosts, session_settings=None, concurrency=25):
    hosts = hosts or 'localhost:4200'
    if hosts.startswith('asyncpg://'):
        if not asyncpg:
            raise ValueError('Cannot use "asyncpg" scheme if asyncpg is not available')
        return AsyncpgClient(hosts, pool_size=concurrency, session_settings=session_settings)
    return HttpClient(_to_http_hosts(hosts), conn_pool_limit=concurrency, session_settings=session_settings)
