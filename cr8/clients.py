import json
import aiohttp
import itertools
from datetime import datetime
from typing import List, Union, Iterable


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
            raise ValueError(r['error']['message'])
        return r['duration']


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


class HttpClient:
    def __init__(self, hosts, conn_pool_limit=25):
        self.hosts = hosts
        self.urls = itertools.cycle([i + '/_sql' for i in hosts])
        conn = aiohttp.TCPConnector(limit=conn_pool_limit)
        self.session = aiohttp.ClientSession(connector=conn)

    async def execute(self, stmt, args=None):
        payload = {'stmt': _plain_or_callable(stmt)}
        if args:
            payload['args'] = _plain_or_callable(args)
        return await _exec(self.session, next(self.urls), json.dumps(payload))

    async def execute_many(self, stmt, bulk_args):
        data = json.dumps(dict(
            stmt=_plain_or_callable(stmt),
            bulk_args=_plain_or_callable(bulk_args)
        ))
        return await _exec(self.session, next(self.urls), data)

    async def get_server_version(self):
        async with self.session.get(self.hosts[0] + '/') as resp:
            r = await resp.json()
            version = r['version']
            return {
                'hash': version['build_hash'],
                'number': version['number'],
                'date': _date_or_none(version['build_timestamp'][:10])
            }

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def client(hosts, concurrency=25):
    return HttpClient(_to_http_hosts(hosts), conn_pool_limit=concurrency)
