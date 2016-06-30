
import functools
import os
import asyncio
import aiohttp
import json
import itertools
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from tqdm import tqdm
tqdm = functools.partial(
    tqdm,
    unit=' requests',
    smoothing=0.1,
    disable=os.environ.get('CR8_NO_TQDM') == 'True'
)


async def _exec(session, url, data):
    async with session.post(url, data=data) as resp:
        r = await resp.json()
        if 'error' in r:
            raise ValueError(r['error']['message'])
        return r['duration']


class Client:
    def __init__(self, hosts, conn_pool_limit=25):
        self.hosts = hosts
        self.urls = itertools.cycle([i + '/_sql' for i in hosts])
        conn = aiohttp.TCPConnector(limit=conn_pool_limit)
        self.session = aiohttp.ClientSession(connector=conn)

    async def execute(self, stmt, args=None):
        payload = {'stmt': stmt}
        if args:
            payload['args'] = args
        return await _exec(self.session, next(self.urls), json.dumps(payload))

    async def execute_many(self, stmt, bulk_args):
        data = json.dumps(dict(stmt=stmt, bulk_args=bulk_args))
        return await _exec(self.session, next(self.urls), data)

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


async def measure(stats, f, *args, **kws):
    duration = await f(*args, **kws)
    stats.measure(duration)
    return duration


async def map_async(q, corof, iterable):
    for i in iterable:
        task = asyncio.ensure_future(corof(*i))
        await q.put(task)
    await q.put(None)


async def consume(q, total=None):
    with tqdm(total=total) as t:
        while True:
            task = await q.get()
            if task is None:
                break
            await task
            t.update(1)


async def run_sync(coro, iterable, total=None):
    for i in tqdm(iterable, total=total):
        await coro(*i)


def run(coro, iterable, concurrency, loop=None, num_items=None):
    loop = loop or asyncio.get_event_loop()
    if concurrency == 1:
        return loop.run_until_complete(run_sync(coro, iterable, total=num_items))
    q = asyncio.Queue(maxsize=concurrency)
    loop.run_until_complete(asyncio.gather(
        map_async(q, coro, iterable),
        consume(q, total=num_items)))
