
import functools
import sys
import asyncio
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
    disable=not sys.stdin.isatty()
)


async def execute(loop, cursor, stmt, args=None):
    f = loop.run_in_executor(None, cursor.execute, stmt, args)
    await f
    return cursor.duration


async def execute_many(loop, cursor, stmt, bulk_args):
    f = loop.run_in_executor(None, cursor.executemany, stmt, bulk_args)
    await f
    return cursor.duration


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
