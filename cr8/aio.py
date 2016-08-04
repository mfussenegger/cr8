
import functools
import os
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
    disable=os.environ.get('CR8_NO_TQDM') == 'True'
)


async def measure(stats, f, *args, **kws):
    duration = await f(*args, **kws)
    stats.measure(duration)
    return duration


async def qmap(q, corof, iterable):
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


async def map(coro, iterable, total=None):
    for i in tqdm(iterable, total=total):
        await coro(*i)


def run(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro())


def run_many(coro, iterable, concurrency, num_items=None):
    loop = asyncio.get_event_loop()
    if concurrency == 1:
        return loop.run_until_complete(map(coro, iterable, total=num_items))
    q = asyncio.Queue(maxsize=concurrency)
    loop.run_until_complete(asyncio.gather(
        qmap(q, coro, iterable),
        consume(q, total=num_items)))
