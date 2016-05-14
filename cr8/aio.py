
from tqdm import tqdm
import asyncio
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


async def map_async(q, corof, iterable):
    for i in iterable:
        task = asyncio.ensure_future(corof(*i))
        await q.put(task)
    await q.put(None)


async def consume(q, total=None):
    with tqdm(total=total, unit=' requests', smoothing=0.1) as t:
        while True:
            task = await q.get()
            if task is None:
                break
            await task
            t.update(1)


def run(coro, iterable, concurrency, loop=None):
    loop = loop or asyncio.get_event_loop()
    q = asyncio.Queue(maxsize=concurrency)
    loop.run_until_complete(asyncio.gather(
        map_async(q, coro, iterable),
        consume(q)))
