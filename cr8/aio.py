
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
    await q.join()


async def consume(q):
    with tqdm(unit=' requests') as t:
        while True:
            task = await q.get()
            await task
            t.update(1)
            q.task_done()


def run(coro, iterable, concurrency, loop=None):
    loop = loop or asyncio.get_event_loop()
    q = asyncio.Queue(maxsize=concurrency)
    consume_task = loop.create_task(consume(q))
    loop.run_until_complete(map_async(q, coro, iterable))
    consume_task.cancel()
