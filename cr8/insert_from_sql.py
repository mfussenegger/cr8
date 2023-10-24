
import argh
import asyncpg
import asyncio

from functools import partial

from cr8 import aio, clients
from cr8.cli import to_int
from cr8.metrics import Stats
from cr8.log import format_stats


def mk_insert(table, attributes):
    columns = ', '.join(('"' + x.name + '"' for x in attributes))
    params = ', '.join((f'${i + 1}' for i in range(len(attributes))))
    return f'INSERT INTO {table} ({columns}) VALUES ({params})'


async def reader(q, conn, stmt, fetch_size):
    async with conn.transaction():
        bulk_args = []
        async for row in stmt.cursor(prefetch=fetch_size):
            bulk_args.append(tuple(row))
            if len(bulk_args) == fetch_size:
                await q.put(bulk_args)
                bulk_args = []
        if bulk_args:
            await q.put(bulk_args)
        await q.put(None)


async def writer(q, insert, insert_many):
    with aio.tqdm() as t:
        while True:
            bulk_args = await q.get()
            if bulk_args is None:
                break
            await insert_many(insert, bulk_args)
            t.update(1)


async def async_insert_from_sql(src_uri,
                                concurrency,
                                query,
                                fetch_size,
                                table,
                                insert_many):
    conn = await asyncpg.connect(src_uri)
    try:
        stmt = await conn.prepare(query)
        insert = mk_insert(table, stmt.get_attributes())
        print(insert)
        q = asyncio.Queue(maxsize=concurrency)
        await asyncio.gather(
            reader(q, conn, stmt, fetch_size),
            writer(q, insert, insert_many)
        )
    finally:
        await conn.close()


@argh.arg('--src-uri', help='source uri', required=True)
@argh.arg('--query', help='select statement', required=True)
@argh.arg('--fetch-size', type=to_int)
@argh.arg('--table', help='Target table', required=True)
@argh.arg('--hosts', help='Target CrateDB hosts')
@argh.arg('-c', '--concurrency', type=to_int)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.wrap_errors([KeyboardInterrupt, BrokenPipeError] + clients.client_errors)
def insert_from_sql(*,
                    src_uri=None,
                    query=None,
                    fetch_size=100,
                    concurrency=25,
                    table=None,
                    hosts=None,
                    output_fmt=None):
    """Insert data read from another SQL source into table."""

    stats = Stats()
    with clients.client(hosts, concurrency=concurrency) as client:
        f = partial(aio.measure, stats, client.execute_many)
        try:
            aio.run(
                async_insert_from_sql,
                src_uri,
                concurrency,
                query,
                fetch_size,
                table,
                f
            )
        except clients.SqlException as e:
            raise SystemExit(str(e))
    try:
        print(format_stats(stats.get(), output_fmt))
    except KeyError:
        if not stats.sampler.values:
            raise SystemExit('No data read from source')
        raise


def main():
    argh.dispatch_command(insert_from_sql)


if __name__ == "__main__":
    main()
