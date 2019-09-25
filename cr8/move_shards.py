import argh
import random
import asyncio
from itertools import cycle
from tqdm import tqdm
from typing import NamedTuple
from cr8 import clients
from cr8.aio import run
from cr8.misc import parse_table


GET_SHARDS = '''
SELECT
    id,
    primary,
    node['name'],
    state,
    routing_state
FROM
    sys.shards
WHERE
    schema_name = $1
    AND table_name = $2
'''
GET_NODE_NAMES = 'SELECT name FROM sys.nodes'

MOVE_SHARD = 'ALTER TABLE {schema}.{table} REROUTE MOVE SHARD $1 FROM $2 TO $3'


class Shard(NamedTuple):
    id_: int
    primary: bool
    node_name: str
    state: str
    routing_state: str


async def _move_shards(client, schema_table):
    node_names = [row[0] for row in (await client.execute(GET_NODE_NAMES))['rows']]
    if len(node_names) == 1:
        raise ValueError("Only 1 node in the cluster. Can't move shards")
    schema, table = schema_table
    spinner = cycle(['/', '-', '\\', '|'])
    while True:
        print('Choosing shard to move', end='\r')
        result = await client.execute(GET_SHARDS, schema_table)
        all_shards = [Shard(*row) for row in result['rows']]
        relocatable_shards = [x for x in all_shards if x.routing_state == 'STARTED']
        if not relocatable_shards:
            print(f'Waiting for relocatable shards (0 / {len(all_shards)}) {next(spinner)}', end='\r')
            await asyncio.sleep(0.5)
            continue
        shard = random.choice(relocatable_shards)
        move_shard = MOVE_SHARD.format(schema=schema, table=table)
        target_node = shard.node_name
        shard_copies = [x for x in all_shards if x.id_ == shard.id_]
        while any(x for x in shard_copies if x.node_name == target_node):
            target_node = random.choice(node_names)
        print(f'Moving {shard.id_} from {shard.node_name} to {target_node}', end='\r')
        await client.execute(move_shard, (shard.id_, shard.node_name, target_node))


@argh.arg('--table', help='table name', required=True)
@argh.arg('--hosts', help='CrateDB hosts', type=str)
@argh.wrap_errors([KeyboardInterrupt] + clients.client_errors)
def move_shards(table=None, hosts=None):
    with clients.client(hosts) as client:
        run(_move_shards, client, parse_table(table))


def main():
    argh.dispatch_command(move_shards)
