"""
reindex all tables that require re-indexing to upgrade to the next major CrateDB version.

This temporarily copies all the data, so additional disk space is required and
as it re-indexed the data the cluster will experience higher load while this is
running.
"""

import argh
from cr8 import clients
from cr8.aio import run
from cr8.misc import parse_version


FIND_TABLES_4_0 = '''
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    substr(version['created'], 0, 1)::int < (
        SELECT
            max(substr(version['number'], 0, 1)::int)
        FROM
            sys.nodes);
'''

FIND_TABLES_3_3 = '''
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    substr(version['created']['cratedb'], 0, 1)::int < (
        SELECT
            max(substr(version['number'], 0, 1)::int)
        FROM
            sys.nodes);
'''


COL_NAMES = '''
SELECT
    column_name
FROM
    information_schema.columns
WHERE
    table_schema = ?
    AND table_name = ?
    AND CAST(is_generated AS string) in ('f', 'NEVER')
    AND column_name NOT LIKE '%]'
ORDER BY
    ordinal_position ASC,
    column_name;
'''


async def _fetch_tables_to_upgrade(client, version):
    query = FIND_TABLES_3_3 if version < (4, 0, 0) else FIND_TABLES_4_0
    result = await client.execute(query)
    return result['rows']


async def _show_create_table(client, fq_table):
    result = await client.execute(f'SHOW CREATE TABLE {fq_table}')
    return result['rows'][0][0]


async def _reindex_table(client, schema, table, create_table, column_names):
    tmp_table = f'tmp__{table}'
    print(f'Creating {schema}.{tmp_table}')
    create_tmp_table = create_table.replace(
        f'"{schema}"."{table}"',
        f'"{schema}"."{tmp_table}"'
    )
    await client.execute(create_tmp_table)
    columns = ', '.join((f'"{x}"' for x in column_names))
    print(f'Copy data from "{schema}"."{table}" to "{schema}"."{tmp_table}"')
    await client.execute(
        f'INSERT INTO "{schema}"."{tmp_table}" ({columns}) (SELECT {columns} FROM "{schema}"."{table}")')
    print(f'Replacing {table} with {tmp_table}')
    await client.execute(
        f'ALTER CLUSTER SWAP TABLE "{schema}"."{tmp_table}" TO "{schema}"."{table}" WITH (drop_source = true)')


async def _fetch_column_names(client, schema, table):
    result = await client.execute(COL_NAMES, (schema, table))
    return (row[0] for row in result['rows'])


async def _async_reindex(client):
    version_dict = await client.get_server_version()
    version = parse_version(version_dict['number'])
    if version < (3, 3, 0):
        raise ValueError("reindex only works on a CrateDB cluster running 3.3.0 or later")
    tables = await _fetch_tables_to_upgrade(client, version)
    for schema, table in tables:
        create_table = await _show_create_table(client, f'{schema}.{table}')
        column_names = await _fetch_column_names(client, schema, table)
        await _reindex_table(client, schema, table, create_table, column_names)


@argh.arg('--hosts', help='crate hosts', type=str, required=True)
def reindex(hosts=None):
    with clients.client(hosts) as client:
        run(_async_reindex, client)


def main():
    argh.dispatch_command(reindex)


reindex.__doc__ = __doc__
