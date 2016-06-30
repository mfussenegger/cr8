#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argh
from crate.client import connect
from cr8.cli import to_hosts


@argh.arg('filename', help='path/filename of the file that should be uploaded')
@argh.arg('--table', help='name of the blob table', required=True)
@argh.arg('--hosts', type=to_hosts,
          help='crate hosts to which the file should be uploaded to',
          default=['http://localhost:4200'])
def insert_blob(filename, hosts=None, table=None):
    """ uploads a file into a blob table """
    conn = connect(hosts)
    container = conn.get_blob_container(table)
    with open(filename, 'rb') as f:
        digest = container.put(f)
    return '{server}/_blobs/{table}/{digest}'.format(
        server=conn.client.active_servers[0],
        table=table,
        digest=digest
    )


def main():
    argh.dispatch_command(insert_blob)


if __name__ == '__main__':
    main()
