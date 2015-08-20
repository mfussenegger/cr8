#!/usr/bin/env python
# -*- coding: utf-8 -*-


import argh
from crate.client import connect


@argh.arg('filename', help='path/filename of the file that should be uploaded')
@argh.arg('table', help='name of the blob table')
@argh.arg('hosts', type=str, nargs='+',
          help='crate hosts to which the file should be uploaded to')
def upload(hosts, table, filename):
    """ uploads a file into a blob table """
    conn = connect(hosts)
    container = conn.get_blob_container(table)
    digest = container.put(open(filename, 'rb'))
    return '{server}/_blobs/{table}/{digest}'.format(
        server=conn.client.active_servers[0],
        table=table,
        digest=digest
    )


def main():
    argh.dispatch_command(upload)


if __name__ == '__main__':
    main()
