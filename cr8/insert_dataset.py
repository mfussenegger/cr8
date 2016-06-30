#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import json
from urllib.request import urlopen
from urllib.parse import urlencode
from crate.client import connect

from cr8.cli import to_hosts


def load_source(source):
    if source.startswith('s3://'):
        raise ValueError('Cannot get data from s3. Use --copy-from')
    raise NotImplemented()


def list_feeds(feed_url):
    r = urlopen(feed_url)
    content = r.read().decode('utf-8')
    datasets = json.loads(content)['datasets']
    for dataset in datasets:
        print(dataset)


def retrieve_dataset(name, feed_url, table):
    r = urlopen(feed_url + '/' + name + '?' + urlencode([('table', table)]))
    content = r.read().decode('utf-8')
    return json.loads(content)


@argh.arg('--table', required=True)
@argh.arg('--hosts', type=to_hosts, default=['http://localhost:4200'])
def insert_dataset(
        name=None, table=None, hosts=None, feed_url=None, copy_from=False):
    """Insert a dataset into a table on a Crate cluster.

    Args:
        name: Name of the dataset.
            If not specified the available datasets are listed.
        table: Name of the table into which the data should be inserted.
        hosts: hostname:port pairs of the Crate nodes
        feed_url: Feed URL which contains the datasets
        copy_from: If COPY FROM should be used instead of doing bulk inserts
            from the client.
    """
    if not name:
        return list_feeds(feed_url)
    dataset = retrieve_dataset(name, feed_url, table)
    with connect(hosts) as conn:
        cursor = conn.cursor()
        cursor.execute(dataset['schema'])
        if copy_from:
            stmt = 'copy {table} from ? with (compression=?)'
            cursor.execute(
                stmt.format(table=table),
                (dataset['sources'], dataset['compression']))
        else:
            for source in dataset['sources']:
                load_source(source)
