#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from .cli import dicts_from_stdin


def to_insert(table, d):
    """ generate a insert statement using the given table and dictionary

    :param table: table name
    :type table: string
    :param d: dictionary containing the columns and values
    :type d: dict

    :return: tuple with the statement and arguments
    """

    columns = []
    args = []
    for key, val in d.items():
        columns.append(key)
        args.append(val)
    stmt = 'insert into {table} ({columns}) values ({params})'.format(
        table=table,
        columns=', '.join(columns),
        params=', '.join(['?'] * len(columns)))
    return (stmt, args)


@argh.arg('table', help='table name that should be used in the statement')
@argh.arg('hosts', help='crate hosts which will be used \
          to execute the insert statement')
def json2insert(table, *hosts):
    """ Converts the given json line (read from stdin) into an insert statement

    If hosts are specified the insert statement will be executed on those hosts.
    Otherwise the statement and the arguments are printed.
    """
    for d in dicts_from_stdin():
        stmt, args = to_insert(table, d)
        if hosts:
            from crate.client import connect
            conn = connect(hosts)
            c = conn.cursor()
            c.execute(stmt, args)
        yield stmt, args


def main():
    argh.dispatch_command(json2insert)


if __name__ == '__main__':
    main()
