#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from cr8.timeit import timeit
from cr8.blobs import upload
from cr8.json2insert import json2insert
from cr8.perf_regressions import find_perf_regressions
from cr8.fill_table import fill_table


def main():
    p = argh.ArghParser()
    p.add_commands([timeit,
                    json2insert,
                    upload,
                    find_perf_regressions,
                    fill_table])
    p.dispatch()


if __name__ == '__main__':
    main()
