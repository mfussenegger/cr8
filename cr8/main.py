#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from cr8.timeit import timeit
from cr8.blobs import upload
from cr8.json2insert import json2insert
from cr8.perf_regressions import find_perf_regressions


def main():
    p = argh.ArghParser()
    p.add_commands([timeit, json2insert, upload, find_perf_regressions])
    p.dispatch()


if __name__ == '__main__':
    main()
