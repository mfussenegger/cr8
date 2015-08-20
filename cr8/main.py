#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from cr8.timeit import timeit
from cr8.blobs import upload
from cr8.json2insert import json2insert


def main():
    p = argh.ArghParser()
    p.add_commands([timeit, json2insert, upload])
    p.dispatch()


if __name__ == '__main__':
    main()
