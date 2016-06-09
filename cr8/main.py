#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh

from cr8.timeit import timeit
from cr8.insert_json import insert_json
from cr8.insert_fake_data import insert_fake_data
from cr8.insert_blob import insert_blob
from cr8.run_spec import run_spec


def main():
    p = argh.ArghParser()
    p.add_commands([timeit,
                    insert_json,
                    insert_fake_data,
                    insert_blob,
                    run_spec])
    p.dispatch()


if __name__ == '__main__':
    main()
