#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argh
import argparse

from cr8 import __version__
from cr8.timeit import timeit
from cr8.insert_json import insert_json
from cr8.insert_fake_data import insert_fake_data
from cr8.insert_blob import insert_blob
from cr8.run_spec import run_spec
from cr8.run_crate import run_crate
from cr8.run_track import run_track


def main():
    p = argh.ArghParser(
        prog='cr8', formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument(
        '--version', action='version', version="%(prog)s " + __version__)
    p.add_commands([timeit,
                    insert_json,
                    insert_fake_data,
                    insert_blob,
                    run_spec,
                    run_crate,
                    run_track])
    p.dispatch()


if __name__ == '__main__':
    main()
