#!/usr/bin/env python
# -*- coding: utf-8 -*-
# PYTHON_ARGCOMPLETE_OK

import sys
import argh
import argparse
import logging

from cr8 import __version__
from cr8.misc import break_iterable, init_logging
from cr8.timeit import timeit
from cr8.insert_json import insert_json
from cr8.insert_fake_data import insert_fake_data
from cr8.insert_blob import insert_blob
from cr8.run_spec import run_spec
from cr8.run_crate import run_crate, create_node
from cr8.run_track import run_track


log = logging.getLogger(__name__)


def _run_crate_and_rest(parser, args_groups):
    args = parser.parse_args(args_groups[0])
    log.info('# run-crate')
    log.info('===========\n')
    with create_node(version=args.version,
                     env=args.env,
                     setting=args.setting,
                     crate_root=args.crate_root,
                     keep_data=args.keep_data) as node:
        node.start()
        log.info('\n')
        for args in args_groups[1:]:
            cmd = '# ' + args[0]
            log.info(cmd)
            log.info('=' * len(cmd) + '\n')
            if '--hosts' in args:
                hosts_idx = args.index('--hosts')
                args[hosts_idx + 1] = args[hosts_idx + 1].format(node=node)
            parser.dispatch(args)
            log.info('\n')


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
    args_groups = list(break_iterable(sys.argv[1:], lambda x: x == '--'))
    if len(args_groups) == 1:
        p.dispatch()
        return

    init_logging(log)
    if args_groups[0][0] == 'run-crate':
        _run_crate_and_rest(p, args_groups)
    else:
        for args in args_groups:
            p.dispatch(args)


if __name__ == '__main__':
    main()
