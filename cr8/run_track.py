import argh
import os
import toml
from glob import glob
from .log import Logger
from .run_spec import do_run_spec
from .run_crate import CrateNode, get_crate
from .clients import client_errors


class Executor:
    def __init__(self,
                 track_dir,
                 log,
                 result_hosts=None,
                 crate_root=None,
                 fail_fast=None):
        self.track_dir = track_dir
        self.result_hosts = result_hosts
        self.crate_root = crate_root
        self.log = log
        self.fail_fast = fail_fast

    def _expand_paths(self, paths):
        paths = (os.path.join(self.track_dir, path) for path in paths)
        paths = (os.path.abspath(path) for path in paths)
        return (p for path in paths for p in glob(path))

    def _run_specs(self, specs, benchmark_host, action=None):
        specs = self._expand_paths(specs)
        for spec in specs:
            self.log.info('### Running spec file: ', os.path.basename(spec))
            try:
                do_run_spec(
                    spec,
                    benchmark_host,
                    self.log,
                    self.result_hosts,
                    action=action)
            except:
                if self.fail_fast:
                    raise
                else:
                    self.log.info('WARNING: Spec file failed due to the following exception:')
                    import traceback
                    traceback.print_exc()

    def _execute_specs(self, specs, benchmark_host):
        if isinstance(specs, list):
            self._run_specs(specs, benchmark_host)
        else:
            fixtures = specs.get('fixtures', [])
            queries = specs.get('queries', [])
            full = specs.get('full', [])
            self._run_specs(fixtures, benchmark_host, action=['setup'])
            self._run_specs(queries, benchmark_host, action=['queries'])
            self._run_specs(fixtures, benchmark_host, action=['teardown'])
            self._run_specs(full, benchmark_host)

    def execute(self, track):
        configurations = list(self._expand_paths(track['configurations']))
        versions = track['versions']
        for version in versions:
            self.log.info('# Version: ', version)
            for c, configuration in enumerate(configurations):
                self.log.info('## Starting Crate {0}, configuration: {1}'.format(
                    os.path.basename(version),
                    os.path.basename(configuration)
                ))
                configuration = toml.load(os.path.join(self.track_dir, configuration))
                crate_dir = get_crate(version, self.crate_root)
                with CrateNode(crate_dir=crate_dir,
                               env=configuration.get('env'),
                               settings=configuration.get('settings')) as node:
                    node.start()
                    self._execute_specs(track['specs'], node.http_url)


@argh.arg('-r', '--result_hosts', type=str)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--failfast', action='store_true')
@argh.arg('--logfile-info', help='Redirect info messages to a file')
@argh.arg('--logfile-result', help='Redirect benchmark results to a file')
@argh.wrap_errors([KeyboardInterrupt] + client_errors)
def run_track(track,
              result_hosts=None,
              crate_root=None,
              output_fmt=None,
              logfile_info=None,
              logfile_result=None,
              failfast=False):
    """Execute a track file"""
    with Logger(output_fmt=output_fmt,
                logfile_info=logfile_info,
                logfile_result=logfile_result) as log:
        executor = Executor(
            track_dir=os.path.dirname(track),
            log=log,
            result_hosts=result_hosts,
            crate_root=crate_root,
            fail_fast=failfast
        )
        executor.execute(toml.load(track))


def main():
    argh.dispatch_command(run_track)


if __name__ == "__main__":
    main()
