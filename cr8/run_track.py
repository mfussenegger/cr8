import argh
import os
import toml
import sys
from glob import glob
from .log import Logger
from .run_spec import do_run_spec
from .run_crate import CrateNode, get_crate
from .clients import client_errors


class Executor:
    def __init__(self,
                 *,
                 track_dir,
                 log,
                 sample_mode,
                 result_hosts=None,
                 crate_root=None,
                 fail_fast=None):
        self.track_dir = track_dir
        self.sample_mode = sample_mode
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
        errors = []
        for spec in specs:
            self.log.info('### Running spec file: ', os.path.basename(spec))
            try:
                do_run_spec(
                    spec=spec,
                    benchmark_hosts=benchmark_host,
                    log=self.log,
                    result_hosts=self.result_hosts,
                    sample_mode=self.sample_mode,
                    action=action)
            except Exception:
                errors.append(True)
                if self.fail_fast:
                    raise
                else:
                    self.log.info('WARNING: Spec file failed due to the following exception:')
                    import traceback
                    traceback.print_exc()
        return any(errors)

    def _execute_specs(self, specs, benchmark_host):
        errors = []
        if isinstance(specs, list):
            errors.append(self._run_specs(specs, benchmark_host))
        else:
            fixtures = specs.get('fixtures', [])
            queries = specs.get('queries', [])
            full = specs.get('full', [])
            errors.append(
                self._run_specs(fixtures, benchmark_host, action=['setup']))
            errors.append(
                self._run_specs(queries, benchmark_host, action=['queries']))
            errors.append(
                self._run_specs(fixtures, benchmark_host, action=['teardown']))
            errors.append(
                self._run_specs(full, benchmark_host))
        return any(errors)

    def execute(self, track):
        configurations = list(self._expand_paths(track['configurations']))
        versions = track['versions']
        error = False
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
                    _error = self._execute_specs(track['specs'], node.http_url)
                    error = error or _error
        return error


@argh.arg('-r', '--result_hosts', type=str)
@argh.arg('-of', '--output-fmt', choices=['json', 'text'], default='text')
@argh.arg('--failfast', action='store_true')
@argh.arg('--logfile-info', help='Redirect info messages to a file')
@argh.arg('--logfile-result', help='Redirect benchmark results to a file')
@argh.arg('--sample-mode', choices=('all', 'reservoir'),
          help='Method used for sampling', default='reservoir')
@argh.wrap_errors([KeyboardInterrupt, BrokenPipeError] + client_errors)
def run_track(track,
              result_hosts=None,
              crate_root=None,
              output_fmt=None,
              logfile_info=None,
              logfile_result=None,
              failfast=False,
              sample_mode='reservoir'):
    """Execute a track file"""
    with Logger(output_fmt=output_fmt,
                logfile_info=logfile_info,
                logfile_result=logfile_result) as log:
        executor = Executor(
            track_dir=os.path.dirname(track),
            log=log,
            result_hosts=result_hosts,
            crate_root=crate_root,
            fail_fast=failfast,
            sample_mode=sample_mode
        )
        error = executor.execute(toml.load(track))
        if error:
            sys.exit(1)


def main():
    argh.dispatch_command(run_track)


if __name__ == "__main__":
    main()
