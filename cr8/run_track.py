import argh
import os
import toml
from glob import glob
from .run_spec import run_spec
from .cli import to_hosts
from .run_crate import CrateNode, get_crate


class Executor:
    def __init__(self, track_dir, result_hosts=None, crate_root=None):
        self.track_dir = track_dir
        self.result_hosts = result_hosts
        self.crate_root = crate_root

    def _expand_paths(self, paths):
        paths = (os.path.join(self.track_dir, path) for path in paths)
        paths = (os.path.abspath(path) for path in paths)
        return (p for path in paths for p in glob(path))

    def _run_specs(self, specs, benchmark_host):
        specs = self._expand_paths(specs)
        for spec in specs:
            print('### Running spec file: ', os.path.basename(spec))
            run_spec(spec, to_hosts(benchmark_host), self.result_hosts)

    def execute(self, track):
        configurations = list(self._expand_paths(track['configurations']))
        versions = track['versions']
        for version in versions:
            print('# Version: ', version)
            for c, configuration in enumerate(configurations):
                print('## Starting Crate {0}, configuration: {1}'.format(
                    os.path.basename(version),
                    os.path.basename(configuration)
                ))
                configuration = toml.load(os.path.join(self.track_dir, configuration))
                crate_dir = get_crate(version, self.crate_root)
                with CrateNode(crate_dir=crate_dir,
                               env=configuration.get('env'),
                               settings=configuration.get('settings')) as node:
                    node.start()
                    self._run_specs(track['specs'], node.http_url)


@argh.arg('-r', '--result_hosts', type=to_hosts)
def run_track(track, result_hosts=None, crate_root=None):
    """Execute a track file"""
    executor = Executor(
        track_dir=os.path.dirname(track),
        result_hosts=result_hosts,
        crate_root=crate_root
    )
    executor.execute(toml.load(track))


def main():
    argh.dispatch_command(run_track)


if __name__ == "__main__":
    main()
