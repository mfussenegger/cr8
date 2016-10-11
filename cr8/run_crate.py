import argh
import os
import json
import re
import subprocess
import tempfile
import sys
import shutil
import contextlib
import logging
import random
import time
import gzip
import io
import tarfile
from typing import Dict, Any
from urllib.request import urlopen
from .exceptions import NoHttpAddressAvailable


log = logging.getLogger(__file__)


RELEASE_URL = 'https://cdn.crate.io/downloads/releases/crate-{version}.tar.gz'
VERSION_RE = re.compile('^(\d+\.\d+\.\d+)$')
DEFAULT_SETTINGS = {
    'cluster.routing.allocation.disk.watermark.low': '1b',
    'cluster.routing.allocation.disk.watermark.high': '1b',
    'discovery.initial_state_timeout': 0,
    'discovery.zen.ping.multicast.enabled': False,
    'network.host': '127.0.0.1',
    'udc.enabled': False
}

HTTP_ADDRESS_RE = re.compile(
    '.*\[http +\] \[.*\] .*'
    'publish_address {'
    '(?:inet\[[\w\d\.-]*/|\[)?'
    '(?:[\w\d\.-]+/)?'
    '(?P<addr>[\d\.:]+)'
    '(?:\])?'
    '}'
)


class Timeout:
    def __init__(self, timeout, sleep=0.1):
        self.start_time = time.time()
        self.sleep = sleep
        self._first_ok = True
        self.timeout = timeout

        def timeout_expired():
            if self._first_ok:
                self._first_ok = False
                return False
            now = time.time()
            if (now - self.start_time) > self.timeout:
                return True
            if self.sleep:
                time.sleep(self.sleep)

        self._timeout_expired = timeout_expired

    def __call__(self):
        return not self._timeout_expired()


def wait_until(f, condition):
    """Calls f while condition returns True until f returns a truthy value

    >>> wait_until(lambda: True, lambda: True)
    """
    while condition():
        r = f()
        if r:
            break


def _get_http_uri(lines):
    """Get the http publish_address from lines that contain Crate log output.

    >>> lines = [
    ...     "[2016-06-11 19:10:16,141][INFO ][node                     ] [Ankhi] initialized",
    ...     "[2016-06-11 19:10:16,141][INFO ][node                     ] [Ankhi] starting ...",
    ...     "[2016-06-11 19:10:16,171][INFO ][http                     ] [Ankhi] publish_address {10.68.2.10:4200}, bound_addresses {[::]:4200}",
    ...     "[2016-06-11 19:10:16,188][INFO ][discovery                ] [Ankhi] crate/GJAvonoFSfS3Y1IaUPTqfA"
    ... ]
    >>> _get_http_uri(lines)
    'http://10.68.2.10:4200'

    >>> lines = [
    ...     "[2016-06-11 21:26:53,798][INFO ][node                     ] [Rex Mundi] starting ...",
    ...     "[2016-06-11 21:26:53,828][INFO ][http                     ] [Rex Mundi] bound_address {inet[/0:0:0:0:0:0:0:0:4200]}, publish_address {inet[/192.168.0.19:4200]}",
    ... ]
    >>> _get_http_uri(lines)
    'http://192.168.0.19:4200'

    >>> lines = [
    ...     "[2016-06-15 22:18:36,639][INFO ][node                     ] [crate] starting ...",
    ...     "[2016-06-15 22:18:36,755][INFO ][http                     ] [crate] publish_address {localhost/127.0.0.1:42203}, bound_addresses {127.0.0.1:42203}, {[::1]:42203}, {[fe80::1]:42203}",
    ...     "[2016-06-15 22:18:36,774][INFO ][transport                ] [crate] publish_address {localhost/127.0.0.1:4300}, bound_addresses {127.0.0.1:4300}, {[::1]:4300}, {[fe80::1]:4300}",
    ...     "[2016-06-15 22:18:36,779][INFO ][discovery                ] [crate] Testing42203/Eroq_ZAgT4CDpF_gzh4tcA",
    ... ]
    >>> _get_http_uri(lines)
    'http://127.0.0.1:42203'

    >>> lines = [
    ...     "[2016-06-16 10:27:20,074][INFO ][node                     ] [Selene] starting ...",
    ...     "[2016-06-16 10:27:20,150][INFO ][http                     ] [Selene] bound_address {inet[/192.168.43.105:4200]}, publish_address {inet[Haudis-MacBook-Pro.local/192.168.43.105:4200]}",
    ...     "[2016-06-16 10:27:20,165][INFO ][transport                ] [Selene] bound_address {inet[/192.168.43.105:4300]}, publish_address {inet[Haudis-MacBook-Pro.local/192.168.43.105:4300]}",
    ...     "[2016-06-16 10:27:20,185][INFO ][discovery                ] [Selene] crate/h9moKMrATmCElYXjfad5Vw",
    ... ]
    >>> _get_http_uri(lines)
    'http://192.168.43.105:4200'
    """
    for line in lines:
        m = HTTP_ADDRESS_RE.match(line)
        if m:
            return 'http://' + m.group('addr')


def _wait_until_reachable(url):
    def cluster_ready():
        try:
            with urlopen(url) as r:
                p = json.loads(r.read().decode('utf-8'))
                return int(p['status']) == 200
        except Exception as e:
            return False
    wait_until(cluster_ready, Timeout(60))


def _get_settings(settings=None):
    s = DEFAULT_SETTINGS.copy()
    if settings:
        s.update(settings)
    return s


class CrateNode(contextlib.ExitStack):
    """Class that allows starting and stopping a Crate process

    This is similar to the ``CrateLayer`` in ``crate.testing.layer``.
    But additionaly it supports setting environment variables and it can infer
    the port to which Crate binds by sniffing Crate's stdout.

    Attributes:
        http_url: The HTTP URL of the Crate process.
            Only available after ``start()`` has been called.
        process: The subprocess. Only available after ``start()`` has been called.
    """

    def __init__(self,
                 crate_dir: str,
                 data_path: str=None,
                 env: Dict[str, Any]=None,
                 settings: Dict[str, Any]=None) -> None:
        """Create a CrateNode

        Args:
            crate_dir: Path to the extracted Crate tarball
            env: Environment variables with which the Crate process will be
                started.
            settings: Additional Crate settings.
        """
        super().__init__()
        self.crate_dir = crate_dir
        self.env = env
        self.process = None  # type: subprocess.Popen
        self.http_url = None  # type: str
        start_script = 'crate.bat' if sys.platform == 'win32' else 'crate'

        settings = _get_settings(settings)
        self.data_path = settings.get('path.data') or tempfile.mkdtemp()
        self.logs_path = settings.get('path.logs') or os.path.join(crate_dir, 'logs')
        self.cluster_name = settings.get('cluster.name') or 'cr8'
        log.info('Work dir: %s (removed on stop)', self.data_path)
        settings['path.data'] = self.data_path
        settings['cluster.name'] = self.cluster_name
        args = ['-Des.{0}={1}'.format(k, v) for k, v in settings.items()]
        self.cmd = [
            os.path.join(crate_dir, 'bin', start_script)] + args

    def start(self):
        """Start the process.

        This will block until the Crate cluster is ready to process requests.
        """
        log.info('Starting Crate process')
        self.process = proc = self.enter_context(subprocess.Popen(
            self.cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=self.env,
            universal_newlines=True
        ))
        log.info('PID: %s', proc.pid)
        self._obtain_http_url()
        log.info('HTTP: %s', self.http_url)
        _wait_until_reachable(self.http_url)
        log.info('Cluster is ready')

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.communicate(timeout=10)
        shutil.rmtree(self.data_path)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.stop()

    def _obtain_http_url(self):
        """
        Read Crate log file to obtain HTTP address of node
        """
        def wait(waited, wait_time=0.25):
            time.sleep(wait_time)
            return waited + wait_time

        logfile = os.path.join(self.logs_path, self.cluster_name + '.log')

        def logfile_exists():
            return os.path.exists(logfile)
        wait_until(logfile_exists, Timeout(10))

        time_waited = 0
        with open(logfile, encoding='utf-8') as fp:
            pos = 0
            while not self.http_url:
                fp.seek(pos)
                if pos > 10000 or time_waited > 30:
                    # don't wait forever
                    # if HTTP url could not be obtained within the first 10000 bytes
                    # or within 30 seconds it's probably not there
                    raise NoHttpAddressAvailable("Couldn't get HTTP URL")
                self.http_url = _get_http_uri(iter(fp.readline, ''))
                pos = fp.tell()
                time_waited = wait(time_waited)


def _download_and_extract(uri, crate_root):
    filename = os.path.basename(uri)
    crate_folder_name = re.sub('\.tar(\.gz)?$', '', filename)
    crate_dir = os.path.join(crate_root, crate_folder_name)
    if os.path.exists(crate_dir):
        log.info('Skipping download, tarball alrady extracted at %s', crate_dir)
        return crate_dir
    log.info('Downloading %s and extracting to %s', uri, crate_root)
    with io.BytesIO(urlopen(uri).read()) as tmpfile:
        with tarfile.open(fileobj=tmpfile) as t:
            t.extractall(crate_root)
    return crate_dir


def init_logging():
    log.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    log.addHandler(ch)


def _from_versions_json(key):
    def retrieve():
        with urlopen('https://crate.io/versions.json') as r:
            if r.headers.get('Content-Encoding') == 'gzip':
                with gzip.open(r, 'rt') as r:
                    versions = json.loads(r.read())
            else:
                versions = json.loads(r.read().decode('utf-8'))
        return versions[key]
    return retrieve


NIGHTLY_RE = re.compile('.*>(?P<filename>crate-\d+\.\d+\.\d+-\d{12}-[a-z0-9]{7,}\.tar\.gz)<.*')


def _find_last_nightly(lines):
    """Return the last nightly release tarball filename.

    >>> lines = [
    ...     '<a href="crate-0.55.0-201606080301-3ceb1ed.tar.gz">crate-0.55.0-201606080301-3ceb1ed.tar.gz</a>           08-Jun-2016 01:01            46298304'
    ...     '<a href="crate-0.55.0-201606090301-b32a36f.tar.gz">crate-0.55.0-201606090301-b32a36f.tar.gz</a>           09-Jun-2016 01:01            46297737'
    ...     '<a href="crate-0.55.0-201606100301-23388dd.tar.gz">crate-0.55.0-201606100301-23388dd.tar.gz</a>           10-Jun-2016 01:01            46300496'
    ... ]
    >>> _find_last_nightly(lines)
    'crate-0.55.0-201606100301-23388dd.tar.gz'
    """
    for line in reversed(lines):
        m = NIGHTLY_RE.match(line)
        if m:
            return m.group('filename')
    raise ValueError("Couldn't find a valid nightly tarball filename in the lines")


def _get_latest_nightly_uri():
    base_uri = 'https://cdn.crate.io/downloads/releases/nightly/'
    with urlopen(base_uri) as r:
        filename = _find_last_nightly([line.decode('utf-8') for line in r])
        return base_uri + filename


_version_lookups = {
    'latest': _from_versions_json('crate-java'),
    'latest-stable': _from_versions_json('crate-java'),
    'latest-testing': _from_versions_json('crate_testing'),
    'latest-nightly': _get_latest_nightly_uri
}


def _lookup_uri(version):
    if version in _version_lookups:
        version = _version_lookups[version]()
    m = VERSION_RE.match(version)
    if m:
        return RELEASE_URL.format(version=m.group(0))
    else:
        return version


def get_crate(version, crate_root=None):
    """Retrieve a Crate tarball, extract it and return the path.

    Args:
        version: The Crate version to get.
            Can be specified in different ways:

            - A concrete version like '0.55.0'
            - An alias: 'latest-stable' or 'latest-testing'
            - A URI pointing to a crate tarball
        crate_root: Where to extract the tarball to.
            If this isn't specified ``$XDG_CACHE_HOME/.cache/cr8/crates``
            will be used.
    """
    uri = _lookup_uri(version)
    crate_root = crate_root or os.environ.get(
        'XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache', 'cr8', 'crates'))
    crate_dir = _download_and_extract(uri, crate_root)
    return crate_dir


@argh.arg('version', help='Crate version to run. Concrete version like\
          "0.55.0", an alias or an URI pointing to a Crate tarball. Supported\
          aliases are: [{0}]'.format(', '.join(_version_lookups.keys())))
@argh.arg('-e', '--env', action='append')
@argh.arg('-s', '--setting', action='append')
def run_crate(version, env=None, setting=None, crate_root=None):
    """Launch a crate instance"""
    init_logging()
    settings = {
        'cluster.name': 'cr8-crate-run' + str(random.randrange(1e9))
    }
    crate_dir = get_crate(version, crate_root)
    if setting:
        settings.update(dict(i.split('=') for i in setting))
    if env:
        env = dict(i.split('=') for i in env)
    with CrateNode(crate_dir=crate_dir, env=env, settings=settings) as node:
        try:
            node.start()
            node.process.communicate()
        except NoHttpAddressAvailable as ex:
            print(ex)
        except KeyboardInterrupt:
            print('Stopping Crate...')


if __name__ == "__main__":
    argh.dispatch_command(run_crate)
