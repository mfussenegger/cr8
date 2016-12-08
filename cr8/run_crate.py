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
import threading
from typing import Dict, Any
from urllib.request import urlopen


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

ADDRESS_RE = re.compile(
    '.*\[(?P<protocol>http|psql|transport) +\] \[.*\] .*'
    'publish_address {'
    '(?:inet\[[\w\d\.-]*/|\[)?'
    '(?:[\w\d\.-]+/)?'
    '(?P<addr>[\d\.:]+)'
    '(?:\])?'
    '}'
)


class OutputMonitor:

    def __init__(self):
        self.consumers = []

    def consume(self, iterable):
        for line in iterable:
            for consumer in self.consumers:
                consumer.send(line)

    def start(self, proc):
        out_thread = threading.Thread(target=self.consume, args=(proc.stdout,))
        out_thread.daemon = True
        out_thread.start()


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
        if self._timeout_expired():
            raise TimeoutError()
        return True


def wait_until(predicate, timeout=30):
    """Wait until predicate returns a truthy value or the timeout is reached.

    >>> wait_until(lambda: True, timeout=10)
    """
    not_expired = Timeout(timeout)
    while not_expired():
        r = predicate()
        if r:
            break


def cluster_state_200(url):
    try:
        with urlopen(url) as r:
            p = json.loads(r.read().decode('utf-8'))
            return int(p['status']) == 200
    except Exception as e:
        log.debug(e)
        return False


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
                 settings: Dict[str, Any]=None,
                 keep_data: bool=False) -> None:
        """Create a CrateNode

        Args:
            crate_dir: Path to the extracted Crate tarball
            env: Environment variables with which the Crate process will be
                started.
            settings: Additional Crate settings.
        """
        super().__init__()
        self.crate_dir = crate_dir
        self.env = env or {}
        self.env.setdefault('JAVA_HOME', os.environ.get('JAVA_HOME', ''))
        self.monitor = OutputMonitor()
        self.process = None  # type: subprocess.Popen
        self.http_url = None  # type: str
        start_script = 'crate.bat' if sys.platform == 'win32' else 'crate'

        settings = _get_settings(settings)
        self.data_path = settings.get('path.data') or tempfile.mkdtemp()
        self.logs_path = settings.get('path.logs') or os.path.join(crate_dir, 'logs')
        self.cluster_name = settings.get('cluster.name') or 'cr8'
        self.keep_data = keep_data
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
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
            universal_newlines=True
        ))
        msg = ('Crate launched:\n'
               '\tPID: %s\n'
               '\tLogs: %s\n'
               '\tData: %s')
        if not self.keep_data:
            msg += ' (removed on stop)\n'
        log.info(
            msg,
            proc.pid,
            os.path.join(self.logs_path, self.cluster_name + '.log'),
            self.data_path
        )
        self.monitor.consumers.append(AddrConsumer(self._set_addr))
        self.monitor.start(proc)

        try:
            line_buf = LineBuffer()
            self.monitor.consumers.append(line_buf)
            wait_until(
                lambda: self.http_url and cluster_state_200(self.http_url),
                timeout=30
            )
        except TimeoutError:
            for line in line_buf.lines:
                log.error(line)
            raise
        else:
            self.monitor.consumers.remove(line_buf)
            line_buf = None
        log.info('Cluster ready to process requests')

    def _set_addr(self, protocol, addr):
        log.info('{0:10}: {1}'.format(protocol.capitalize(), addr))
        if protocol == 'http':
            self.http_url = 'http://' + addr

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.communicate(timeout=10)
        if not self.keep_data:
            path = self.data_path.split(',')
            for p in path:
                shutil.rmtree(p)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.stop()


class LineBuffer:

    def __init__(self):
        self.lines = []

    def send(self, line):
        self.lines.append(line.strip())


class AddrConsumer:

    def __init__(self, on_addr):
        self.on_addr = on_addr

    def send(self, line):
        m = ADDRESS_RE.match(line)
        if m:
            self.on_addr(m.group('protocol'), m.group('addr'))


def _openuri(uri):
    if os.path.isfile(uri):
        return open(uri, 'rb')
    return io.BytesIO(urlopen(uri).read())


def _download_and_extract(uri, crate_root):
    filename = os.path.basename(uri)
    crate_folder_name = re.sub('\.tar(\.gz)?$', '', filename)
    crate_dir = os.path.join(crate_root, crate_folder_name)
    if os.path.exists(crate_dir):
        log.info('Skipping download, tarball alrady extracted at %s', crate_dir)
        return crate_dir
    log.info('Downloading %s and extracting to %s', uri, crate_root)
    with _openuri(uri) as tmpfile:
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
    'latest': _from_versions_json('crate'),
    'latest-stable': _from_versions_json('crate'),
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
@argh.arg('-e', '--env', action='append',
          help='Environment variable. Option can be specified multiple times.')
@argh.arg('-s', '--setting', action='append',
          help='Crate setting. Option can be specified multiple times.')
@argh.arg('--keep-data', help='If this is set the data folder will be kept.')
def run_crate(version, env=None, setting=None, crate_root=None, keep_data=False):
    """Launch a crate instance. """
    init_logging()
    settings = {
        'cluster.name': 'cr8-crate-run' + str(random.randrange(1e9))
    }
    crate_dir = get_crate(version, crate_root)
    if setting:
        settings.update(dict(i.split('=') for i in setting))
    if env:
        env = dict(i.split('=') for i in env)
    with CrateNode(crate_dir=crate_dir,
                   env=env,
                   settings=settings,
                   keep_data=keep_data) as node:
        try:
            node.start()
            node.process.wait()
        except KeyboardInterrupt:
            print('Stopping Crate...')


if __name__ == "__main__":
    argh.dispatch_command(run_crate)
