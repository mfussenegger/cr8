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
import fnmatch
import socket
import ssl
import platform
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from functools import partial
from itertools import cycle
from typing import Optional, Dict, Any, List, NamedTuple
from urllib.request import urlopen

from cr8.java_magic import find_java_home
from cr8.misc import parse_version, init_logging
from cr8.engine import DotDict
from cr8.exceptions import ArgumentError

log = logging.getLogger(__name__)

NO_SSL_VERIFY_CTX = ssl._create_unverified_context()
RELEASE_URL = 'https://cdn.crate.io/downloads/releases/crate-{version}.tar.gz'
RELEASE_PLATFORM_URL = 'https://cdn.crate.io/downloads/releases/cratedb/{arch}_{os}/crate-{version}.{extension}'
VERSION_RE = re.compile(r'^(\d+\.\d+\.\d+)$')
DYNAMIC_VERSION_RE = re.compile(r'^((\d+|x)\.(\d+|x)\.(\d+|x))$')
BRANCH_VERSION_RE = re.compile(r'^((\d+)\.(\d+))$')
FOLDER_VERSION_RE = re.compile(r'crate-(\d+\.\d+\.\d+)')
REPO_URL = 'https://github.com/crate/crate.git'

DEFAULT_SETTINGS = {
    'discovery.initial_state_timeout': 0,
    'network.host': '127.0.0.1',
    'udc.enabled': False
}


class ReleaseUrlSegments(NamedTuple):
    arch: str
    os: str
    extension: str

    @classmethod
    def create(cls):
        extension = 'tar.gz'
        if sys.platform.startswith('linux'):
            os = 'linux'
        elif sys.platform.startswith('win32'):
            os = 'windows'
            extension = 'zip'
        elif sys.platform.startswith('darwin'):
            os = 'mac'
        else:
            raise ValueError(f'Unsupported platform: {sys.platform}')

        machine = platform.machine()
        if machine.startswith('arm'):
            arch = 'aarch64'
        else:
            arch = 'x64'

        return ReleaseUrlSegments(arch=arch, os=os, extension=extension)

    @property
    def platform_key(self):
        return f'{self.arch}_{self.os}'

    def get_uri(self, version):
        return RELEASE_PLATFORM_URL.format(
            version=version,
            os=self.os,
            extension=self.extension,
            arch=self.arch
        )


def _format_cmd_option_legacy(k, v):
    return '-Des.{0}={1}'.format(k, v)


def _format_cmd_option(k, v):
    if isinstance(v, bool):
        return '-C{0}={1}'.format(k, str(v).lower())
    return '-C{0}={1}'.format(k, v)


def _extract_version(crate_dir) -> tuple:
    m = FOLDER_VERSION_RE.findall(crate_dir)
    if m:
        return parse_version(m[0])
    return (1, 0, 0)


class OutputMonitor:

    def __init__(self):
        self.consumers = []

    def _consume(self, proc):
        try:
            for line in proc.stdout:
                for consumer in self.consumers:
                    if callable(consumer):
                        consumer(line)
                    else:
                        consumer.send(line)
        except Exception:
            if proc.returncode is not None:
                return
            raise

    def start(self, proc):
        out_thread = threading.Thread(target=self._consume, args=(proc,))
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


def _is_up(host: str, port: int):
    try:
        conn = _create_connection(host, port)
        conn.close()
        return True
    except (socket.gaierror, ConnectionRefusedError):
        return False


def _create_connection(host: str, port: int):
    if host[0] == '[' and host[-1] == ']':
        host = host[1:-1]
    return socket.create_connection((host, port))


def _has_ssl(host: str, port: int):
    try:
        with NO_SSL_VERIFY_CTX.wrap_socket(_create_connection(host, port)) as s:
            s.close()
            return True
    except (socket.gaierror, ssl.SSLError):
        return False


def cluster_state_200(url):
    try:
        with urlopen(url, context=NO_SSL_VERIFY_CTX) as r:
            p = json.loads(r.read().decode('utf-8'))
            return int(p['status']) == 200
    except Exception as e:
        log.debug(e)
        return False


def _get_settings(settings=None) -> Dict[str, Any]:
    s = DEFAULT_SETTINGS.copy()
    if settings:
        s.update(settings)
    return s


def _try_print_log(logfile):
    try:
        with open(logfile) as f:
            for line in f:
                log.error(line)
    except Exception:
        pass


def _ensure_running(proc):
    result = proc.poll()
    if result:
        raise SystemExit('Process exited: ' + str(result))
    return True


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
                 env: Dict[str, Any] = None,
                 settings: Dict[str, Any] = None,
                 keep_data: bool = False,
                 java_magic: bool = False,
                 version: tuple = None) -> None:
        """Create a CrateNode

        Args:
            crate_dir: Path to the extracted Crate tarball
            env: Environment variables with which the Crate process will be
                started.
            settings: Additional Crate settings.
            java_magic: If set to true, it will attempt to set JAVA_HOME to
                some path that contains a Java version suited to run the given
                CrateDB instance.
            version:
                The CrateDB version as tuple in the format (major, minor, hotfix).
                This is usually inferred from the given `crate_dir`, but can be
                passed explicitly to overrule the detection mechanism.
                This argument is used to provide the right defaults and use the
                right commandline argument syntax to launch CrateDB.

        """
        super().__init__()
        self.crate_dir = crate_dir
        self.version = version or _extract_version(crate_dir)
        self.env = env or {}
        if java_magic:
            java_home = find_java_home(self.version)
        else:
            java_home = os.environ.get('JAVA_HOME', '')
        self.env.setdefault('JAVA_HOME', java_home)
        self.env.setdefault('LANG',
                            os.environ.get('LANG', os.environ.get('LC_ALL')))
        if not self.env['LANG']:
            raise SystemExit('Your locale are not configured correctly. '
                             'Please set LANG or alternatively LC_ALL.')
        self.monitor = OutputMonitor()
        self.process = None  # type: Optional[subprocess.Popen]
        self.http_url = None  # type: Optional[str]
        self.http_host = None  # type: Optional[str]
        start_script = 'crate.bat' if sys.platform == 'win32' else 'crate'

        settings = _get_settings(settings)
        if self.version < (1, 1, 0):
            settings.setdefault('discovery.zen.ping.multicast.enabled', False)
        self.data_path = settings.get('path.data') or tempfile.mkdtemp()
        self.logs_path = settings.get('path.logs') or os.path.join(crate_dir, 'logs')
        self.cluster_name = settings.get('cluster.name') or 'cr8'
        self.keep_data = keep_data
        settings['path.data'] = self.data_path
        settings['cluster.name'] = self.cluster_name
        if self.version < (1, 0, 0):
            _format_option = _format_cmd_option_legacy
        else:
            _format_option = _format_cmd_option
        args = [_format_option(k, v) for k, v in settings.items()]
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

        msg = ('CrateDB launching:\n'
               '    PID: %s\n'
               '    Logs: %s\n'
               '    Data: %s')
        if not self.keep_data:
            msg += ' (removed on stop)\n'

        logfile = os.path.join(self.logs_path, self.cluster_name + '.log')
        log.info(
            msg,
            proc.pid,
            logfile,
            self.data_path
        )
        self.addresses = DotDict({})
        self.monitor.consumers.append(AddrConsumer(self._set_addr))
        self.monitor.start(proc)

        log_lines = []
        self.monitor.consumers.append(log_lines.append)
        spinner = cycle(['/', '-', '\\', '|'])

        def show_spinner():
            if sys.stdout.isatty():
                print(next(spinner), end='\r')
            return True
        try:
            wait_until(
                lambda: show_spinner() and _ensure_running(proc) and self.http_host,
                timeout=60
            )
            host = self.addresses.http.host
            port = self.addresses.http.port
            wait_until(
                lambda: _ensure_running(proc) and _is_up(host, port),
                timeout=30
            )
            if _has_ssl(host, port):
                self.http_url = self.http_url.replace('http://', 'https://')
            wait_until(
                lambda: show_spinner() and cluster_state_200(self.http_url),
                timeout=30
            )
        except (SystemExit, TimeoutError):
            if not log_lines:
                _try_print_log(logfile)
            else:
                for line in (x.rstrip() for x in log_lines if x):
                    log.error(line)
            raise SystemExit("CrateDB didn't start in time or couldn't form a cluster.") from None
        else:
            self.monitor.consumers.remove(log_lines.append)
        log.info('Cluster ready to process requests')

    def _set_addr(self, protocol, addr):
        log.info('{0:10}: {1}'.format(protocol.capitalize(), addr))
        host, port = addr.rsplit(':', 1)
        port = int(port)
        self.addresses[protocol] = Address(host, port)
        if protocol == 'http':
            self.http_host = addr
            self.http_url = 'http://' + addr

    def stop(self):
        if self.process:
            self.process.terminate()
            self.process.communicate(timeout=120)
        self.addresses = DotDict({})
        self.http_host = None
        self.http_url = None
        if not self.keep_data:
            path = self.data_path.split(',')
            for p in path:
                shutil.rmtree(p)

    def __enter__(self):
        return self

    def __exit__(self, *ex):
        self.stop()


class Address(NamedTuple):
    host: str
    port: int


class AddrConsumer:

    ADDRESS_RE = re.compile(
        r'.*\[(?P<protocol>http|i.c.p.h.CrateNettyHttpServerTransport|o.e.h.n.Netty4HttpServerTransport|o.e.h.HttpServer|psql|transport|o.e.t.TransportService)\s*\] \[.*\] .*'
        r'publish_address {'
        r'(?:(inet\[[A-Za-z-\.]*/)|([A-Za-z\.]*/))?'
        r'?(?P<addr>\[?[\d\.:]+\]?:?\d+)'
        r'(?:\])?'
        r'}'
    )
    PROTOCOL_MAP = {
        'i.c.p.h.CrateNettyHttpServerTransport': 'http',
        'o.e.h.n.Netty4HttpServerTransport': 'http',
        'o.e.h.HttpServer': 'http',
        'o.e.t.TransportService': 'transport'
    }

    def __init__(self, on_addr):
        self.on_addr = on_addr

    @staticmethod
    def _parse(line):
        """ Parse protocol and bound address from log message

        >>> AddrConsumer._parse('NONE')
        (None, None)

        >>> AddrConsumer._parse('[INFO ][i.c.p.h.CrateNettyHttpServerTransport] [Widderstein] publish_address {127.0.0.1:4200}, bound_addresses {[fe80::1]:4200}, {[::1]:4200}, {127.0.0.1:4200}')
        ('http', '127.0.0.1:4200')

        >>> AddrConsumer._parse('[INFO ][o.e.h.n.Netty4HttpServerTransport] [Piz Forun] publish_address {127.0.0.1:4200}, bound_addresses {[::1]:4200}, {127.0.0.1:4200}')
        ('http', '127.0.0.1:4200')

        >>> AddrConsumer._parse('[INFO ][o.e.t.TransportService   ] [Widderstein] publish_address {127.0.0.1:4300}, bound_addresses {[fe80::1]:4300}, {[::1]:4300}, {127.0.0.1:4300}')
        ('transport', '127.0.0.1:4300')

        >>> AddrConsumer._parse('[INFO ][psql                     ] [Widderstein] publish_address {127.0.0.1:5432}, bound_addresses {127.0.0.1:5432}')
        ('psql', '127.0.0.1:5432')
        """
        m = AddrConsumer.ADDRESS_RE.match(line)
        if not m:
            return None, None
        protocol = m.group('protocol')
        protocol = AddrConsumer.PROTOCOL_MAP.get(protocol, protocol)
        return protocol, m.group('addr')

    def send(self, line):
        protocol, addr = AddrConsumer._parse(line)
        if protocol:
            self.on_addr(protocol, addr)


def _openuri(uri):
    if os.path.isfile(uri):
        return open(uri, 'rb')
    return io.BytesIO(urlopen(uri).read())


def _can_use_cache(uri, crate_dir):
    if not os.path.exists(crate_dir):
        return False
    os.utime(crate_dir)  # update mtime to avoid removal
    if os.path.isfile(uri):
        with _openuri(uri) as f:
            checksum = sha1(f.read()).hexdigest()
            return os.path.exists(os.path.join(crate_dir, checksum))
    # Always enable use of the cache if the source is not local
    return True


def _download_and_extract(uri, crate_root):
    filename = os.path.basename(uri)
    crate_folder_name = re.sub(r'\.tar(\.gz)?$', '', filename)
    crate_dir = os.path.join(crate_root, crate_folder_name)

    if _can_use_cache(uri, crate_dir):
        log.info('Skipping download, tarball alrady extracted at %s', crate_dir)
        return crate_dir
    elif os.path.exists(crate_dir):
        shutil.rmtree(crate_dir, ignore_errors=True)
    log.info('Downloading %s and extracting to %s', uri, crate_root)
    with _openuri(uri) as tmpfile:
        with tarfile.open(fileobj=tmpfile) as t:
            t.extraction_filter = getattr(tarfile, 'data_filter', (lambda member, path: member))
            t.extractall(crate_root)
        tmpfile.seek(0)
        checksum = sha1(tmpfile.read()).hexdigest()
        with open(os.path.join(crate_dir, checksum), 'a'):
            os.utime(os.path.join(crate_dir, checksum))
    return crate_dir


def _from_versions_json(key):
    def retrieve():
        with urlopen('https://cratedb.com/releases.json') as r:
            if r.headers.get('Content-Encoding') == 'gzip':
                with gzip.open(r, 'rt') as r:
                    versions = json.loads(r.read())
            else:
                versions = json.loads(r.read().decode('utf-8'))
        segments = ReleaseUrlSegments.create()
        downloads = versions[key]['downloads']
        if segments.platform_key in downloads:
            return downloads[segments.platform_key]['url']
        else:
            return downloads['tar.gz']['url']
    return retrieve


RELEASE_RE = re.compile(r'.*>(?P<filename>crate-(?P<version>\d+\.\d+\.\d+)\.tar\.gz)<.*')


def _retrieve_crate_versions():
    base_uri = 'https://cdn.crate.io/downloads/releases/'
    with urlopen(base_uri) as r:
        lines = (line.decode('utf-8') for line in r)
        for line in lines:
            m = RELEASE_RE.match(line)
            if m:
                yield m.group('version')


def _find_matching_version(versions, version_pattern):
    """
    Return the first matching version

    >>> _find_matching_version(['1.1.4', '1.0.12', '1.0.5'], '1.0.x')
    '1.0.12'

    >>> _find_matching_version(['1.1.4', '1.0.6', '1.0.5'], '2.x.x')
    """
    pattern = fnmatch.translate(version_pattern.replace('x', '*'))
    return next((v for v in versions if re.match(pattern, v)), None)


_version_lookups = {
    'latest': _from_versions_json('stable'),
    'latest-stable': _from_versions_json('stable'),
    'latest-testing': _from_versions_json('testing'),
    'latest-nightly': _from_versions_json('nightly')
}


def _get_uri_from_released_version(version: str) -> str:
    version_tup = parse_version(version)
    if version_tup < (4, 2, 0):
        return RELEASE_URL.format(version=version)
    try:
        return ReleaseUrlSegments.create().get_uri(version)
    except ValueError:
        # Unsupported platform, just return the linux tarball
        return RELEASE_URL.format(version=version)


def _lookup_uri(version):
    if version in _version_lookups:
        version = _version_lookups[version]()
    m = VERSION_RE.match(version)
    if m:
        return _get_uri_from_released_version(m.group(0))
    m = DYNAMIC_VERSION_RE.match(version)
    if m:
        versions = sorted(map(parse_version, list(_retrieve_crate_versions())))
        versions = ['.'.join(map(str, v)) for v in versions[::-1]]
        release = _find_matching_version(versions, m.group(0))
        if release:
            return _get_uri_from_released_version(release)
    return version


def _is_project_repo(src_repo):
    path = Path(src_repo)
    return (
        path.is_dir()
        and (path / ".git").exists()
        and ((path / "gradlew").exists() or (path / "mvnw").exists())
    )


def _build_tarball(src_repo) -> Path:
    """ Build a tarball from src and return the path to it """
    run = partial(subprocess.run, cwd=src_repo, check=True, stdin=subprocess.DEVNULL)
    run(['git', 'clean', '-xdff'])
    path = Path(src_repo)
    if (path / 'es' / 'upstream').exists():
        run(['git', 'submodule', 'update', '--init', '--', 'es/upstream'])
    if (path / "mvnw").exists():
        run([
            "./mvnw",
            "-T", "1C",
            "clean",
            "package",
            "-DskipTests=true",
            "-Dcheckstyle.skip",
            "-Djacoco.skip=true"
        ])
        distributions = path / "app" / "target"
    else:
        run(['./gradlew', '--parallel', '--no-daemon', 'clean', 'distTar'])
        distributions = path / 'app' / 'build' / 'distributions'
    return next(distributions.glob('crate-*.tar.gz'))


def _extract_tarball(tarball):
    with tarfile.open(tarball) as t:
        first_file = t.getnames()[0]
        # First file name might be the folder, or a file inside the folder
        # Normalize to folder
        folder_name = os.path.dirname(first_file)
        folder_name = folder_name == "" and first_file or folder_name
        target = tarball.parent / folder_name
        if target.exists():
            shutil.rmtree(target)
        t.extraction_filter = getattr(tarfile, 'data_filter', (lambda member, path: member))
        t.extractall(tarball.parent)
    return str(tarball.parent / folder_name)


def _build_from_release_branch(branch, crate_root):
    crates = Path(crate_root)
    src_repo = crates / 'sources_tmp'
    run_in_repo = partial(
        subprocess.run,
        cwd=src_repo,
        check=True,
        stdin=subprocess.DEVNULL
    )
    if not src_repo.exists() or not (src_repo / '.git').exists():
        clone = ['git', 'clone', REPO_URL, 'sources_tmp']
        subprocess.run(clone, cwd=crate_root, check=True, stdin=subprocess.DEVNULL)
    else:
        run_in_repo(['git', 'fetch'])
    run_in_repo(['git', 'checkout', branch])
    run_in_repo(['git', 'pull', 'origin', branch])
    rev_parse_p = run_in_repo(
        ['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE, encoding='utf-8')
    revision = rev_parse_p.stdout.strip()
    builds_dir = crates / 'builds'
    os.makedirs(builds_dir, exist_ok=True)
    cached_build = builds_dir / (revision + '.tar.gz')
    if os.path.isfile(cached_build):
        return _extract_tarball(cached_build)
    tarball = _build_tarball(str(src_repo))
    shutil.copy(tarball, cached_build)
    return _extract_tarball(tarball)


def _remove_old_crates(path):
    now = time.time()
    s7days_ago = now - (7 * 24 * 60 * 60)
    with contextlib.suppress(FileNotFoundError):
        old_unused_dirs = (e for e in os.scandir(path)
                           if e.is_dir() and e.stat().st_mtime < s7days_ago)
        for e in old_unused_dirs:
            last_use = datetime.fromtimestamp(e.stat().st_mtime)
            msg = f'Removing from cache: {e.name} (last use: {last_use:%Y-%m-%d %H:%M})'
            print(msg, file=sys.stderr)
            shutil.rmtree(e.path)


def _crates_cache() -> str:
    """ Return the path to the crates cache folder """
    return os.environ.get(
        'XDG_CACHE_HOME',
        os.path.join(os.path.expanduser('~'), '.cache', 'cr8', 'crates'))


def get_crate(version, crate_root=None):
    """Retrieve a Crate tarball, extract it and return the path.

    Args:
        version: The Crate version to get.
            Can be specified in different ways:

            - A concrete version like '0.55.0'
            - A version including a `x` as wildcards. Like: '1.1.x' or '1.x.x'.
              This will use the latest version that matches.
            - Release branch, like `3.1`
            - Any branch: 'branch:<branchName>'
            - An alias: 'latest-stable' or 'latest-testing'
            - A URI pointing to a crate tarball
        crate_root: Where to extract the tarball to.
            If this isn't specified ``$XDG_CACHE_HOME/.cache/cr8/crates``
            will be used.
    """
    if not crate_root:
        crate_root = _crates_cache()
        os.makedirs(crate_root, exist_ok=True)
        _remove_old_crates(crate_root)
    if _is_project_repo(version):
        return _extract_tarball(_build_tarball(version))
    m = BRANCH_VERSION_RE.match(version)
    if m:
        return _build_from_release_branch(m.group(0), crate_root)
    if version.startswith('branch:'):
        return _build_from_release_branch(version[len('branch:'):], crate_root)
    uri = _lookup_uri(version)
    crate_dir = _download_and_extract(uri, crate_root)
    return crate_dir


def _parse_options(options: List[str]) -> Dict[str, str]:
    """ Parse repeatable CLI options

    >>> opts = _parse_options(['cluster.name=foo', 'CRATE_JAVA_OPTS="-Dxy=foo"'])
    >>> print(json.dumps(opts, sort_keys=True))
    {"CRATE_JAVA_OPTS": "\\"-Dxy=foo\\"", "cluster.name": "foo"}
    """
    try:
        return dict(i.split('=', maxsplit=1) for i in options)  # type: ignore
    except ValueError:
        raise ArgumentError(
            f'Option must be in format <key>=<value>, got: {options}')


def create_node(
        version,
        env=None,
        setting=None,
        crate_root=None,
        keep_data=False,
        java_magic=False,
):
    init_logging(log)
    settings = {
        'cluster.name': 'cr8-crate-run' + str(random.randrange(int(1e9)))
    }
    crate_dir = get_crate(version, crate_root)
    if setting:
        settings.update(_parse_options(setting))
    if env:
        env = _parse_options(env)
    return CrateNode(
        crate_dir=crate_dir,
        env=env,
        settings=settings,
        keep_data=keep_data,
        java_magic=java_magic
    )


@argh.arg('version', help='Crate version to run')
@argh.arg('-e', '--env', action='append',
          help='Environment variable. Option can be specified multiple times.')
@argh.arg('-s', '--setting', action='append',
          help='Crate setting. Option can be specified multiple times.')
@argh.arg('--keep-data', help='If this is set the data folder will be kept.')
@argh.arg(
    '--disable-java-magic',
    help='Disable the logic to detect a suitable JAVA_HOME')
@argh.wrap_errors([ArgumentError])
def run_crate(
        version,
        *,
        env=None,
        setting=None,
        crate_root=None,
        keep_data=False,
        disable_java_magic=False,
):
    """Launch a crate instance.

    Supported version specifications:
        - Concrete version like "0.55.0" or with wildcard: "1.1.x"
        - An alias (one of [latest-nightly, latest-stable, latest-testing])
        - A URI pointing to a CrateDB tarball (in .tar.gz format)
        - A URI pointing to a checked out CrateDB repo directory
        - A branch like `branch:master` or `branch:my-new-feature`

    run-crate supports command chaining. To launch a CrateDB node and another
    sub-command use:

        cr8 run-crate <ver> -- timeit -s "select 1" --hosts '{node.http_url}'

    To launch any (blocking) subprocess, prefix the name with '@':

        cr8 run-crate <version> -- @http '{node.http_url}'

    If run-crate is invoked using command chaining it will exit once all
    chained commands finished.

    The postgres host and port are available as {node.addresses.psql.host} and
    {node.addresses.psql.port}
    """
    with create_node(
            version,
            env,
            setting,
            crate_root,
            keep_data,
            java_magic=not disable_java_magic,
    ) as n:
        try:
            n.start()
            n.process.wait()
        except KeyboardInterrupt:
            print('Stopping Crate...')


if __name__ == "__main__":
    argh.dispatch_command(run_crate)
