import os
import subprocess
import re
from glob import glob
from pathlib import Path
from typing import Callable

from cr8.misc import parse_version


JAVA_CANDIDATES = tuple(
    glob('/usr/lib/jvm/java-*-openjdk')
    + glob('/usr/lib/java-*')
    + glob('/Library/Java/JavaVirtualMachines/jdk*/Contents/Home')
)
MIN_VERSION_FOR_JVM11 = (2, 3, 6)
VERSION_RE = re.compile(r'(\d+\.\d+\.\d+(_\d+)?)|"(\d+)"')


def _parse_java_version(line: str) -> tuple:
    """ Return the version number found in the first line of `java -version`

    >>> _parse_java_version('openjdk version "11.0.2" 2018-10-16')
    (11, 0, 2)
    """
    m = VERSION_RE.search(line)
    version_str = m and m.group(0).replace('"', '') or '0.0.0'
    if '_' in version_str:
        fst, snd = version_str.split('_', maxsplit=2)
        version = parse_version(fst)
        return (version[1], version[2], int(snd))
    else:
        return parse_version(version_str)


def _detect_java_version(java_home: str) -> tuple:
    p = subprocess.run(
        [Path(java_home) / 'bin' / 'java', '-version'],
        check=True,
        encoding='utf-8',
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    line = p.stdout.split('\n')[0]
    return _parse_java_version(line)


def _find_matching_java_home(version_matches: Callable[[], bool]) -> str:
    java_home = os.environ.get('JAVA_HOME', '')
    for path in filter(os.path.exists, (java_home, ) + JAVA_CANDIDATES):
        version = _detect_java_version(path)
        if version_matches(version):
            return path
    return java_home


def find_java_home(cratedb_version: tuple) -> str:
    """ Return a path to a JAVA_HOME suites for the given CrateDB version """
    if MIN_VERSION_FOR_JVM11 <= cratedb_version < (4, 0):
        # Supports 8 to 11+, use whatever is set
        return os.environ.get('JAVA_HOME', '')
    if cratedb_version < MIN_VERSION_FOR_JVM11:
        return _find_matching_java_home(lambda ver: ver[0] == 8)
    else:
        return _find_matching_java_home(lambda ver: ver[0] >= 11)
