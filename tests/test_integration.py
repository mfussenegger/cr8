import os
import doctest
import subprocess
import functools
import sys
import unittest

from cr8.run_crate import get_crate
from tests.integration_util import teardown, node, setup, transform


def final_teardown(*args):
    try:
        teardown()
    finally:
        node.stop()


class Parser(doctest.DocTestParser):

    def parse(self, string, name='<string>'):
        r = super().parse(string, name)
        for s in r:
            if isinstance(s, doctest.Example):
                s.source = transform(s.source)
        return r


@unittest.skipIf(sys.platform.startswith("win"), "Not supported on Windows")
class SourceBuildTest(unittest.TestCase):

    def test_build_from_branch(self):
        self.assertIsNotNone(get_crate('4.1'))


def load_tests(loader, tests, ignore):
    """
    Intercept test discovery, in order to add doctests from `README.rst`.
    """

    # FIXME: doctests have errors on Windows.
    if sys.platform.startswith("win"):
        return tests

    # Parsing doctests happens early, way before the test suite is invoked.
    # However, the doctest translator needs to know about the TCP address
    # of CrateDB, so it needs to be started right away.
    env = os.environ.copy()
    env['CR8_NO_TQDM'] = 'True'
    node.start()
    assert node.http_host, "http_url must be available"

    # Add integration tests defined as doctests in README.rst.
    tests.addTests(doctest.DocFileSuite(
        os.path.join('..', 'README.rst'),
        globs={
            'sh': functools.partial(
                subprocess.run,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=60,
                shell=True,
                env=env
            )
        },
        optionflags=doctest.NORMALIZE_WHITESPACE | doctest.ELLIPSIS,
        setUp=setup,
        tearDown=final_teardown,
        parser=Parser()
    ))
    return tests


if __name__ == "__main__":
    import unittest
    unittest.main()
