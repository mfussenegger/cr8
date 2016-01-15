
from doctest import DocTestSuite
from cr8 import misc


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(misc))
    return tests
