
from unittest import TestCase
from doctest import DocTestSuite
from cr8.engine import eval_fail_if, Result, FailIf, TimedStats
from cr8.metrics import Stats
from cr8 import engine


class FailIfTest(TestCase):

    def test_fail_if_fails(self):
        timed_stats = TimedStats(1, 2, Stats())
        result = Result({}, 'select name', timed_stats, 1)
        with self.assertRaises(FailIf):
            eval_fail_if("'{statement}' == 'select name'", result)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(engine))
    return tests
