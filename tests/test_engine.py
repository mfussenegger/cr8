
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

    def test_fail_if_supports_runtime_stats(self):
        stats = Stats()
        stats.measure(103.2)
        stats.measure(205.2)
        timed_stats = TimedStats(1, 2, stats)
        result = Result({}, 'select name', timed_stats, 1)
        with self.assertRaises(FailIf):
            eval_fail_if("{runtime_stats.max} > 30", result)

    def test_fail_if_supports_concurrency(self):
        stats = Stats()
        timed_stats = TimedStats(1, 2, stats)
        result = Result({}, 'select name', timed_stats, 1)
        with self.assertRaises(FailIf):
            eval_fail_if("{concurrency} == 1", result)

    def test_fail_if_supports_bulk_size(self):
        stats = Stats()
        timed_stats = TimedStats(1, 2, stats)
        result = Result({}, 'select name', timed_stats, 1, bulk_size=200)
        eval_fail_if("{bulk_size} < 200", result)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(engine))
    return tests
