from unittest import TestCase
from cr8.timeit import Result
from cr8.metrics import Stats


class ResultTest(TestCase):

    def test_short_result_output_with_only_1_measurement(self):
        stats = Stats(1)
        stats.measure(23.4)
        result = Result(
            version_info={},
            meta=None,
            statement='select name from sys.cluster',
            started=10,
            ended=20,
            stats=stats,
            concurrency=1,
            output_fmt='short')
        self.assertEqual(
            str(result),
            ('Runtime:\n'
             '    mean: 23.400 +/- 0.000\n'
             '    min:  23.400\n'
             '    max:  23.400')
        )

    def test_short_result_output_with_more_measurements(self):
        stats = Stats(4)
        stats.measure(23.4)
        stats.measure(48.7)
        stats.measure(32.5)
        stats.measure(15.9)
        result = Result(
            version_info={},
            meta=None,
            statement='select name from sys.cluster',
            started=10,
            ended=20,
            stats=stats,
            concurrency=1,
            output_fmt='short')
        self.assertEqual(
            str(result),
            ('Runtime:\n'
             '    mean: 30.125 +/- 14.121\n'
             '    min:  15.900\n'
             '    max:  48.700\n'
             'Percentile:\n'
             '    50:   23.400\n'
             '    99.9: 48.700')
        )
