from unittest import main, TestCase
from doctest import DocTestSuite

from cr8 import metrics


class StatsTest(TestCase):

    def test_stats(self):
        hist = metrics.Stats(size=4)
        hist.measure(10.5)
        hist.measure(38.1)
        hist.measure(234.7)
        hist.measure(50.2)

        result = hist.get()
        self.assertEqual(result['min'], 10.5)
        self.assertEqual(result['max'], 234.7)
        self.assertEqual(result['mean'], 83.375)
        self.assertEqual(round(result['median'], 3), 44.150)
        self.assertEqual(round(result['variance'], 3), 10453.476)
        self.assertEqual(round(result['stdev'], 3), 102.242)

        self.assertEqual(result['percentile']['50'], 38.1)
        self.assertEqual(result['percentile']['75'], 50.2)
        self.assertEqual(result['percentile']['99'], 234.7)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(metrics))
    return tests


if __name__ == "__main__":
    main()
