from unittest import main, TestCase
from doctest import DocTestSuite

from cr8 import metrics


class UniformReservoirTest(TestCase):

    def test_fewer_values_than_size(self):
        r = metrics.UniformReservoir(10)
        r.add(10)
        r.add(20)
        self.assertEqual([10, 20], r.values)


class StatsTest(TestCase):

    def test_stats_are_empty_without_values(self):
        hist = metrics.Stats(size=4)
        result = hist.get()
        self.assertEqual(len(result), 1)
        self.assertEqual(result['n'], 0)

    def test_stats_only_has_min_max_mean_with_1_value(self):
        hist = metrics.Stats(size=4)
        hist.measure(23.2)
        result = hist.get()
        self.assertIn('mean', result)
        self.assertIn('min', result)
        self.assertIn('max', result)
        self.assertEqual(result['n'], 1)

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

    def test_n_is_number_of_iterations(self):
        hist = metrics.Stats(size=2)
        for i in range(10):
            hist.measure(i)
        self.assertEqual(hist.get()['n'], 10)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(metrics))
    return tests


if __name__ == "__main__":
    main()
