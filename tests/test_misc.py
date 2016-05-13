
from unittest import TestCase, main
from doctest import DocTestSuite
from cr8 import misc


class MiscTest(TestCase):

    def test_as_bulk_queries(self):
        queries = [
            ('x', (1, 2)),
            ('x', (3, 4)),
            ('x', (5, 6)),
            ('y', (1, 2))]
        bulk_queries = sorted(list(misc.as_bulk_queries(queries, 2)))
        self.assertEqual(bulk_queries, [
            ('x', [(1, 2), (3, 4)]),
            ('x', [(5, 6)]),
            ('y', [(1, 2)])
        ])


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(misc))
    return tests


if __name__ == "__main__":
    main()
