import json
from datetime import datetime, date
from decimal import Decimal
from unittest import main, TestCase
from doctest import DocTestSuite
from cr8 import clients
from cr8.clients import CrateJsonEncoder


class EncoderTest(TestCase):

    def test_decimal_encoding(self):
        s = json.dumps({'x': Decimal(10)}, cls=CrateJsonEncoder)
        self.assertEqual(s, '{"x": "10"}')

    def test_datetime_encoding(self):
        d = {'x': datetime(2017, 1, 26, 23, 33, 1)}
        s = json.dumps(d, cls=CrateJsonEncoder)
        self.assertEqual(s, '{"x": 1485473581000}')

    def test_date_encoding(self):
        d = {'x': date(2017, 1, 26)}
        s = json.dumps(d, cls=CrateJsonEncoder)
        self.assertEqual(s, '{"x": 1485388800000}')


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(clients))
    return tests


if __name__ == "__main__":
    main()
