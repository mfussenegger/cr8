from cr8.insert_fake_data import DataFaker
from cr8 import insert_fake_data
from unittest import TestCase, main
from doctest import DocTestSuite
from decimal import Decimal


class TestDataFaker(TestCase):

    def setUp(self):
        self.f = DataFaker()
        self.f.fake.seed(42)

    def test_fake_provider_for_name_column(self):
        provider = self.f.provider_for_column('name', 'string')
        self.assertEqual(provider(), 'Alonza Schmitt')

    def test_fake_provider_for_string_id_column(self):
        provider = self.f.provider_for_column('id', 'string')
        # even with seed set the uuid is not deterministic.. just check the length
        self.assertEqual(len(provider()), len('8731cdac-8671-441d-b07f-e766ffe303e1'))

    def test_fake_provider_for_int_id_column(self):
        provider = self.f.provider_for_column('id', 'integer')
        self.assertEqual(provider(), 1)
        self.assertEqual(provider(), 2)
        provider2 = self.f.provider_for_column('id', 'integer')
        self.assertEqual(provider2(), 1)

    def test_custom_auto_inc(self):
        provider = self.f.provider_for_column('auto_inc', 'long')
        self.assertEqual(provider(), 1)
        self.assertEqual(provider(), 2)

    def test_fake_provider_for_long_id_column(self):
        provider = self.f.provider_for_column('id', 'long')
        self.assertEqual(provider(), 1)

    def test_type_default_provider_for_unknown_int_column(self):
        provider = self.f.provider_for_column(
            'column_name_without_provider', 'integer')
        self.assertEqual(provider(), 1824)  # got random_int provider

    def test_timestamp_column_default(self):
        provider = self.f.provider_for_column('timestamp', 'timestamp')
        self.assertEqual(provider(), 1373158606000)

    def test_timestamp_type_default(self):
        provider = self.f.provider_for_column('some_ts_column', 'timestamp')
        self.assertEqual(provider(), 1373158606000)

    def test_provider_from_mapping(self):
        mapping = {'x': ['random_int', [10, 20]]}
        provider = self.f.provider_from_mapping('x', mapping)
        self.assertEqual(provider(), 20)

    def test_float_type_default(self):
        provider = self.f.provider_for_column('x', 'float')
        self.assertEqual(provider(), -37544673531.0)

    def test_double_type_default(self):
        provider = self.f.provider_for_column('x', 'double')
        self.assertEqual(provider(), Decimal(-37544673531.0))

    def test_ip_type_default(self):
        provider = self.f.provider_for_column('x', 'ip')
        self.assertEqual(provider(), '163.177.121.157')

    def test_geopoint_type_default(self):
        provider = self.f.provider_for_column('location', 'geo_point')
        self.assertEqual(provider(), [50.19364744483815, -85.49806405991995])

    def test_invalid_provider_for_column(self):
        msg = 'No fake provider found for column "x" with type "y"'
        with self.assertRaises(ValueError) as cm:
            self.f.provider_for_column('x', 'y')
        self.assertEqual(str(cm.exception), msg)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(insert_fake_data))
    return tests


if __name__ == "__main__":
    main()
