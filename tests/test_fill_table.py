from cr8.fill_table import DataFaker
from cr8 import fill_table
from unittest import TestCase, main
from doctest import DocTestSuite


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
        self.assertEqual(provider(), 1824)

    def test_fake_provider_for_long_id_column(self):
        provider = self.f.provider_for_column('id', 'long')
        self.assertEqual(provider(), 1824)

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


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(fill_table))
    return tests


if __name__ == "__main__":
    main()
