from cr8.fill_table import DataFaker
from unittest import TestCase


class TestDataFaker(TestCase):

    def setUp(self):
        self.f = DataFaker()
        self.f.fake.seed(42)

    def test_fake_provider_for_name_column(self):
        provider = self.f.provider_for_column('name', 'string')
        self.assertEqual(provider(), 'Alonza Schmidt')

    def test_fake_provider_for_string_id_column(self):
        provider = self.f.provider_for_column('id', 'string')
        # even with seed set the uuid is not deterministic.. just check the length
        self.assertEqual(len(provider()), len('8731cdac-8671-441d-b07f-e766ffe303e1'))

    def test_fake_provider_for_int_id_column(self):
        provider = self.f.provider_for_column('id', 'integer')
        self.assertEqual(provider(), 1824)
