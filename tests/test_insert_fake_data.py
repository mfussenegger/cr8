from cr8.insert_fake_data import DataFaker, Column
from cr8 import insert_fake_data
from unittest import TestCase, main
from doctest import DocTestSuite
from decimal import Decimal
import datetime


class TestDataFaker(TestCase):

    def setUp(self):
        self.f = DataFaker()
        self.f.fake.seed_instance(42)

    def test_fake_provider_for_name_column(self):
        provider = self.f.provider_for_column(Column('name', 'string', None))
        self.assertEqual(provider(), 'Allison Hill')

    def test_fake_provider_for_string_id_column(self):
        provider = self.f.provider_for_column(Column('id', 'string', None))
        # even with seed set the uuid is not deterministic.. just check the length
        self.assertEqual(len(provider()), len('8731cdac-8671-441d-b07f-e766ffe303e1'))

    def test_fake_provider_for_int_id_column(self):
        provider = self.f.provider_for_column(Column('id', 'integer', None))
        self.assertEqual(provider(), 1)
        self.assertEqual(provider(), 2)
        provider2 = self.f.provider_for_column(Column('id', 'integer', None))
        self.assertEqual(provider2(), 1)

    def test_custom_auto_inc(self):
        provider = self.f.provider_for_column(Column('auto_inc', 'long', None))
        self.assertEqual(provider(), 1)
        self.assertEqual(provider(), 2)

    def test_fake_provider_for_long_id_column(self):
        provider = self.f.provider_for_column(Column('id', 'long', None))
        self.assertEqual(provider(), 1)

    def test_type_default_provider_for_unknown_int_column(self):
        provider = self.f.provider_for_column(
            Column('column_name_without_provider', 'integer', None))
        self.assertEqual(provider(), 598833565)  # got random_int provider

    def test_timestamp_column_default(self):
        provider = self.f.provider_for_column(Column('timestamp', 'timestamp', None))
        dt = provider()
        diff = datetime.datetime(2017, 11, 18, 19, 0, 0) - dt
        self.assertLessEqual(diff, datetime.timedelta(seconds=1))

    def test_timestamp_type_default(self):
        provider = self.f.provider_for_column(Column('some_ts_column', 'timestamp', None))
        dt = provider()
        diff = datetime.datetime(2017, 11, 18, 19, 0, 0) - dt
        self.assertLessEqual(diff, datetime.timedelta(seconds=1))

    def test_timestamp_with_time_zone_type_default(self):
        provider = self.f.provider_for_column(Column('some_ts_column', 'timestamp with time zone', None))
        dt = provider()
        diff = datetime.datetime(2017, 11, 18, 19, 0, 0) - dt
        self.assertLessEqual(diff, datetime.timedelta(seconds=1))

    def test_timestamp_without_time_zone_type_default(self):
        provider = self.f.provider_for_column(Column('some_ts_column', 'timestamp without time zone', None))
        dt = provider()
        diff = datetime.datetime(2017, 11, 18, 19, 0, 0) - dt
        self.assertLessEqual(diff, datetime.timedelta(seconds=1))

    def test_provider_from_mapping(self):
        mapping = {'x': ['random_int', [10, 20]]}
        column = Column("x", "integer", None)
        provider = self.f.provider_from_mapping(column, mapping)
        self.assertEqual(provider(), 20)

    def test_float_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'float', None))
        self.assertEqual(provider(), 31246013015.0)

    def test_real_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'real', None))
        self.assertEqual(provider(), 31246013015.0)

    def test_double_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'double', None))
        self.assertEqual(provider(), Decimal(31246013015.0))

    def test_double_precision_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'double precision', None))
        self.assertEqual(provider(), Decimal(31246013015.0))

    def test_short_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'short', None))
        self.assertEqual(provider(), -18176)

    def test_smallint_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'smallint', None))
        self.assertEqual(provider(), -18176)

    def test_byte_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'byte', None))
        self.assertEqual(provider(), -71)

    def test_char_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'char', None))
        self.assertEqual(provider(), -71)

    def test_ip_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'ip', None))
        self.assertEqual(provider(), '129.153.198.180')

    def test_text_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'text', None))
        self.assertEqual(provider(), 'police')

    def test_bigint_type_default(self):
        provider = self.f.provider_for_column(Column('x', 'bigint', None))
        self.assertEqual(provider(), -7169676182496904803)

    def test_geopoint_type_default(self):
        provider = self.f.provider_for_column(Column('location', 'geo_point', None))
        self.assertEqual(provider(), [50.19364744483815, -85.49806405991995])

    def test_geoshape_type_default(self):
        provider = self.f.provider_for_column(Column('area', 'geo_shape', None))
        self.assertEqual(provider(),
                         'POLYGON (( '
                         '-132.47048275355667 44.147296981090086, '
                         '-131.2679223792111 42.09369742374501, '
                         '-132.14018682738413 37.17894586552094, '
                         '-133.04540290479406 36.10457754779138, '
                         '-142.31051949147854 46.75961787621673, '
                         '-132.47048275355667 44.147296981090086 '
                         '))')

    def test_invalid_provider_for_column(self):
        msg = 'No fake provider found for column "x" with type "y"'
        with self.assertRaises(ValueError) as cm:
            self.f.provider_for_column(Column('x', 'y', None))
        self.assertEqual(str(cm.exception), msg)

    def test_provider_for_object_column_creates_empty_dicts(self):
        provider = self.f.provider_for_column(Column('obj', 'object', None))
        self.assertEqual(provider(), dict())

    def test_provider_for_string_array(self):
        provider = self.f.provider_for_column(Column('foo', 'string_array', None))
        value = provider()
        self.assertEqual(len(value), 40)
        self.assertEqual(value[0], 'born')

    def test_provider_for_nested_string_array(self):
        provider = self.f.provider_for_column(Column('foo', 'string_array_array', None))
        value = provider()
        self.assertEqual(len(value), 40)
        self.assertEqual(len(value[0]), 7)
        self.assertEqual(value[0][0:2], ['agent', 'every'])

    def test_provider_for_bitstring(self):
        provider = self.f.provider_for_column(Column('foo', 'bit', 4))
        value = provider()
        self.assertEqual(value, "0110")

        provider = self.f.provider_for_column(Column('foo', 'bit', 8))
        value = provider()
        self.assertEqual(value, "11110100")


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(insert_fake_data))
    return tests


if __name__ == "__main__":
    main()
