import os
from unittest import TestCase
from doctest import DocTestSuite

from cr8.bench_spec import load_spec

from cr8 import engine


class SpecTest(TestCase):

    def test_session_settings_from_spec(self):
        spec = self.get_spec('sample.py')
        self.assertEqual(spec.session_settings, {'application_name': 'my_app', 'timezone': 'UTC'})

    def test_session_settings_from_toml(self):
        spec = self.get_spec('sample.toml')
        self.assertEqual(spec.session_settings, {'application_name': 'my_app', 'timezone': 'UTC'})

    def test_session_settings_from_json(self):
        spec = self.get_spec('count_countries.json')
        self.assertEqual(spec.session_settings, {'application_name': 'my_app', 'timezone': 'UTC'})

    def get_spec(self, name):
        return load_spec(os.path.abspath(os.path.join(os.path.dirname(__file__), '../specs/', name)))


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(engine))
    return tests
