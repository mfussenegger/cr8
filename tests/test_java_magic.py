from unittest import TestCase
from doctest import DocTestSuite
from cr8 import java_magic
from cr8.java_magic import _parse_java_version


class JavaVersionParsingTest(TestCase):

    def assertVersion(self, line, expected):
        version = _parse_java_version(line)
        self.assertEqual(version, expected)

    def test_java_8_line(self):
        self.assertVersion('openjdk version "1.8.0_202"', (8, 0, 202))

    def test_java_10_line(self):
        self.assertVersion('openjdk version "10.0.2" 2018-07-17', (10, 0, 2))

    def test_java_11_line(self):
        self.assertVersion('java 11.0.1 2018-10-16 LTS', (11, 0, 1))

    def test_java_12_line(self):
        self.assertVersion('openjdk version "12" 2019-03-19', (12, 0, 0))


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(java_magic))
    return tests
