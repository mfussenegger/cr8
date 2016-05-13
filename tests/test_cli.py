from cr8 import cli
from cr8.cli import dicts_from_stdin, lines_from_stdin
from doctest import DocTestSuite
from unittest import TestCase, main
from unittest.mock import patch
import io


class CliTest(TestCase):

    @patch('sys.stdin',
           new_callable=lambda: io.StringIO('{"name": "n1"}\n{"name": "n2"}'))
    def test_dicts_from_stdin_multi_json(self, stdin):
        stdin.isatty = lambda: False

        dicts = iter(dicts_from_stdin())
        d1 = next(dicts)
        d2 = next(dicts)

        self.assertEqual({"name": "n1"}, d1)
        self.assertEqual({"name": "n2"}, d2)

        self.assertRaises(StopIteration, next, dicts)

    @patch('sys.stdin', new_callable=lambda: io.StringIO('{\n    "name": "n1"\n}'))
    def test_dicts_from_stdin_single_json(self, stdin):
        stdin.isatty = lambda: False

        dicts = iter(dicts_from_stdin())
        d1 = next(dicts)
        self.assertEqual({"name": "n1"}, d1)
        self.assertRaises(StopIteration, next, dicts)

    @patch('sys.stdin', new_callable=lambda: io.StringIO(
        '[\n{\n"name": "n1"\n},\n{\n"name": "n2"\n}\n]'))
    def test_dicts_from_json_list_of_obj(self, stdin):

        dicts = iter(dicts_from_stdin())
        d1 = next(dicts)
        d2 = next(dicts)

        self.assertEqual({"name": "n1"}, d1)
        self.assertEqual({"name": "n2"}, d2)

        self.assertRaises(StopIteration, next, dicts)

    @patch('sys.stdin', new_callable=io.StringIO)
    def test_lines_from_stdin_isatty_but_default(self, stdin):
        stdin.isatty = lambda: True

        lines = list(lines_from_stdin('default'))
        self.assertEqual(['default'], lines)


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(cli))
    return tests


if __name__ == "__main__":
    main()
