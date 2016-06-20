from unittest import main
from doctest import DocTestSuite
from cr8 import insert_json


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(insert_json))
    return tests


if __name__ == "__main__":
    main()
