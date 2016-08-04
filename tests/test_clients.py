from unittest import main
from doctest import DocTestSuite
from cr8 import clients


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(clients))
    return tests


if __name__ == "__main__":
    main()
