from unittest import main
from doctest import DocTestSuite
from cr8 import run_crate


def load_tests(loader, tests, ignore):
    tests.addTests(DocTestSuite(run_crate))
    return tests


if __name__ == "__main__":
    main()
