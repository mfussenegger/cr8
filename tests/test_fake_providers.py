import doctest
from unittest import main
from cr8 import fake_providers


def load_tests(loader, tests, ignore):
    flags = (doctest.ELLIPSIS | doctest.IGNORE_EXCEPTION_DETAIL)
    tests.addTests(doctest.DocTestSuite(fake_providers, optionflags=flags))
    return tests


if __name__ == "__main__":
    main()
