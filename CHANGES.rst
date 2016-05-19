

Unreleased
==========

- Python 3.4 support has been dropped.

- Subcommands that take numbers as arguments now support python literal
  notation. So something like ``1e3`` can be used.

Features
--------

fill-table
~~~~~~~~~~

- Consumes less memory and is faster since it no longer generates all data
  upfront but starts inserting as soon as possible.

- Added a concurrency option to control how many requests to make in parallel
  (at most).

- Columns of type long are automatically mapped to the ``random_int``
  provider.

json2insert
~~~~~~~~~~~

- ``json2insert`` can now be used to bulk insert JSON files.
  The following input formats are supported::

    1 JSON object per line

        {"name": "n1"}
        {"name": "n2"}

    Or 1 JSON object:

        {
            "name": "n1"
        }

    Or a list of JSON objects:

        [
            {"name": "n1"},
            {"name": "n2"},
        ]

  The input must be fed into ``stdin``.

- The ``--bulk-size`` and ``--concurrency`` options have been added.
