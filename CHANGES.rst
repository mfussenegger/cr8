
2016-10-14 0.7.0
================

Breaking (but only a little)
----------------------------

- The ``run-track`` subcommand now does not fail any more if a single
  spec file of the track fails. To achieve the same behaviour as before, you
  can use the ``--failfast`` command line option.

Improvements
------------

- Added a ``--output-fmt`` option to most commands.
  This option can be used to get a succinct output.

- Added a new ``auto_inc`` fake data provider for ``insert-fake-data``.
  This provider may be a bit slow. This is due to the fact that the fake data
  generation utilizes multiple processes and this provider requires
  synchronization. But it's still awesome.

- Spec files now support a ``min_version`` setting.
  This can be used to skip certain queries if the server doesn't meet the
  ``min_version`` requirement.

- Improved the error handling a bit.

- Statements and arguments in spec files can now be defines as callables.

- Added ``meta`` object column to results table.
  It's now possible to add a name to the spec so the benchmark results can
  easily be identified by this spec label.

- Added Crate build date column to version_info in benchmark result table.

- ``timeit`` now shows a progress bar.

- The ``--setting`` and ``-env`` options of ``run-crate`` are now repeatable.


Fixes
-----

- Fixed an issue with the ``num-records`` option of ``insert-fake-data``.
  It didn't work correctly if the number of records specified was smaller than
  the bulk size.

- Fixed some issues with the way Crate is launched using ``run-track``.
  If Crate produced a lot of logging output it could get stuck.


2016-07-04 0.6.0
================

Breaking
--------

- ``hosts`` and ``table`` is now always a named argument.
  This affects ``timeit``, ``insert-json``, ``insert-blob`` and
  ``insert-fake-data``


Features 🍒
-----------

run-track
~~~~~~~~~

Added a new ``run-track`` command.
This command can be used to execute ``track`` files. A ``track`` file is a file
in ``TOML`` format containing a matrix definition of Crate versions, Crate
configurations and spec files.

The command will run each listed Crate version with each configuration and run
all listed spec files against it.


Other improvements
~~~~~~~~~~~~~~~~~~

- Added a new ``run-crate`` command.

- Added a fake-data provider for ``geo_point`` columns.

- Improved the ``--help`` output of most commands.

- Run-spec output is now proper JSON

- Spec files can be written in python

- ``args`` and ``bulk_args`` can now be specified in ``toml`` spec files.


Fixes 💩
--------

- ``runtime_stats['n']`` is no longer capped to 1000

- ``insert-json`` now ignores empty lines instead of causing an error.


2016-06-09 0.5.0
================

Breaking 💔
-----------

Pretty much everything:

- Renamed ``blob upload`` to ``insert-blob``

- Renamed ``json2insert`` to ``insert-json``

- Renamed ``fill-table`` to ``insert-fake-data``

- Removed ``find-perf-regressions``

New & shiny features ✨
-----------------------

run-spec
~~~~~~~~

Added a new command which can be used to "run" spec files. Spec files are
either ``.json`` or ``.toml`` files which contain setup, queries and tear-down
directives. A minimal example::

    [setup]
    statement_files = ["sql/create_countries.sql"]

        [[setup.data_files]]
        target = "countries"
        source = "data/countries.json" # paths are relative to the spec file

    [[queries]]
    statement = "select count(*) from countries"
    iterations = 1000

    [teardown]
    statements = ["drop table countries"]


``run-spec`` will execute the given specification and output runtime statistics.
The result can also directly be inserted into a Crate cluster.

insert-fake-data & insert-json
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Column names are now quoted in the insert statement

insert-fake-data
~~~~~~~~~~~~~~~~

- No longer tries to generate data for generated columns

- Speed improvements

- Added default provider mappings for columns of type ``float``, ``double`` and
  ``ip``

insert-json
~~~~~~~~~~~

- Prints runtime stats after the inserts are finished

timeit
~~~~~~

- Added a histogram and percentiles to the runtime statistics that are printed

- Added a concurrency option


2016-05-19 0.4.0
================

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
