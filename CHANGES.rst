2024-03-20 0.26.0
=================

- Added support for python 3.12

- Fixed a permission denied error that could occur using `cr8 run-crate` with a
  branch version specification.

2023-10-24 0.25.0
=================

- Added compatibility for ``argh>=0.30.1``. Due to this several of the main
  entry point functions have now mandatory keyword arguments and no longer
  support positional arguments.
  For CLI use nothing changes, but for API consumers this is a breaking change.

2023-10-12 0.24.0
=================

- Fixed an issue that prevented ``cr8 run-crate path/to/source`` from working
  if used as part of ``git bisect run``.

- Added handling for maven as build system when using ``cr8 run-crate`` with a
  source path or ``branch:<name>``.

- Changed ``cr8 insert-from-sql`` to quote the column names used in the insert
  statements to make them case sensitive.


2023-07-10 0.23.0
=================

- Use ``tomllib`` if available (Python 3.11+) in favour of ``toml``. The
  ``tomllib`` parser seems more resilient when parsing multi-line strings.

- Fixed a column lookup error in ``insert-fake-data`` that happened when using
  a mapping file.

- Increased the timeout for ``CrateNode.stop()`` to give the CrateDB shutdown
  procedure more time to shutdown gracefully.

2023-01-16 0.22.0
=================

- Added support for HTTP(s) URLs as source to ``load_data`` of spec files.

- Added a ``--re-name`` option to the ``run-spec`` command which allows to only
  run queries where the ``name`` property matches the given regex.

- Added a ``warmup`` option to queries in spec files

- Dropped Python 3.6 Support

2022-02-08 0.21.1
=================

- Changed ``cr8 run-crate`` to implicitly create the cache folder if it is
  missing instead of raising an error.

- Improved the performance of the ``insert-fake-data ``auto_inc`` provider for
  ``id`` columns of type ``integer``.

2022-01-26 0.21.0
=================

- Added a ``setup.data_cmds`` directive to allow data generation via external
  commands within spec files.
  The command must output one JSON object per line.

- Added a data provider for the ``bit`` data type.


2020-09-23 0.20.1
=================

- Added ``asyncpg`` as non-optional dependency, as it is required by
  ``insert-from-sql``.

2020-09-22 0.20.0
=================

- Added a new ``insert-from-sql`` command that can be used to insert data from
  another CrateDB or PostgreSQL server.

- Extended ``insert-json`` to also support a list of JSON objects within a
  single line as input format. (Something like ``[{"name": "n1"}, {"name":
  "n2"}]``)


2020-07-15 0.19.3
=================

- ``run-crate`` now uses a system specific CrateDB tarball if available. This
  is required for the platform dependent builds starting with CrateDB 4.2.

2020-06-10 0.19.2
=================

- Extended the ``faker`` version constraint to ``>= 4, < 5``, this should make
  it easier to use ``cr8`` as a dependency in projects where another dependency
  uses ``faker`` as well.

2020-05-14 0.19.1
=================

- Extended ``run_crate`` to be forward compatible with an upcoming change in
  the log format in CrateDB.

- Fixed the handling of the ``?verify_ssl=false`` parameter in ``--hosts``

2020-05-02 0.19.0
=================

- Spec files now support a ``name`` property in the ``queries`` section to make
  it easier to analyse the results.

- ``run-spec`` can now also be used with PostgreSQL if using ``asyncpg``.

- ``reindex`` now skips blob tables and quotes schema and table names, so it
  should work in more cases.

2020-03-27 0.18.0
=================

- ``timeit`` can now be used with PostgreSQL and Cockroach if using
  ``asyncpg``.

- ``insert-fake-data`` now uses ``uuid4`` automatically for columns of type
  ``text``. This was already the case pre CrateDB 4.0 for columns of type
  ``string``.

- ``run-spec`` now implicitly triggers an ``ANALYZE`` after the setup.

2019-08-20 0.17.0
=================

- Added a new ``reindex`` command which will go through all tables on a cluster
  which need to be re-indexed to be compatible with CrateDB major_version+1. It
  will one by one create a copy of a table and then remove the old table,
  replacing it with the copy.

- Added a new ``infile`` argument to the ``insert-json`` command. This should
  make certain scripting scenarios easier.

2019-07-01 0.16.0
=================

- Added type mapping for the ``timestamp with time zone`` and ``timestamp
  without time zone`` data types so that ``insert-fake-data`` can generate data
  for those columns by default.

- ``run-crate`` now supports arbitrary branch builds. E.g. ``run-crate
  branch:master``

- Removed the ``LineBuffer`` from ``run_crate`` and added support for callables
  instead.

2019-04-04 0.15.1
=================

- Fixed a compatibility issue with Python 3.6

- Adapted some queries and type mappings to be compatible with CrateDB 4.0

- Corrected the minimum CrateDB version that can be run with Java 11.

- Added a ``version`` arg to ``CrateNode`` which can be used to overrule
  the auto-detection.

2019-02-28 0.15.0
=================

- ``run-crate`` will now try to choose a different ``JAVA_HOME`` if the given
  version of ``CrateDB`` can't be run with the default ``JAVA_HOME``. This
  behavior can be disabled with ``--disable-java-magic``.
  The behavior of the ``CrateNode`` API is unchanged and by default won't try
  to change the ``JAVA_HOME``.

- ``run-crate`` will now avoid re-building branches from source if there aren't
  any new commits.

2019-02-14 0.14.2
=================

- ``run-crate <release_branch>`` will now make sure that the sources are
  updated to avoid stale builds.

2019-02-05 0.14.1
=================

- Made ``run-crate`` where the argument is a path to a repository forward
  compatible with upcoming build changes.

2018-10-08 0.14.0
=================

- SSL validation can no be disabled by including ``verify_ssl=False`` in the
  hosts URI.

- ``insert-fake-data`` now generates timestamps differently so it works with
  ``asyncpg``

- JSON output is no longer pretty printed by default. Use ``jq`` or ``python
  -mjson.tool`` to do so.

- Release branches can now be used as argument to ``run-crate``. (Something
  like ``run-crate 3.1``. This will result in a source checkout and the tarball
  will be built locally.)

- Improved the error message when connecting via ``HTTP`` and running into a
  ``401``.


2018-07-04 0.13.0
=================

- Added experimental postgres protocol support. It's available if the optional
  ``asyncpg`` dependency is installed.

- Bumped the ``aiohttp`` dependency for ``Python 3.7`` support.

- The ``load_data`` directive in spec files now can read ``gzipped`` files.

- Cached local tarballs are now checksummed to avoid re-using a stale tarball
  from cache.

- Samples and stdev are now included in the ``runtime_stats`` output if only 1
  sample is available.

2018-06-08 0.12.1
=================

- ``run-track`` now exists with an error code if any statement failed.

- The statements printed during ``run-spec`` are no longer trimmed.

- Version wildcards like ``2.3.x`` now work correctly for digits greater than
  9.

2018-05-24 0.12.0
=================

- Fixed an issue that caused ``insert-fake-data`` to fail with a ``TypeError``.

- spec files written in python can now use generators for statements or
  arguments. 

- Improved the fake data generation for ``insert-fake-data``. It now works for
  arrays and objects (although they'll simply be empty)

- Added a ``duration`` option to spec files and ``timeit``

- Added a ``sample-mode`` option to control how many samples will be kept for
  the results.

- Improved some error handling and error reporting

2018-02-04 0.11.1
=================

- The ``disk.watermark`` settings are no longer set by default by ``run-crate``
  in order to be compatible with CrateDB 3.0

- ``run-crate`` will now exit with a failure if process chaining is used and
  one of the chained processes failed.

- ``stop()`` on ``CrateNode`` now resets certain attributes correctly, so that
  ``start()`` doesn't fail with connection errors.

2017-11-05 0.11.0
=================

- Added a new default provider for columns of type ``BYTE``.

- Added a new default provider for columns of type ``GEO_SHAPE``, which
  provides a POLYGON WKT string.

- Dropped support for Python 3.5

- ``run-crate`` now correctly supports settings using unicode characters.

- ``run-crate`` will now remove old tarballs from the cache folder after a
  while.

- ``run-crate`` should now fail faster if an invalid setting is used.

- ``run-crate`` now supports arbitrary command chaining using ``-- @cmdname``
  If command chaining is used, ``run-crate`` will terminate after all commands
  have been run.

- ``run-crate`` should now work correctly if CrateDB is bound to a IPv6 address.

2017-09-12 0.10.2
=================

- ``insert-fake-data`` should no longer generate the same values using the
  ``uuid4`` provider. The amount of duplicate values generated using other
  providers should be reduced as well.

2017-08-04 0.10.1
=================

- ``run-crate`` now works again with ``latest-nightly``. It ran into a timeout
  as it couldn't parse the HTTP address from the log due to a format change.

2017-06-30 0.10.0
=================

- Added a new ``--fail-if`` argument to ``timeit`` and ``run-spec``.

- Added support for sub-command chaining using ``--``. This is especially
  useful if the first command is ``run-crate``.
  Together with ``--fail-if`` this can be used with ``git bisect`` to determine
  the first commit that introduced a performance regression.
  An example:

    cr8 run-crate /path/to/crate/src \
        -- timeit -s "select... " --hosts '{node.http_url}' --fail-if "{runtime_stats.mean} > 1.34"


``insert-fake-data``
--------------------

- Added a default provider for columns of type ``short``.


``run-crate``
-------------

- Pass ``LANG`` environment variable to ``crate`` subprocess.
  This fixes encoding issues when passing unicode characters as CrateDB setting
  values.

- It's now possible to launch SSL enabled nodes. Before ``run-crate`` would run
  into a timeout.

- The version identifier can now include ``x`` as wildcard. For example, use:
  ``run-crate 2.0.x`` to run the latest hotfix version in the ``2.0`` series.

- Added support for building and running crate from a source tree.

- Environment variables set using ``--env`` can now contain ``=`` signs.


2017-05-14 0.9.3
================

- ``insert-fake-data``: Increased the default value range for columns of type
  ``integer`` or ``long``.

- Updated ``aiohttp`` to version 2

- ``insert-fake-data``: The schema and table name is now quoted to allow using
  reserved keywords as schema or table name.

2017-02-11 0.9.2
================

- Values of type ``Decimal`` or ``datetime`` can now be serialized.
  This fixes an issue that could cause ``insert-fake-data`` to not work with
  schemas that contained columns of type ``double``.
  It also allows track files written in python to use ``Decimal`` or
  ``datetime`` objects as arguments.

- If python-argcomplete is installed and registered that should now be picked
  up to enable tab-completion in bash.

- Fixed an issue that caused warnings with newer ``aiohttp`` versions.

- Adapted ``run-crate`` to handle upcoming breaking changes. It's now able to
  launch tarballs of CrateDB ``1.1`` and ``1.2.`` snapshots.

2017-01-03 0.9.1
================

- Fixed an issue that caused failures on Windows

- ``timeit`` can now receive multi-line statements via stdin

2016-12-13 0.9.0
================

Breaking
--------

- Changed the default output format to ``text``. In addition, the values of
  ``--output-format`` were renamed from ``full`` and ``short`` to ``json`` and
  ``text``.

Miscellaneous improvements
--------------------------

- Added a ``--keep-data`` option to ``run-crate``. If this is set the data
  folder isn't removed if the process is stopped.

- The ``version`` argument of ``run-crate`` can now also be a fs path to a
  CrateDB tarball.

- Various error handling and ``Ctrl+c`` improvements.

- Added ``--logfile-info`` and ``--logfile-result`` options to ``run-spec`` and
  ``run-track``.


2016-11-12 0.8.1
================

- Fixed a regression that caused ``run-spec`` to save results into ``hosts``
  instead of ``result-hosts``.


2016-11-10 0.8.0
================

insert-fake-data improvements
-----------------------------

- Multiple cores are now utilized better for fake data generation.

- Adopted internal queries to be compatible with Crate versions > ``0.57``.

- ``insert-fake-data`` will now insert the accurate number of rows specified
  instead of rounding to the nearest bulk size.

Miscellaneous
-------------

- ``run-crate latest-stable`` now correctly launches the latest released stable
  version of Crate.
  It incorrectly retrieved the version of the latest Java client release.

- ``run-crate`` now outputs the postgres port if found in the logs.

- Added a ``--action`` argument to ``run-spec`` which can be used to only run a
  subset of a spec file.

- Extended the track-file format to allow re-using a setup across multiple spec
  files.

- Added a ``--version`` option.
  Best feature ever.

- Changed the ``--help`` output formatting so it's easier to read.


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
