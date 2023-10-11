===
cr8
===

.. image:: https://github.com/mfussenegger/cr8/workflows/test%20&%20publish/badge.svg
    :target: https://github.com/mfussenegger/cr8/actions
    :alt: github actions

.. image:: https://img.shields.io/pypi/wheel/cr8.svg
    :target: https://pypi.python.org/pypi/cr8/
    :alt: Wheel

.. image:: https://img.shields.io/pypi/v/cr8.svg
   :target: https://pypi.python.org/pypi/cr8/
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/cr8.svg
   :target: https://pypi.python.org/pypi/cr8/
   :alt: Python Version

A collection of command line tools for `Crate
<https://github.com/crate/crate>`_ developers (and maybe users as well).

TOC
====

- `Why cr8? 🤔`_
- `Install 💾`_
- `Usage`_
- `Sub-commands`_
    - `timeit 🕐`_
    - `insert-fake-data`_
    - `insert-json`_
    - `insert-from-sql`_
    - `run-spec`_
    - `run-crate`_
        - `Script reproduction`_
        - `Find regressions`_
        - `Profiling`_
        - `Creating a CrateDB cluster`_
    - `run-track`_
    - `reindex`_
- `Protocols`_
- `Development ☢`_


Why cr8? 🤔
===========

1. To quickly produce sample data. Often if someone reports an issue sample
   data is required to be able to reproduce it.
   `insert-fake-data`_ and `insert-json`_ address this problem.

2. To benchmark queries & compare runtime across Crate versions.  `timeit 🕐`_,
   `run-spec`_ and `run-track`_ can be used to get runtime statistics of
   queries.
   These tools focus on response latencies. Being able to benchmark throughput
   is NOT a goal of cr8. Similarly, being able to simulate real-world use
   cases is also NOT a goal of cr8.



.. note::

    Although most commands output text by default. Most take a ``--output-fmt
    json`` argument to output JSON.
    This is useful if used together with `jq`_ to post-process the output


Install 💾
==========

Python >= 3.7 is required to use the command line tools.

Install them using `pip <https://pip.pypa.io/en/stable/>`_::

    python3.7 -m pip install --user cr8

This will install ``cr8`` into ``~/.local/bin``. Either use
``~/.local/bin/cr8`` to launch it or add ``~/.local/bin`` to your ``$PATH``
environment variable.


An alternative is to download a single ``zipapp`` file from the `releases page
<https://github.com/mfussenegger/cr8/releases>`_.


Usage
=====

The main binary is called ``cr8`` which contains a couple of sub-commands.

Use ``cr8 -h`` or ``cr8 <subcommand> -h`` to get a more detailed usage
description.

The included sub-commands are described in more detail below:

**Tip**:

Any `<subcommand>` with ``--hosts`` argument supports password authentication
like this::

    cr8 <subcommand> --hosts http://username:password@localhost:4200 <remaining args>


Sub-commands
============

timeit 🕐
---------

A tool that can be used to measure the runtime of a given SQL statement on a
cluster::

    >>> echo "select name from sys.cluster" | cr8 timeit --hosts localhost:4200
    Runtime (in ms):
        mean:    ... ± ...
        min/max: ... → ...
    Percentile:
        50:   ... ± ... (stdev)
        95:   ...
        99.9: ...


insert-fake-data
----------------

A tool that can be used to fill a table with random data. The script will
generate the records using `faker <https://github.com/joke2k/faker>`_.

For example given the table as follows::

    create table x.demo (
        id int,
        name text,
        country text
    );

The following command can be used to insert 1000 records::

    >>> cr8 insert-fake-data --hosts localhost:4200 --table x.demo --num-records 200
    Found schema:
    {
        "country": "text",
        "id": "integer",
        "name": "text"
    }
    Using insert statement:
    insert into "x"."demo" ("id", "name", "country") values ($1, $2, $3)
    Will make 1 requests with a bulk size of 200
    Generating fake data and executing inserts
    <BLANKLINE>

It will automatically read the schema from the table and map the columns to
faker `providers
<https://faker.readthedocs.io/en/latest/providers.html>`_ and insert the
give number of records.

(Currently only top-level columns are supported)

An alternative way to generate random records is `mkjson
<https://github.com/mfussenegger/mkjson>`_ which can be used together with
``insert-json``.

insert-json
-----------

``insert-json`` can be used to insert records from a JSON file::

    >>> cat tests/demo.json | cr8 insert-json --table x.demo --hosts localhost:4200
    Executing inserts: bulk_size=1000 concurrency=25
    Runtime (in ms):
        mean:    ... ± 0.000

Or simply print the insert statement generated from a JSON string::

    >>> echo '{"name": "Arthur"}' | cr8 insert-json --table mytable
    ('insert into mytable ("name") values ($1)', ['Arthur'])
    ...

insert-from-sql
---------------

Copies data from one CrateDB cluster or PostgreSQL server to another.

::

    >>> cr8 insert-from-sql \
    ...   --src-uri "postgresql://crate@localhost:5432/doc" \
    ...   --query "SELECT name FROM x.demo" \
    ...   --hosts localhost:4200 \
    ...   --table y.demo \
    INSERT INTO y.demo ("name") VALUES ($1)
    Runtime (in ms):
    ...


The ``concurrency`` option of the command only affects the number of concurrent
write operations that will be made. There will always be a single read
operation, so copy operations may be bound by the read performance.


run-spec
--------

A tool to run benchmarks against a cluster and store the result in another
cluster. The benchmark itself is defined in a spec file which defines `setup`,
`benchmark` and `teardown` instructions.

The instructions itself are just SQL statements (or files containing SQL
statements).

In the `specs` folder is an example spec file.

Usage::

    >>> cr8 run-spec specs/sample.toml localhost:4200 -r localhost:4200
    # Running setUp
    # Running benchmark
    <BLANKLINE>
    ## Running Query:
       Name: count countries
       Statement: select count(*) from countries
       Concurrency: 2
       Duration: 1
    Runtime (in ms):
        mean:    ... ± ...
        min/max: ... → ...
    Percentile:
        50:   ... ± ... (stdev)
        95:   ...
        99.9: ...
    ...
    ## Skipping (Version ...
       Statement: ...
    # Running tearDown
    <BLANKLINE>

`-r` is optional and can be used to save the benchmark result into a cluster.
A table named `benchmarks` will be created if it doesn't exist.

Writing spec files in python is also supported::

    >>> cr8 run-spec specs/sample.py localhost:4200
    # Running setUp
    # Running benchmark
    ...

run-crate
---------

Launch a Crate instance::

    > cr8 run-crate 0.55.0

This requires Java 8.

``run-crate`` supports chaining of additional commands using ``--``. Under the
context of ``run-crate`` any host urls can be formatted using the
``{node.http_url}`` format string::

    >>> cr8 run-crate latest-stable -- timeit -s "select 1" --hosts '{node.http_url}'
     # run-crate
    ===========
    <BLANKLINE>
    ...
    Starting Crate process
    CrateDB launching:
        PID: ...
        Logs: ...
        Data: ...
    <BLANKLINE>
    ...
    Cluster ready to process requests
    <BLANKLINE>
    <BLANKLINE>
    # timeit
    ========
    <BLANKLINE>
    <BLANKLINE>
    <BLANKLINE>
    <BLANKLINE>

In the above example ``timeit`` is a ``cr8`` specific sub-command. But it's
also possible to use arbitrary commands by prefixing them with ``@``::

    cr8 run-crate latest-nightly -- @http '{node.http_url}'


Script reproduction
~~~~~~~~~~~~~~~~~~~

One common use of this feature is to quickly reproduce bug reports::

    cr8 run-crate latest-nightly -- @crash --hosts {node.http_url} <<EOF
        create table mytable (x int);
        insert into mytable (x) values (1);
        refresh mytable;
        ...
    EOF


Find regressions
~~~~~~~~~~~~~~~~

Another use case is to use ``run-crate`` in combination with ``run-spec`` and
``git bisect``::

    git bisect run cr8 run-crate path/to/crate/src \
        -- run-spec path/to/spec.toml '{node.http_url}' --fail-if '{runtime_stats.mean} > 15'

This could also be combined with `timeout
<https://www.gnu.org/software/coreutils/manual/html_node/timeout-invocation.html#timeout-invocation>`_.


Profiling
~~~~~~~~~

This can also be used in combination with the Java flight recorder to do
profiling::

    cr8 run-crate latest-nightly \
        -e CRATE_HEAP_SIZE=4g \
        -e CRATE_JAVA_OPTS="-Dcrate.signal_handler.disabled=true -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UnlockCommercialFeatures -XX:+FlightRecorder" \
        -s discovery.type=single-node \
        -- run-spec path/to/specs/example.toml {node.http_url} --action setup \
        -- @jcmd {node.process.pid} JFR.start duration=60s filename=myrecording.jfr \
        -- run-spec path/to/specs/example.toml {node.http_url} --action queries \
        -- @jcmd {node.process.pid} JFR.stop


Creating a CrateDB cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~

``cr8`` doesn't contain a dedicated command to spawn a CrateDB cluster. But you
can run ``cr8 run-crate <version> -s cluster.name=<name>`` to launch multiple
nodes. If the cluster name matches, it will form a cluster.


run-track
---------

A tool to run ``.toml`` track files.
A track is a matrix definition of node version, configurations and spec files.

For each version and configuration a Crate node will be launched and all specs
will be executed::

    >>> cr8 run-track tracks/sample.toml
    # Version:  latest-testing
    ## Starting Crate latest-testing, configuration: default.toml
    ### Running spec file:  sample.toml
    # Running setUp
    # Running benchmark
    ...


reindex
-------

A command to re-index all tables on a cluster which have been created in the
previous major versions. So if you're running a 3.x CrateDB cluster, all tables
from 2.x would be re-created::

   >>> cr8 reindex --help
   usage: cr8 reindex [-h] --hosts HOSTS
   ...


Protocols
=========

``cr8`` supports using ``HTTP`` or the ``postgres`` protocol.

Note that using the postgres protocol will cause ``cr8`` to measure the
round-trip time instead of the service time. So measurements will be different.

To use the ``postgres`` protocol, the ``asyncpg`` scheme must be used inside hosts URIs:

::


    >>> echo "select 1" | cr8 timeit --hosts asyncpg://localhost:5432
    Runtime (in ms):
    ...


Development ☢
==============

To get a sandboxed environment with all dependencies installed use ``venv``::

    python -m venv .venv
    source .venv/bin/activate

Install the ``cr8`` package using pip::

    python -m pip install -e .

Run ``cr8``::

    cr8 -h

Tests are run with ``python -m unittest``

.. _jq: https://stedolan.github.io/jq/
