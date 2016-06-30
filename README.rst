===
cr8
===

.. image:: https://travis-ci.org/mfussenegger/cr8.svg?branch=master
    :target: https://travis-ci.org/mfussenegger/cr8
    :alt: travis-ci

.. image:: https://img.shields.io/pypi/wheel/cr8.svg
    :target: https://pypi.python.org/pypi/cr8/
    :alt: Wheel

.. image:: https://img.shields.io/pypi/v/cr8.svg
   :target: https://pypi.python.org/pypi/cr8/
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/cr8.svg
   :target: https://pypi.python.org/pypi/cr8/
   :alt: Python Version

A collection of command line tools for `Crate <https://github.com/crate/crate>`_.
Most of these tools output JSON. In order to pretty-print the output or filter
it using `jq`_  is recommended.

Target audience are mostly developers of Crate and not users.

Install 💾
==========

Python >= 3.5.1 is required to use the command line tools.

Install them using `pip <https://pip.pypa.io/en/stable/>`_::

    pip install cr8

(Users of Python 3.5.0 might run into `this AssertionError
<http://bugs.python.org/issue25233>`_.)

Usage
=====

The main binary is called ``cr8`` which contains a couple of sub-commands.

Use ``cr8 -h`` or ``cr8 <subcommand> -h`` to get a more detailed usage
description.

The included sub-commands are described in more detail below:

Sub-commands
============

timeit 🕐
---------

A tool that can be used to measure the runtime of a given SQL statement on a
cluster::

    >>> echo "select name from sys.cluster" | cr8 timeit --hosts localhost:4200
    {
        "bulk_size": null,
        "concurrency": 1,
        "ended": ...
        "runtime_stats": {
            ...
        },
        "started": ...
        "statement": "select name from sys.cluster\n",
        "version_info": {
            "hash": "...",
            "number": "..."
        }
    }


insert-fake-data
----------------

A tool that can be used to fill a table with random data. The script will
generate the records using `faker <https://github.com/joke2k/faker>`_.

For example given the table as follows::

    create table demo (
        name string,
        country string
    );

The following command can be used to insert 1000 records::

    >>> cr8 insert-fake-data --hosts localhost:4200 --table demo --num-records 1000
    Found schema: 
    {
        "country": "string",
        "name": "string"
    }
    Using insert statement: 
    insert into demo ("country", "name") values (?, ?)
    Will make 1 requests with a bulk size of 1000
    Generating fake data and executing inserts
    <BLANKLINE>


It will automatically read the schema from the table and map the columns to
faker `providers
<http://fake-factory.readthedocs.org/en/latest/providers.html>`_ and insert the
give number of records.

(Currently only top-level columns are supported)

insert-json
-----------

``insert-json`` can be used to insert records from a JSON file::

    >>> cat tests/demo.json | cr8 insert-json --table demo --hosts localhost:4200
    Executing inserts: bulk_size=1000 concurrency=25
    {
        "max": ...,
        "mean": ...,
        "min": ...,
        "n": 1
    }

Or simply print the insert statement generated from a JSON string::

    >>> echo '{"name": "Arthur"}' | cr8 insert-json --table mytable
    ('insert into mytable ("name") values (?)', ['Arthur'])
    ...

insert-blob
-----------

A tool to upload a file into a blob table::

    >>> cr8 insert-blob --hosts localhost:4200 --table blobtable specs/sample.toml
    http://localhost:44200/_blobs/blobtable/433114a08ae9258c8a41ca74b57d0c8670bdece6

run-spec
--------

A tool to run benchmarks against a cluster and store the result in another
cluster. The benchmark itself is defined in a spec file which defines `setup`,
`benchmark` and `teardown` instructions.

The instructions itself are just SQL statements (or files containing SQL
statements).

In the `specs` folder is an example spec file.

Usage::

    >>> cr8 run-spec specs/sample.toml localhost:44200 -r localhost:44200
    Running setUp
    Running benchmark
    {
        "concurrency": 2,
        "iterations": 1000,
        "statement": "select count(*) from countries"
    }
    {
        "bulk_size": null,
        "concurrency": 2,
        "ended": ...,
        "runtime_stats": {...
        "started": ...
        "statement": "select count(*) from countries",
        "version_info": {
            "hash": ...
            "number": ...
        }
    }
    <BLANKLINE>
    Running tearDown
    <BLANKLINE>


`-r` is optional and can be used to save the benchmark result into a cluster.
The cluster must contain the table specified in `sql/benchmarks_table.sql`.

Writing spec files in python is also supported::

    >>> cr8 run-spec specs/sample.py localhost:44200
    Running setUp
    Running benchmark
    ...

run-crate
---------

Launch a Crate instance::

    > cr8 run-crate 0.55.0

This requires Java 8.


run-track
---------

A tool to run ``.toml`` track files.
A track is a matrix definition of node version, configurations and spec files.

For each version and configuration a Crate node will be launched and all specs
will be executed.

    >>> cr8 run-track tracks/sample.toml
    # Version:  latest-testing
    ## Starting Crate latest-testing, configuration: default.toml
    ### Running spec file:  sample.toml
    Running setUp
    Running benchmark
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
