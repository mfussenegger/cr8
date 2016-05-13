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

Python >= 3.5 is required to use the command line tools.

Install them using `pip <https://pip.pypa.io/en/stable/>`_::

    pip install cr8

Usage 🎠
========

The main binary is called ``cr8`` which contains a couple of sub-commands.

Use ``cr8 -h`` or ``cr8 <subcommand> -h`` to get a more detailed usage
description.

An example using ``cr8``::

    > echo '{"name": "Arthur"}' | cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])

The included sub-commands are described in more detail below:

Sub-commands
============

timeit
------

A tool that can be used to measure the runtime of a given SQL statement on a
cluster::

    > cr8 timeit cluster.hostname:4200 --stmt "select name from sys.cluster"

fill_table
----------

A tool that can be used to fill a table with random data. The script will
generate the records using `faker <https://github.com/joke2k/faker>`_.

For example given the table as follows::

    create table demo (
        name string,
        country string
    );

The following command can be used to insert 100k records::

    > cr8 fill-table localhost:4200 demo 100000

It will automatically read the schema from the table and map the columns to
faker `providers
<http://fake-factory.readthedocs.org/en/latest/providers.html>`_ and insert the
give number of records.

(Currently only top-level string columns are supported)

json2insert
-----------

json2insert generates an insert statement from a JSON string::

    > echo '{"name": "Arthur"}' | cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])

If a Crate host is provided the insert statement will be executed as well.

blobs
------

A tool to upload a file into a blob table::

    > cr8 upload crate.cluster:4200 blobtable /tmp/screenshot.png

bench.sh
--------

A wrapper script that combines timeit with json2insert to measure the runtime
of a query and insert the result into the `benchmarks` table which can be
created using the `sql/benchmarks_table.sql` file and crash::

    ./bench.sh "select * from rankings limit 100" mycratecluster.hostname:4200 mycratecluster.hostname:4200


Where the first hostname is used to benchmark the query and the
second hostname is used to store the results.

(this script also requires `jq`_ to be installed)

perf_regressions
----------------

A tool which will re-run all queries recorded with the `bench.sh` script. It
will record the runtimes again and output the new runtimes::

    > cr8 find-perf-regressions \
            cluster.to.benchmark:4200 \
            cluster.with.log.table:4200

Development 😕
==============

Tests are run using ``python setup.py test``.

To get a sandboxed environment with all dependencies installed one can either
use ``venv`` or ``buildout``:

venv
----

Create a new virtualenv using ``venv`` and active it::

    python -m venv .venv
    source .venv/bin/activate

Install the ``cr8`` package using pip::

    python -m pip install -e .

Run ``cr8``::

    cr8 -h

.. _jq: https://stedolan.github.io/jq/
