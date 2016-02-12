====================
cr8 - Crate Devtools
====================

.. image:: https://travis-ci.org/mfussenegger/crate-devtools.svg?branch=master
    :target: https://travis-ci.org/mfussenegger/crate-devtools
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

A collection of small utility scripts that can make using / testing /
developing crate easier.

Install
=======

The scripts are written in python and require at least version 3.4.

Install using pip::

    pip install cr8

Usage
=====

The main binary is called ``cr8`` which contains a couple of sub-commands.

Use ``cr8 -h`` or ``cr8 <subcommand> -h`` to get a more detailed usage
description.

An example using ``cr8``::

    > echo '{"name": "Arthur"}' | cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])

The included sub-commands are described in more detail below:

Scripts/Sub-commands
====================

The included scripts are:

timeit.py
---------

A script that can be used to measure the runtime of a given SQL statement on a
cluster::

    > cr8 timeit "select * from rankings limit 10" mycratecluster.hostname:4200

json2insert.py
--------------

A script that generates an insert statement from a json string::

    > echo '{"name": "Arthur"}' | cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])

If a Crate host is also provided the statement will be executed on the cluster.


blobs.py
--------

A script to upload a file into a blob table::

    > cr8 upload crate.cluster:4200 blobtable /tmp/screenshot.png


bench.sh
--------

A wrapper script that combines timeit with json2insert to measure the runtime
of a query and insert the result into the `benchmarks` table which can be
created using the `sql/benchmarks_table.sql` file and crash::

    ./bench.sh "select * from rankings limit 100" mycratecluster.hostname:4200 mycratecluster.hostname:4200


Where the first hostname is used to benchmark the query and the
second hostname is used to store the results.

(this script also requires `jq <http://stedolan.github.io/jq/>`_ to be
installed)

perf_regressions.py
-------------------

A script which will re-run all queries recorded with the `bench.sh` script. It
will record the runtimes again and output the new runtimes::

    > cr8 find-perf-regressions \
            cluster.to.benchmark:4200 \
            cluster.with.log.table:4200

fill_table.py
-------------

A script that can be used to fill a table with random data.  The script
will generate the records using `faker
<https://github.com/joke2k/faker>`_.

For example given the table as follows::

    create table demo (
        name string,
        country string
    );

The following command can be used to insert 100k records::

    > cr8 fill-table localhost:4200 demo 100000

It will automatically read the schema from the table and map the
columns to faker providers and insert the give number of records.

(Currently only top-level string columns are supported)

Development
===========

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
