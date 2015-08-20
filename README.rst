==============
Crate Devtools
==============

A collection of small utility scripts that can make using / testing /
developing crate easier.

Each script can be called with `-h` or `--help` to get a more detailed usage
description.

The included scripts are:

timeit.py
---------

A script that can be used to measure the runtime of a given SQL statement on a
cluster::

    > bin/cr8 timeit "select * from rankings limit 10" mycratecluster.hostname:4200

json2insert.py
--------------

A script that generates an insert statement from a json string::

    > echo '{"name": "Arthur"}' | bin/cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])


blobs.py
--------

A script to upload a file into a blob table::

    > bin/cr8 upload crate.cluster:4200 blobtable /tmp/screenshot.png


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

    > bin/cr8 find-perf-regressions \
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

    > bin/cr8 fill-table localhost:4200 demo 100000
    
It will automatically read the schema from the table and map the
columns to faker providers and insert the give number of records.

(Currently only top-level string columns are supported)

Installation / Setup
====================

The scripts are written in python and require at least version 3.4.
Use buildout to create a sandboxed environment with all dependencies installed:

Bootstrap buildout::

    /path/to/clean/python bootstrap.py

Run buildout::

    bin/buildout -N

After that you can access the tools via ``bin/cr8`` which is a central entry
point for all scripts.


Using ``bin/cr8``::

    > echo '{"name": "Arthur"}' | bin/cr8 json2insert mytable
    ('insert into mytable (name) values (?)', ['Arthur'])
