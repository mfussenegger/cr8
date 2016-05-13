create table if not exists benchmarks (
    version_info object (strict) as (
        number string,
        hash string
    ),
    statement string,
    started timestamp,
    ended timestamp,
    repeats int,
    server_runtimes array(double),
    client_runtimes array(double),
    runtime_stats object (strict) as (
        avg double,
        min double,
        max double,
        pvariance double,
        stdev double
    )
) clustered into 8 shards with (column_policy='strict', number_of_replicas='1-3')
