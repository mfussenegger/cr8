create table if not exists benchmarks (
    version_info object (strict) as (
        number string,
        hash string
    ),
    statement string,
    started timestamp,
    ended timestamp,
    repeats int,
    runtime_stats object (strict) as (
        avg double,
        min double,
        max double
    )
) clustered into 8 shards with (number_of_replicas = '1-3')
