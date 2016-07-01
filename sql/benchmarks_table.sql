create table if not exists benchmarks (
    version_info object (strict) as (
        number string,
        hash string
    ),
    statement string,
    started timestamp,
    ended timestamp,
    concurrency int,
    bulk_size int,
    runtime_stats object (strict) as (
        avg double,
        min double,
        max double,
        mean double,
        median double,
        percentile object as (
            "50" double,
            "75" double,
            "90" double,
            "99" double,
            "99_9" double
        ),
        n integer,
        variance double,
        stdev double,
        samples array(double)
    )
) clustered into 8 shards with (number_of_replicas = '1-3', column_policy='strict');
