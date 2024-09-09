from itertools import count
from cr8.bench_spec import Spec, Instructions


def queries():
    for i in range(5):
        yield {
            'statement': 'insert into t (x) values (?)',
            'args': [i]
        }
    c = count(100)
    yield {
        'statement': 'insert into t (x) values (?)',
        'bulk_args': lambda: [[next(c)] for i in range(10)]
    }


# Spec and Instructions are injected by the spec runner
spec = Spec(
    setup=Instructions(statements=["create table t (x int)"]),
    teardown=Instructions(statements=["drop table t"]),
    queries=queries(),
    session_settings={'application_name': 'my_app', 'timezone': 'UTC'}
)