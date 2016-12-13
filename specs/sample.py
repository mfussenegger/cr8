
from itertools import count


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
)
