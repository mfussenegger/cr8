def queries():
    for i in range(5):
        yield {
            'statement': 'insert into t (x) values (?)',
            'args': [i]
        }


# Spec and Instructions are injected by the spec runner
spec = Spec(
    setup=Instructions(statements=["create table t (x int)"]),
    teardown=Instructions(statements=["drop table t"]),
    queries=queries(),
)
