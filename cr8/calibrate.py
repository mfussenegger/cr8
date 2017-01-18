import itertools
from functools import partial

from cr8.run_crate import CrateNode, get_crate
from cr8.engine import Runner


def _get_mean(crate_dir, statement, warmup, iterations):
    with CrateNode(crate_dir=crate_dir) as n:
        n.start()
        with Runner(n.http_url, concurrency=1) as r:
            r.warmup(statement, warmup)
            stats = r.run(statement, iterations)
            return stats.stats.get()['mean']


def _pairwise(iterable):
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


def _perc_diff(a, b):
    return (abs(a - b) / ((a + b) / 2.0)) * 100.0


def _within_perc(values, perc):
    diffs = (_perc_diff(a, b) for a, b in _pairwise(values))
    diffs = list(diffs)
    print(diffs)
    return all(d < perc for d in diffs)


def calibrate(statement,
              version='latest-stable',
              start_warmup=1,
              start_iterations=10,
              match_size=10,
              within_perc=1):
    crate_dir = get_crate(version)
    warmup = start_warmup
    iterations = start_iterations
    means = []
    _run = partial(_get_mean, crate_dir, statement)
    for i in range(match_size):
        means.append(_run(warmup, iterations))
    while not _within_perc(means, perc=within_perc):
        warmup *= 2
        iterations *= 2
        means.clear()
        for i in range(match_size):
            means.append(_run(warmup, iterations))
    print('warmup: ', warmup)
    print('iterations: ', iterations)
