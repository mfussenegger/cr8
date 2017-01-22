import statistics
from functools import partial

from cr8.engine import Runner


def _get_median(hosts, statement, warmup, iterations):
    with Runner(hosts, concurrency=1) as r:
        r.warmup(statement, warmup)
        stats = r.run(statement, iterations)
        return stats.stats.get()['median']


def _perc_diff(a, b):
    return (abs(a - b) / ((a + b) / 2.0)) * 100.0


def _within_perc(values, perc):
    mean = statistics.harmonic_mean(values)
    print('Values: ', values)
    print('Harmonic mean: ', mean)
    diffs = (_perc_diff(mean, i) for i in values)
    diffs = list(diffs)
    print([f'{d:.3f}' for d in diffs])
    return all(d < perc for d in diffs)


def calibrate(statement,
              hosts='localhost:4200',
              start_warmup=1,
              start_iterations=10,
              match_size=10,
              within_perc=1):
    warmup = start_warmup
    iterations = start_iterations
    measurements = []
    _run = partial(_get_median, hosts, statement)
    for i in range(match_size):
        measurements.append(_run(warmup, iterations))
    while not _within_perc(measurements, perc=within_perc):
        warmup *= 2
        iterations *= 2
        measurements.clear()
        for i in range(match_size):
            measurements.append(_run(warmup, iterations))
    print('warmup: ', warmup)
    print('iterations: ', iterations)
