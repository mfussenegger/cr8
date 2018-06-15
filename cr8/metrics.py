import statistics
import random
import math
from functools import partial


DEFAULT_NUM_SAMPLES = 1000


def percentile(sorted_values, p):
    """Calculate the percentile using the nearest rank method.

    >>> percentile([15, 20, 35, 40, 50], 50)
    35

    >>> percentile([15, 20, 35, 40, 50], 40)
    20

    >>> percentile([], 90)
    Traceback (most recent call last):
        ...
    ValueError: Too few data points (0) for 90th percentile
    """
    size = len(sorted_values)
    idx = (p / 100.0) * size - 0.5
    if idx < 0 or idx > size:
        raise ValueError('Too few data points ({}) for {}th percentile'.format(size, p))
    return sorted_values[int(idx)]


class UniformReservoir:
    """Reservoir Sampling Algorithm R by Jeffrey Vitter.

    See https://en.wikipedia.org/wiki/Reservoir_sampling#Algorithm_R
    """

    def __init__(self, size=DEFAULT_NUM_SAMPLES):
        self.size = size
        self.count = 0
        self.values = []

    def add(self, value):
        count = self.count
        if count < self.size:
            self.values.append(value)
        else:
            k = random.randint(0, self.count)
            if k < self.size:
                self.values[k] = value
        self.count = count + 1


class All:
    """Sampler that keeps all values"""

    def __init__(self):
        self.values = []

    def add(self, value):
        self.values.append(value)

    @property
    def count(self):
        return len(self.values)


def get_sampler(sample_mode: str):
    """Return a sampler constructor

    >>> get_sampler('all')
    <class 'cr8.metrics.All'>

    >>> get_sampler('reservoir')
    <class 'cr8.metrics.UniformReservoir'>

    >>> get_sampler('reservoir:100')
    functools.partial(<class 'cr8.metrics.UniformReservoir'>, size=100)
    """
    if sample_mode == 'all':
        return All
    mode = sample_mode.split(':')
    if mode[0] == 'reservoir':
        if len(mode) == 2:
            return partial(UniformReservoir, size=int(mode[1]))
        else:
            return UniformReservoir
    raise TypeError(f'Invalid sample_mode: {sample_mode}')


_z_map = {
    80: 1.282,
    85: 1.440,
    90: 1.645,
    95: 1.960,
    99: 2.576,
    99.5: 2.807,
    99.9: 3.291,
}


def error_margin(confidence_level, stdev, sample_size):
    return _z_map[confidence_level] * (stdev / math.sqrt(sample_size))


class Stats:
    plevels = [50, 75, 90, 95, 99, 99.9]

    def __init__(self, sampler=None):
        self.sampler = sampler() if sampler else UniformReservoir()

    def measure(self, value):
        self.sampler.add(value)

    def get(self):
        values = sorted(self.sampler.values)
        count = len(values)
        # instead of failing return empty / subset so that json2insert & co
        # don't fail
        if count == 0:
            return dict(n=0)
        elif count == 1:
            return dict(
                min=values[0],
                max=values[0],
                mean=values[0],
                n=count,
                stdev=0,
                samples=self.sampler.values
            )
        percentiles = [percentile(values, p) for p in self.plevels]
        min_ = values[0]
        max_ = values[-1]
        stdev = statistics.stdev(values)
        return dict(
            min=min_,
            max=max_,
            mean=statistics.mean(values),
            median=statistics.median(values),
            variance=statistics.variance(values),
            error_margin=error_margin(95, stdev, self.sampler.count),
            stdev=stdev,
            # replace . with _ so that the output can be inserted into crate
            # crate doesn't allow dots in column names
            percentile={str(i[0]).replace('.', '_'): i[1] for i in
                        zip(self.plevels, percentiles)},
            n=self.sampler.count,
            samples=self.sampler.values
        )
