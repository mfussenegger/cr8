import statistics
import random


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

    def __init__(self, size):
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


class Stats:
    plevels = [50, 75, 90, 95, 99, 99.9]

    def __init__(self, size=DEFAULT_NUM_SAMPLES):
        self.reservoir = UniformReservoir(size=size or DEFAULT_NUM_SAMPLES)

    def measure(self, value):
        self.reservoir.add(value)

    def get(self):
        values = sorted(self.reservoir.values)
        count = len(values)
        # instead of failing return empty / subset so that json2insert & co
        # don't fail
        if count == 0:
            return dict(n=0)
        elif count == 1:
            return dict(min=values[0], max=values[0], mean=values[0], n=count)
        percentiles = [percentile(values, p) for p in self.plevels]
        return dict(
            min=values[0],
            max=values[-1],
            mean=statistics.mean(values),
            median=statistics.median(values),
            variance=statistics.variance(values),
            stdev=statistics.stdev(values),
            # replace . with _ so that the output can be inserted into crate
            # crate doesn't allow dots in column names
            percentile={str(i[0]).replace('.', '_'): i[1] for i in
                        zip(self.plevels, percentiles)},
            n=count
        )
