
import statistics
import random


def percentile(sorted_values, p):
    """ calculate the percentile using the nearest rank method

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
    """ Reservoir Sampling Algorithm R by Jeffrey Vitter
    https://en.wikipedia.org/wiki/Reservoir_sampling#Algorithm_R
    """

    def __init__(self, size):
        self.size = size
        self.count = 0
        self.values = [0] * size

    def add(self, value):
        count = self.count
        if count < self.size:
            self.values[count] = value
        else:
            k = random.randint(0, self.count)
            if k < self.size:
                self.values[k] = value
        self.count = count + 1


class Stats:
    plevels = [50, 75, 90, 95, 99, 99.9]

    def __init__(self, size=1000):
        self.reservoir = UniformReservoir(size=size)

    def measure(self, value):
        self.reservoir.add(value)

    def get(self):
        values = sorted(self.reservoir.values)
        percentiles = [percentile(values, p) for p in self.plevels]
        return dict(
            min=values[0] if values else 0,
            max=values[-1] if values else 0,
            mean=statistics.mean(values),
            median=statistics.median(values),
            variance=statistics.variance(values),
            stdev=statistics.stdev(values),
            # replace . with _ so that the output can be inserted into crate
            # crate doesn't allow dots in column names
            percentile={str(i[0]).replace('.', '_'): i[1] for i in
                        zip(self.plevels, percentiles)},
            n=len(values)
        )
