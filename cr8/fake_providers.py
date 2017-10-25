#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
from faker.providers import BaseProvider
from multiprocessing import Manager


# Not implemented as Provider because with providers it's not possible to have
# 1 instance per column. So either there would be one shared Counter accross
# multiple auto_inc columns or there could be duplicate values within one column


class Counter:
    def __init__(self, value, lock):
        self.value = value
        self.lock = lock

    def __call__(self):
        val = self.value
        with self.lock:
            val.value += 1
            return val.value


def auto_inc(fake):
    manager = Manager()
    return Counter(manager.Value('i', 0), manager.Lock())


class GeoSpatialProvider(BaseProvider):
    """
    A Faker provider for geospatial data types, such as GEO_POINT.

    >>> from faker import Faker
    >>> fake = Faker()
    >>> fake.add_provider(GeoSpatialProvider)
    >>> hasattr(fake, 'geo_point')
    True

    >>> loc = fake.geo_point()
    >>> -180 <= loc[0] <= 180
    True
    >>> -90 <= loc[1] <= 90
    True

    >>> loc = fake.geo_point(0.0, 180.0, 0.0, 90.0)
    >>> 0 <= loc[0] <= 180
    True
    >>> 0 <= loc[1] <= 90
    True
    """

    EARTH_RADIUS = 6371.0  # in km

    def geo_point(self,
                  lon_min=-180.0, lon_max=180.0,
                  lat_min=-90.0, lat_max=90.0):
        assert isinstance(lon_min, float)
        assert isinstance(lon_max, float)
        assert isinstance(lat_min, float)
        assert isinstance(lat_max, float)

        assert lon_min >= -180.0
        assert lon_max <= 180.0
        assert lat_min >= -90.0
        assert lat_max <= 90.0

        # longitude: -180 .. 0 .. +180 (E-W)
        # latitude: -90 .. 0 .. +90 (S-N)
        u = self.generator.random.uniform
        return [
            u(lon_min, lon_max),
            u(lat_min, lat_max)
        ]

    def geo_shape(self, sides=5, center=None, distance=None):
        """
        Return a WKT string for a POLYGON with given amount of sides.
        The polygon is defined by its center (random point if not provided) and
        the distance (random distance if not provided; in km) of the points to
        its center.
        """
        assert isinstance(sides, int)

        if center is None:
            center = self.geo_point()
        else:
            assert -180.0 <= center[0] <= 180.0
            assert -90.0 <= center[1] <= 90.0

        if distance is None:
            distance = self.random_int(100, 1000)
        else:
            # 6371 => earth radius in km
            # assert that shape radius is maximum half of earth's circumference
            assert isinstance(distance, int)
            assert distance <= self.EARTH_RADIUS * math.pi

        d_arc = distance * 180.0 / self.EARTH_RADIUS / math.pi

        points = []
        angles = self.random_sample(range(360), sides)
        angles.sort()
        for a in angles:
            rad = a * math.pi / 180.0
            points.append(
                [center[0] + d_arc * math.sin(rad),
                 center[1] + d_arc * math.cos(rad)]
            )
        # close polygon
        points.append(points[0])

        path = ', '.join([' '.join(p) for p in ([str(lon), str(lat)] for lon, lat in points)])
        return f'POLYGON (( {path} ))'
