#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
from faker.providers import BaseProvider
from multiprocessing import Manager

EARTH_RADIUS = 6371  # earth radius in km

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


def _dest_point(point, distance, bearing, radius):
    # calculation taken from
    # https://cdn.rawgit.com/chrisveness/geodesy/v1.1.2/latlon-spherical.js
    # https://www.movable-type.co.uk/scripts/latlong.html

    δ = distance / radius  # angular distance in rad
    θ = math.radians(bearing)

    φ1 = math.radians(point[1])
    λ1 = math.radians(point[0])

    sinφ1 = math.sin(φ1)
    cosφ1 = math.cos(φ1)
    sinδ = math.sin(δ)
    cosδ = math.cos(δ)
    sinθ = math.sin(θ)
    cosθ = math.cos(θ)

    sinφ2 = sinφ1 * cosδ + cosφ1 * sinδ * cosθ
    φ2 = math.asin(sinφ2)
    y = sinθ * sinδ * cosφ1
    x = cosδ - sinφ1 * sinφ2
    λ2 = λ1 + math.atan2(y, x)

    return [
        (math.degrees(λ2) + 540) % 360 - 180,  # normalise to −180..+180°
        math.degrees(φ2)
    ]


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

        if distance is None:
            distance = self.random_int(100, 1000)
        else:
            # 6371 => earth radius in km
            # assert that shape radius is maximum half of earth's circumference
            assert isinstance(distance, int)
            assert distance <= EARTH_RADIUS * math.pi, \
                'distance must not be greater than half of earth\'s circumference'

        if center is None:
            # required minimal spherical distance from north/southpole
            dp = distance * 180.0 / EARTH_RADIUS / math.pi
            center = self.geo_point(lat_min=-90.0 + dp, lat_max=90.0 - dp)
        else:
            assert -180.0 <= center[0] <= 180.0, 'Longitude out of bounds'
            assert -90.0 <= center[1] <= 90.0, 'Latitude out of bounds'

        angles = list(self.random_sample(range(360), sides))
        angles.sort()
        points = [_dest_point(center, distance, bearing, EARTH_RADIUS) for bearing in angles]
        # close polygon
        points.append(points[0])

        path = ', '.join([' '.join(p) for p in ([str(lon), str(lat)] for lon, lat in points)])
        return f'POLYGON (( {path} ))'
