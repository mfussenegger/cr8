#!/usr/bin/env python
# -*- coding: utf-8 -*-

from faker.generator import random
from faker.providers import BaseProvider


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
        # longitude: -180 .. 0 .. +180 (E-W)
        # latitude: -90 .. 0 .. +90 (S-N)
        return [
            random.uniform(lon_min, lon_max),
            random.uniform(lat_min, lat_max)
        ]

