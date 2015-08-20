#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


setup(
    name='cr8',
    version='0.1',
    entry_points={
        'console_scripts': [
            'cr8 = cr8.main:main',
        ]
    },
    packages=['cr8'],
    install_requires=['crate', 'argh', 'requests', 'tqdm', 'fake-factory']
)
