#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

try:
    with open('README.rst', 'r', encoding='utf-8') as f:
        readme = f.read()
except IOError:
    readme = ''


setup(
    name='cr8',
    author='Mathias Fußenegger',
    author_email='pip@zignar.net',
    url='https://github.com/mfussenegger/cr8',
    description='A collection of command line tools for crate devs',
    long_description=readme,
    entry_points={
        'console_scripts': [
            'cr8 = cr8.main:main',
        ]
    },
    packages=['cr8'],
    install_requires=[
        'crate>=0.16',
        'argh',
        'tqdm',
        'fake-factory',
        'aiohttp',
        'toml'
    ],
    extras_require={
        'uvloop': ['uvloop'],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm']
)
