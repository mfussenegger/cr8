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
    url='https://github.com/mfussenegger/crate-devtools',
    description='A collection of utility scripts to work with testing and developing crate',
    long_description=readme,
    entry_points={
        'console_scripts': [
            'cr8 = cr8.main:main',
        ]
    },
    packages=['cr8'],
    install_requires=[
        'crate',
        'argh',
        'requests',
        'tqdm',
        'fake-factory'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm']
)
