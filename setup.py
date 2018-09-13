#!/usr/bin/env python

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
            'cr8 = cr8.__main__:main',
        ]
    },
    packages=['cr8'],
    install_requires=[
        'crate>=0.16',
        'argh',
        'tqdm',
        'Faker==0.9.1',
        'aiohttp>=3.3,<4',
        'toml'
    ],
    extras_require={
        'uvloop': ['uvloop'],
        'asyncpg': ['asyncpg']
    },
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm']
)
