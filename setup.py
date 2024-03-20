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
    long_description_content_type='text/x-rst',
    entry_points={
        'console_scripts': [
            'cr8 = cr8.__main__:main',
        ]
    },
    packages=['cr8'],
    install_requires=[
        'argh',
        'tqdm',
        'Faker>=4.0,<5.0',
        'aiohttp>=3.3,<4',
        'toml;python_version<"3.11"',
        'asyncpg'
    ],
    extras_require={
        'extra': ['uvloop', 'pysimdjson']
    },
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm']
)
