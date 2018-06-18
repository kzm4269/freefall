#!/usr/bin/env python
import sys

from setuptools import setup, find_packages

if sys.version_info < (3, 5):
    sys.exit('Sorry, Python < 3.5 is not supported')

with open('README.md') as f:
    README = f.read()

with open('LICENSE') as f:
    LICENSE = f.read()

setup(
    name='freefall',
    version='3.0.2',
    description='Directory based simple downloader',
    long_description=README,
    author='kzm4269',
    author_email='4269kzm@gmail.com',
    url='https://github.com/kzm4269/freefall',
    license=LICENSE,
    packages=find_packages(exclude=['tests']),
    test_suite='tests',
    install_requires=[
        'filelock',
    ],
    extras_require={
        'test': [
            'psutil',
        ],
    },
)
