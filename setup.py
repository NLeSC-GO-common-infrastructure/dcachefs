#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
version = {}
# To update the package version number, edit dcachefs/__version__.py
with open(os.path.join(here, 'dcachefs', '__version__.py')) as f:
    exec(f.read(), version)

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.read().split()

setup(
    name='dcachefs',
    version=version['__version__'],
    description="Python filesystem interface for dCache",
    long_description=readme + '\n\n',
    author="Francesco Nattino",
    author_email='f.nattino@esciencecenter.nl',
    url='https://github.com/NLeSC-GO-common-infrastructure/dcachefs',
    packages=[
        'dcachefs',
    ],
    include_package_data=True,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='dcachefs',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    test_suite='tests',
    install_requires=requirements,
    setup_requires=[
        # dependency for `python setup.py test`
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
        'pytest-cov',
        'pycodestyle',
        'webdavclient3',
    ]
)
