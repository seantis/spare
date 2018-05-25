# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

name = 'spare'
description = 'One S3 backup, encrypted on the fly.'
version = '0.2.2'


def get_long_description():
    readme = open('README.rst').read()
    history = open('HISTORY.rst').read()

    # cut the part before the description to avoid repetition on pypi
    readme = readme[readme.index(description) + len(description):]

    return '\n'.join((readme, history))


setup(
    name=name,
    version=version,
    description=description,
    long_description=get_long_description(),
    url='http://github.com/seantis/spare',
    author='Seantis GmbH',
    author_email='info@seantis.ch',
    license='MIT',
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=name.split('.')[:-1],
    include_package_data=True,
    zip_safe=False,
    platforms='any',

    # we require 3.6.2 (not just 3.6.*), due to this bug:
    # https://bugs.python.org/issue29581
    python_requires='>=3.6.2',

    install_requires=[
        'boto3',
        'cached_property',
        'click',
        'cryptography',
        'logbook',
        'miscreant',
        'ulid-py',
    ],
    extras_require=dict(
        test=[
            'hypothesis',
            'mirakuru',
            'flake8',
            'port-for',
            'pytest',
            'pytest-cov',
            'pytest-flake8',
            'pytest-logbook'
        ],
    ),
    entry_points={
        'console_scripts': 'spare=spare.cli:cli'
    },
    classifiers=[
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Archiving :: Backup',
    ]
)
