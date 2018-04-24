#!/usr/bin/env python3

from setuptools import setup, find_packages


setup(
    name='contortionist',
    version='0.1.0',

    description='Mail content filtering service',

    url='https://github.com/mjcaley/contortionist',

    author='Michael Caley',
    author_email='mjcaley@darkarctic.com',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',

        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',

        'License :: OSI Approved :: MIT License',

        'Topic :: Communications :: Email :: Filters',
    ],

    keywords='spam spamc spamassassin',

    packages=find_packages(exclude=['tests']),

    python_requires='!=2.*,!=3.0,!=3.1,!=3.2,!=3.3,!=3.4',
    setup_requires=['pytest-runner', ],
    install_requires=['toml',
                      'aiofiles',
                      'aiosmtpd',
                      'aiosmtplib',
                      'aiosqlite',
                      'aiospamc'],
    tests_require=['pytest-asyncio>=0.6', 'pytest-cov', 'pytest>=3.0', 'asynctest'],
)
