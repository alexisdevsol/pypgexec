#  pypgexec. Script to execute queries in postgres database
#  Copyright (C) 2021  Alexis Torres Valdes
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#
#  This library is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#  Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public
#  License along with this library; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
#  USA
#
#  Contact: alexis89.dev@gmail.com

import os
import sys

from setuptools import setup, find_packages

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(BASE_DIR, 'pypgexec', '__version__.py')) as f:
    exec(f.read(), about)

NAME = about['__title__']

CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 6)

# This check and everything above must remain compatible with Python 2.7.
if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write(f"""
==========================
Unsupported Python version
==========================

This version of {NAME} requires Python {REQUIRED_PYTHON[0]}.{REQUIRED_PYTHON[1]}, but you're trying to
install it on Python {REQUIRED_PYTHON[0]}.{REQUIRED_PYTHON[1]}.

This may be because you are using a version of pip that doesn't
understand the python_requires classifier. Make sure you
have pip >= 9.0 and setuptools >= 24.2, then try again:

    $ python -m pip install --upgrade pip setuptools
    $ python -m pip install usbsecurity
""")
    sys.exit(1)


def read(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    try:
        file = open(path, encoding='utf-8')
    except TypeError:
        file = open(path)
    return file.read()


def get_install_requires():
    return [i.strip() for i in open('requirements.txt').readlines()]


setup(
    name=about['__title__'],
    description=about['__description__'],
    version=about['__version__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    license=about['__license__'],
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    keywords=['python', 'database', 'postgres'],
    platforms=['Linux', 'Windows', 'MacOS'],
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'pypgexec=pypgexec.pypgexec:main',
        ]
    },
    include_package_data=True,
    python_requires='>={}.{}'.format(*REQUIRED_PYTHON),
    install_requires=get_install_requires(),
    zip_safe=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Database :: Front-Ends',
    ],
)
