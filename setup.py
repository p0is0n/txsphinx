#!/usr/bin/env python

# Copyright (c) knigobaza.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Distutils installer for txsphinx.
"""

version = '0.1'

try:
    from setuptools import setup
    
    # Mark
    use_setuptools = True
except ImportError:
    from distutils.core import setup
    
    # Mark
    use_setuptools = False
 
import sys, os

def main(args):
    setup_args = dict(
        # metadata
        name="txsphinx",
        version=version,
        description="An asynchronous Python driver for the Sphinx search engine, based on Twisted.",
        author="knigobaza",
        author_email="poisonoff@gmail.com",
        url="https://github.com/p0is0n/txsphinx",
        license="Apache License, Version 2.0",
        long_description="""\
An asynchronous Python driver for the Sphinx search engine, based on Twisted. The txsphinx package is an alternative to the original sphinxapi implemented on python.

Because the original sphinxapi package has it's own connection pool and blocking low-level socket operations, it is hard to fully implement network servers using the Twisted framework. Instead of deferring all operations to threads, now it's possible to do it asynchronously, as easy as using the original API.
""",
        packages=["txsphinx"],
        platforms="All",
        classifiers=[
            "Programming Language :: Python :: 2.6",
            "Programming Language :: Python :: 2.7",
            ])

    if use_setuptools:
        from pkg_resources import parse_requirements
        requirements = ["twisted", "qbuf"]
        try:
            list(parse_requirements(requirements))
        except:
            print """You seem to be running a very old version of setuptools.
This version of setuptools has a bug parsing dependencies, so automatic
dependency resolution is disabled.
"""
        else:
            setup_args['install_requires'] = requirements
        setup_args['include_package_data'] = True
        setup_args['zip_safe'] = True
    setup(**setup_args)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        sys.exit(1)

