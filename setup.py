#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import codecs
from setuptools import setup, find_packages


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="vre",
    description=("Inselspital SpitalHygiene"),
    keywords="inselspital vre python",
    url="http://bitbucket.org/sqooba/...",  # url of source code repository
    classifiers=[
        "Topic :: Utilities",
    ],
    install_requires=[
        "scikit-learn==0.19.1",
        "numpy==1.14.5",
        "pandas==0.23.0",
        "scipy==1.1.0",
        "pytest==3.7.1",  # for testing
    ],
    packages=find_packages(),  # find all the modules automatically
    include_package_data=True,  # use MANIFEST.in during install if needed
)