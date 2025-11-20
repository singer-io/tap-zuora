#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-zuora",
    version="1.6.0",
    description="Singer.io tap for extracting data from the Zuora API",
    author="Stitch",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_zuora"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.4",
        "pendulum==3.1.0",
        "backoff==2.2.1",
    ],
    extras_require={"dev": ["ipdb", "pylint"]},
    entry_points="""
          [console_scripts]
          tap-zuora=tap_zuora:main
      """,
    packages=["tap_zuora"],
)
