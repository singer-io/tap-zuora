#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zuora',
      version='0.0.9',
      description='Singer.io tap for extracting data from the Zuora API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zuora'],
      install_requires=[
          'singer-python==1.6.0',
          'requests==2.12.4',
      ],
      entry_points='''
          [console_scripts]
          tap-zuora=tap_zuora:main
      ''',
      packages=['tap_zuora'],
)
