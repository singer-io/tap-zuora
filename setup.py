#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zuora',
      version='0.2.1',
      description='Singer.io tap for extracting data from the Zuora API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zuora'],
      install_requires=[
          'singer-python==5.0.6',
          'requests==2.12.4',
          'pendulum==1.2.0',
      ],
      entry_points='''
          [console_scripts]
          tap-zuora=tap_zuora:main
      ''',
      packages=['tap_zuora'],
)
