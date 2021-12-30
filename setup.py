#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zuora',
      version='1.3.3',
      description='Singer.io tap for extracting data from the Zuora API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zuora'],
      install_requires=[
          'singer-python==5.7.0',
          'requests==2.20.0',
          'pendulum==1.2.0',
          'backoff==1.8.0',
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint==2.5.3'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-zuora=tap_zuora:main
      ''',
      packages=['tap_zuora'],
)
