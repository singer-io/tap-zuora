#!/usr/bin/env python

from setuptools import setup

setup(name='tap-zuora',
      version='1.1.10',
      description='Singer.io tap for extracting data from the Zuora API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zuora'],
      install_requires=[
          'singer-python==5.1.1',
          'requests==2.20.0',
          'pendulum==1.2.0',
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-zuora=tap_zuora:main
      ''',
      packages=['tap_zuora'],
)
