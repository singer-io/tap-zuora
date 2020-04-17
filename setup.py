#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-zuora',
      version='1.0.0',
      description='Singer.io tap for extracting data from the Zuora API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Stitch',
      url='https://github.com/transferwise/pipelinewise-tap-zuora',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
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
