#!/usr/bin/env python

import sys

from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand


def read_readme():
    with open('README.md') as f:
        return f.read()


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []  # pylint: disable=attribute-defined-outside-init
        self.test_suite = True  # pylint: disable=attribute-defined-outside-init

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(name='dblue_stores',
      version='2.0.2',
      description='Dblue stores is an abstraction and a collection of clients to interact with storages.',
      long_description=read_readme(),
      long_description_content_type="text/markdown",
      maintainer='Rajesh Hegde',
      maintainer_email='rh@dblue.ai',
      author='Rajesh Hegde',
      author_email='rh@dblue.ai',
      url='https://github.com/dblueai/dblue-stores',
      license='MIT',
      platforms='any',
      packages=find_packages(),
      keywords=[
          'dblue',
          'aws',
          's3',
          'microsoft',
          'azure',
          'google cloud storage',
          'gcs',
          'machine-learning',
          'data-science',
          'artificial-intelligence',
          'ai',
          'reinforcement-learning',
          'kubernetes',
          'docker',
          'sftp',
      ],
      install_requires=[
          "python-decouple==3.1",
      ],
      extras_require={
          "s3": [
              "boto3==1.7.73",
              "botocore==1.10.84",
          ],
          "gcs": [
              "google-cloud-storage==1.10.0",
          ],
          "azure": [
              "azure-storage==0.36.0",
          ],
          "sftp": [
              "paramiko==2.6.0"
          ],
      },
      classifiers=[
          'Programming Language :: Python',
          'Operating System :: OS Independent',
          'Intended Audience :: Developers',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: Artificial Intelligence'
      ],
      tests_require=[
          "pytest",
      ],
      cmdclass={'test': PyTest})
