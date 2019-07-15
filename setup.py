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
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(name='dblue_stores',
      version='1.0.1',
      description='Dblue is an abstraction and a collection of clients '
                  'to interact with cloud storages.',
      long_description=read_readme(),
      long_description_content_type="text/markdown",
      maintainer='Rajesh Hegde',
      maintainer_email='rajesh@dblue.ai',
      author='Rajesh Hegde',
      author_email='rajesh@dblue.ai',
      url='',
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
          'docker'
      ],
      install_requires=[
          "python-decouple==3.1",
          "rhea>=0.5.4",
      ],
      extras_require={
          "s3": [
              "boto3",
              "botocore",
          ],
          "gcs": [
              "google-cloud-storage",
          ],
          "azure": [
              "azure-storage",
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
