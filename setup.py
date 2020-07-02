import os
from setuptools import setup, find_namespace_packages


def readlines(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as fh:
        return fh.readlines()


setup(
    name='charm_product',
    version='4.1.1',
    packages=find_namespace_packages(include=['charm_product', 'charm_product.*']),
    include_package_data=True,
    install_requires=readlines('requirements.txt'),
)
