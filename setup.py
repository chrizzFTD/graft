#!/usr/bin/env python
from setuptools import setup, find_packages

_VERSION = '0.0.1'
setup(
    name='graft',
    version=_VERSION,
    packages=find_packages(),
    description='they can graft a new hand onto the arm.',
    author='Christian López Barrón',
    author_email='chris.gfz@gmail.com',
    url='https://github.com/chrizzFTD/graft',
    download_url=f'https://github.com/chrizzFTD/graft/releases/tag/{_VERSION}',
    classifiers=[
        'Programming Language :: Python :: 3.8',
    ],
    extras_require={'docs': ['sphinx_autodoc_typehints', 'sphinx_rtd_theme']},
)
