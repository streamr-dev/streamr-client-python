"""
setup func
"""

from setuptools import setup, find_packages


import sys

if sys.version_info < (3, 4):
    sys.exit('Sorry, Python 3.5 or higher is required.')

with open('requirements.txt') as f:
    reqs = f.read()

setup(
    name='streamr',
    version='0.0.1',
    python_requires='>3.4',
    packages=find_packages(),
    package_dir={'streamr': 'streamr'},
    install_requires=reqs.strip().split('\n'),
    )
