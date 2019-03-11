"""
setup func
"""


from setuptools import setup

setup(
    name='streamr', 
    version='0.0.1',
    packages=['streamr.client', 'streamr.client.util',
              'streamr.client.errors', 'streamr.protocol', 'streamr.protocol.errors',
              'streamr.protocol.util', 'streamr.rest'],
    install_requires=['websocket-client == 0.54.0',
                      'requests == 2.18.4',
                      'six==1.11.0',
                      'setuptools==38.4.0']
    )
