from setuptools import setup

setup(
    name='streamr', 
    version='0.0.1',
    packages=['streamr.client', 'streamr.client.util',
              'streamr.client.errors', 'streamr.protocol', 'streamr.protocol.errors', 'streamr.protocol.utils', 'streamr.rest'],
    install_requires=['websocket-client >= 0.54','requests >= 2.0']
    )
