from distutils.core import setup, Extension

murmur3 = Extension('cassandra.murmur3', sources=['cassandra/murmur3.c'])

setup(
    name='cassandra',
    version='0.1',
    description='Python driver for Cassandra',
    packages=["cassandra"],
    ext_modules=[murmur3],
    install_requires=['pyev', 'python-snappy', 'futures'],
    test_requires=['nose', 'mock'])
