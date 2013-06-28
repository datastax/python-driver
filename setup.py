from distutils.core import setup, Extension

murmur3 = Extension('cassandra.murmur3', sources=['cassandra/murmur3.c'])

setup(
    name='cassandra',
    version='0.1',
    description='Python driver for Cassandra',
    packages=["cassandra"],
    ext_modules=[murmur3],
    install_requires=['futures'],
    test_requires=['nose', 'mock'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]

)
