from setuptools import setup, find_packages

#next time:
#python setup.py register
#python setup.py sdist upload

version = open('cqlengine/VERSION', 'r').readline().strip()

long_desc = """
Cassandra CQL 3 Object Mapper for Python

[Documentation](https://cqlengine.readthedocs.org/en/latest/)

[Report a Bug](https://github.com/bdeggleston/cqlengine/issues)

[Users Mailing List](https://groups.google.com/forum/?fromgroups#!forum/cqlengine-users)
"""

setup(
    name='cqlengine',
    version=version,
    description='Cassandra CQL 3 Object Mapper for Python',
    long_description=long_desc,
    classifiers = [
        "Environment :: Web Environment",
        "Environment :: Plugins",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords='cassandra,cql,orm',
    install_requires = ['cassandra-driver >= 2.1.0', 'six >= 1.7.2'],
    author='Blake Eggleston, Jon Haddad',
    author_email='bdeggleston@gmail.com, jon@jonhaddad.com',
    url='https://github.com/cqlengine/cqlengine',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
)

