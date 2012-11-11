cassandraengine
===============

Django ORM / Mongoengine style ORM for Cassandra

In it's current state you can define column families, create and delete column families
based on your model definiteions, save models and retrieve models by their primary keys.

That's about it. Also, there are only 2 tests and the CQL stuff is very simplistic at this point.

##TODO
* dynamic column support
* return None when row isn't found in find()
* tests
* query functionality
* nice column and model class __repr__
