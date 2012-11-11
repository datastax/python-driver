cassandraengine
===============

Python Cassandra ORM in the style of django / mongoengine

In it's current state you can define column families, create and delete column families
based on your model definiteions, save models and retrieve models by their primary keys.

That's about it. Also, the CQL stuff is pretty simple at this point.

##TODO
* Complex queries (class Q(object))
* Match column names to mongoengine field names?
* mongoengine fields? URLField, EmbeddedDocument, ListField, DictField
* column ttl?
* ForeignKey/DBRef fields?
* dynamic column support
* tests
* query functionality
* nice column and model class __repr__


