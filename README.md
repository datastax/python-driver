cassandraengine
===============

Cassandra ORM for Python in the style of the Django orm and mongoengine

In it's current state you can define column families, create and delete column families
based on your model definiteions, save models and retrieve models by their primary keys.

That's about it. Also, the CQL stuff is very basic at this point.

##TODO
* Real querying
* mongoengine fields? URLField, EmbeddedDocument, ListField, DictField
* column ttl?
* ForeignKey/DBRef fields?
* dynamic column support
* query functionality
* Match column names to mongoengine field names?
* nice column and model class __repr__


