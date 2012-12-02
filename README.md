cqlengine
===============

cqlengine is a Cassandra CQL ORM for Python in the style of the Django orm and mongoengine

[Documentation](https://cqlengine.readthedocs.org/en/latest/)

[Report a Bug](https://github.com/bdeggleston/cqlengine/issues)

[Users Mailing List](https://groups.google.com/forum/?fromgroups#!forum/cqlengine-users)

[Dev Mailing List](https://groups.google.com/forum/?fromgroups#!forum/cqlengine-dev)

**NOTE: cqlengine is in alpha and under development, some features may change (hopefully with notice). Make sure to check the changelog and test your app before upgrading**

## Installation
```
pip install cqlengine
```

## Getting Started

```python
#first, define a model
>>> from cqlengine import columns
>>> from cqlengine.models import Model

>>> class ExampleModel(Model):
>>>     example_id      = columns.UUID(primary_key=True)  
>>>     example_type    = columns.Integer(index=True)
>>>     created_at      = columns.DateTime()
>>>     description     = columns.Text(required=False)

#next, setup the connection to your cassandra server(s)...
>>> from cqlengine import connection
>>> connection.setup(['127.0.0.1:9160'])

#...and create your CQL table
>>> from cqlengine.management import create_table
>>> create_table(ExampleModel)

#now we can create some rows:
>>> em1 = ExampleModel.create(example_type=0, description="example1")
>>> em2 = ExampleModel.create(example_type=0, description="example2")
>>> em3 = ExampleModel.create(example_type=0, description="example3")
>>> em4 = ExampleModel.create(example_type=0, description="example4")
>>> em5 = ExampleModel.create(example_type=1, description="example5")
>>> em6 = ExampleModel.create(example_type=1, description="example6")
>>> em7 = ExampleModel.create(example_type=1, description="example7")
>>> em8 = ExampleModel.create(example_type=1, description="example8")
# Note: the UUID and DateTime columns will create uuid4 and datetime.now
# values automatically if we don't specify them when creating new rows

#and now we can run some queries against our table
>>> ExampleModel.objects.count()
8
>>> q = ExampleModel.objects(example_type=1)
>>> q.count()
4
>>> for instance in q:
>>>     print q.description
example5
example6
example7
example8

#here we are applying additional filtering to an existing query
#query objects are immutable, so calling filter returns a new
#query object
>>> q2 = q.filter(example_id=em5.example_id)

>>> q2.count()
1
>>> for instance in q2:
>>>     print q.description
example5
```
