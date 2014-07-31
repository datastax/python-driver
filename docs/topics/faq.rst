==========================
Frequently Asked Questions
==========================

Q: Why don't updates work correctly on models instantiated as Model(field=blah, field2=blah2)?
-------------------------------------------------------------------

A: The recommended way to create new rows is with the models .create method. The values passed into a model's init method are interpreted by the model as the values as they were read from a row. This allows the model to "know" which rows have changed since the row was read out of cassandra, and create suitable update statements.