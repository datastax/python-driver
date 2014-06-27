==========================
Frequently Asked Questions
==========================

Q: Why does calling my Model(field=blah, field2=blah2) not work?
-------------------------------------------------------------------

A: The __init__() of a model is used by cqlengine internally.  If you want to create a new row in the database, use create().
