from cpython.tuple cimport (
        PyTuple_New,
        # Return value: New reference.
        # Return a new tuple object of size len, or NULL on failure.
        PyTuple_SET_ITEM,
        # Like PyTuple_SetItem(), but does no error checking, and should
        # only be used to fill in brand new tuples. Note: This function
        # ``steals'' a reference to o.
        )

from cpython.ref cimport (
        Py_INCREF
        # void Py_INCREF(object o)
        #     Increment the reference count for object o. The object must not
        #     be NULL; if you aren't sure that it isn't NULL, use
        #     Py_XINCREF().
        )

cdef inline tuple tuple_new(Py_ssize_t n):
    """Allocate a new tuple object"""
    return PyTuple_New(n)

cdef inline void tuple_set(tuple tup, Py_ssize_t idx, object item):
    """Insert new object into tuple. No item must have been set yet."""
    # PyTuple_SET_ITEM steals a reference, so we need to INCREF
    Py_INCREF(item)
    PyTuple_SET_ITEM(tup, idx, item)
