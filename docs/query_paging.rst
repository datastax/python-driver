.. _query-paging:

Paging Large Queries
====================
Cassandra 2.0+ offers support for automatic query paging.  Starting with
version 2.0 of the driver, if :attr:`~.Cluster.protocol_version` is greater than
:const:`2` (it is by default), queries returning large result sets will be
automatically paged.

Controlling the Page Size
-------------------------
By default, :attr:`.Session.default_fetch_size` controls how many rows will
be fetched per page.  This can be overridden per-query by setting
:attr:`~.fetch_size` on a :class:`~.Statement`.  By default, each page
will contain at most 5000 rows.

Handling Paged Results
----------------------
Whenever the number of result rows for are query exceed the page size, an
instance of :class:`~.PagedResult` will be returned instead of a normal
list.  This class implements the iterator interface, so you can treat
it like a normal iterator over rows::

    from cassandra.query import SimpleStatement
    query = "SELECT * FROM users"  # users contains 100 rows
    statement = SimpleStatement(query, fetch_size=10)
    for user_row in session.execute(statement):
        process_user(user_row)

Whenever there are no more rows in the current page, the next page will
be fetched transparently.  However, note that it *is* possible for
an :class:`Exception` to be raised while fetching the next page, just
like you might see on a normal call to ``session.execute()``.

If you use :meth:`.Session.execute_async()` along with,
:meth:`.ResponseFuture.result()`, the first page will be fetched before
:meth:`~.ResponseFuture.result()` returns, but latter pages will be
transparently fetched synchronously while iterating the result.

Handling Paged Results with Callbacks
-------------------------------------
If callbacks are attached to a query that returns a paged result,
the callback will be called once per page with a normal list of rows.

Use :attr:`.ResponseFuture.has_more_pages` and
:meth:`.ResponseFuture.start_fetching_next_page()` to continue fetching
pages.  For example::

    class PagedResultHandler(object):

        def __init__(self, future):
            self.error = None
            self.finished_event = Event()
            self.future = future
            self.future.add_callbacks(
                callback=self.handle_page,
                errback=self.handle_err)

        def handle_page(self, rows):
            for row in rows:
                process_row(row)

            if self.future.has_more_pages:
                self.future.start_fetching_next_page()
            else:
                self.finished_event.set()

        def handle_error(self, exc):
            self.error = exc
            self.finished_event.set()

    future = session.execute_async("SELECT * FROM users")
    handler = PagedResultHandler(future)
    handler.finished_event.wait()
    if handler.error:
        raise handler.error

Resume Paged Results
--------------------

You can resume the pagination when executing a new query by using the :attr:`.ResultSet.paging_state`. This can be useful if you want to provide some stateless pagination capabilities to your application (ie. via http). For example::

    from cassandra.query import SimpleStatement
    query = "SELECT * FROM users"
    statement = SimpleStatement(query, fetch_size=10)
    results = session.execute(statement)

    # save the paging_state somewhere and return current results
    session['paging_stage'] = results.paging_state


    # resume the pagination sometime later...
    statement = SimpleStatement(query, fetch_size=10)
    ps = session['paging_state']
    results = session.execute(statement, paging_state=ps)
