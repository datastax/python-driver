# New API for Python Driver's DSEGraph integration

## GraphSession

#### constructor
- parameters: `Session` mandatory, `default_graph_options` optional.

#### public fields
 - `default_graph_options`, dictionary initialised with the default graph options defined in the specs. Options can be overriten or other added.
- `session`, the wrapped `cassandra.cluster.Session`.

#### exposed methods
 - `execute()` takes arguments as either a `GraphStatement` or `BoundGraphStatement` or a query string, to execute it *synchronously* against the DSE cluster. Returns a `GraphResultSet` object containing the results of the execution.
 - `prepare()` takes in argument a `GraphStatement` or a query string, and prepares it against the DSE instance. Returns a `PreparedGraphStatement`.

##### private fields
_None_



## AbstractGraphStatement

_Is not supposed to be directly used by users, is not part of the public API. Whereas child implementations will be exposed._

#### constructor
- parameters: _None_

#### public fields
- `graph_options`, dictionary containing the graph options to use and to override from the default ones defined in the session.

#### exposed methods
_None_

##### private fields
- `_wrapped`, the wrapped driver's statement. It is not supposed to be modified by users but only used internally.



## GraphStatement _[extends AbstractGraphStatement]_

#### constructor
- parameters: `query_string` mandatory, `graph_options` optional.

#### public fields
- inherited from `AbstractGraphStatement`
- `graph_options`, used to override `GraphSession`'s default graph options, on a per-statement basis.

#### exposed methods 

_None_

##### private fields
- inherited from `AbstractGraphStatement`



## PreparedGraphStatement

#### constructor
- not supposed to be used by users

#### public fields 
_None_

#### exposed methods
- `bind()`, binds the provided values, and creates a `BoundGraphStatement` instance. The values must be in a dictionary, meaning that all parameters must be named, an exception will be raised if it is not the case.

##### private fields
- `_prepared_statement`, the `cassandra.query.PreparedStatement` instance wrapped.
- `_graph_statement`, the graph statement associated with this statement. Used to propagate the graph options to a `BoundGraphStatement` created from a `PreparedGraphStatement`.



## BoundGraphStatement _[extends AbstractGraphStatement]_

#### constructor
- not supposed to be used by users.

#### public fields
- inherited from `AbstractGraphStatement`
- `values`, the values to be bound with the statement and sent to the server.

#### exposed methods
- `bind()`, bind the values parameters to this `BoundGraphStatement` instance. The values must be in a dictionary, meaning that all parameters must be named, an exception will be raised if it is not the case.

##### private fields:
- inherited from `AbstractGraphStatement`
- `_prepared_statement`, the `cassandra.query.PreparedStatement` tied to this `BoundGraphStatement`.
- `_graph_statement`, the graph statement associated with this statement. Used to propagate the graph options to a `BoundGraphStatement` created from a `PreparedGraphStatement`.



## GraphResultSet

#### constructor
- not supposed to be used by users.

#### public fields 
_None_

#### exposed methods
- `__iter__()`/`next()`, allows iteration in the set, in `for ... in ...` loops. And manual iteration. Each iteration produces and returns one `GraphTraversalResult`.
- `__getitem__`, allows getting results by index as a array. Each indexed access will return a `GraphTraversalResult`.

##### private fields
 - `_wrapped_rs`, the wrapped `cassandra.cluster.ResultSet`.
