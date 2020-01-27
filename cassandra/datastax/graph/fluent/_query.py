# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from cassandra.graph import SimpleGraphStatement
from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT

from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.structure.io.graphsonV2d0 import GraphSONWriter

from cassandra.datastax.graph.fluent.serializers import serializers

log = logging.getLogger(__name__)

graphson_writer = GraphSONWriter(serializer_map=serializers)

__all__ = ['TraversalBatch', '_query_from_traversal', '_DefaultTraversalBatch']


def _query_from_traversal(traversal):
    """
    From a GraphTraversal, return a query string.

    :param traversal: The GraphTraversal object
    """
    try:
        query = graphson_writer.writeObject(traversal)
    except Exception:
        log.exception("Error preparing graphson traversal query:")
        raise

    return query


class TraversalBatch(object):
    """
    A `TraversalBatch` is used to execute multiple graph traversals in a
    single transaction. If any traversal in the batch fails, the entire
    batch will fail to apply.

    If a TraversalBatch is bounded to a DSE session, it can be executed using
    `traversal_batch.execute()`.
    """

    _session = None
    _execution_profile = None

    def __init__(self, session=None, execution_profile=None):
        """
        :param session: (Optional) A DSE session
        :param execution_profile: (Optional) The execution profile to use for the batch execution
        """
        self._session = session
        self._execution_profile = execution_profile

    def add(self, traversal):
        """
        Add a traversal to the batch.

        :param traversal: A gremlin GraphTraversal
        """
        raise NotImplementedError()

    def add_all(self, traversals):
        """
        Adds a sequence of traversals to the batch.

        :param traversals: A sequence of gremlin GraphTraversal
        """
        raise NotImplementedError()

    def execute(self):
        """
        Execute the traversal batch if bounded to a `DSE Session`.
        """
        raise NotImplementedError()

    def as_graph_statement(self):
        """
        Return the traversal batch as GraphStatement.
        """
        raise NotImplementedError()

    def clear(self):
        """
        Clear a traversal batch for reuse.
        """
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def __str__(self):
        return u'<TraversalBatch traversals={0}>'.format(len(self))
    __repr__ = __str__


class _DefaultTraversalBatch(TraversalBatch):

    _traversals = None

    def __init__(self, *args, **kwargs):
        super(_DefaultTraversalBatch, self).__init__(*args, **kwargs)
        self._traversals = []

    @property
    def _query(self):
        return u"[{0}]".format(','.join(self._traversals))

    def add(self, traversal):
        if not isinstance(traversal, GraphTraversal):
            raise ValueError('traversal should be a gremlin GraphTraversal')

        query = _query_from_traversal(traversal)
        self._traversals.append(query)

        return self

    def add_all(self, traversals):
        for traversal in traversals:
            self.add(traversal)

    def as_graph_statement(self):
        return SimpleGraphStatement(self._query)

    def execute(self):
        if self._session is None:
            raise ValueError('A DSE Session must be provided to execute the traversal batch.')

        execution_profile = self._execution_profile if self._execution_profile else EXEC_PROFILE_GRAPH_DEFAULT
        return self._session.execute_graph(self._query, execution_profile=execution_profile)

    def clear(self):
        del self._traversals[:]

    def __len__(self):
        return len(self._traversals)
