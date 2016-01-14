from cassandra.cluster import Cluster, Session
import dse.cqltypes  # unsued here, imported to cause type registration
from dse.util import Point, Circle, LineString, Polygon


class Cluster(Cluster):

    def _new_session(self):
        session = Session(self, self.metadata.all_hosts())
        self._session_register_user_types(session)
        self.sessions.add(session)
        return session


class Session(Session):

    def __init__(self, cluster, hosts):
        super(Session, self).__init__(cluster, hosts)

        def cql_encode_str_quoted(val):
            return "'%s'" % val

        for typ in (Point, Circle, LineString, Polygon):
            self.encoder.mapping[typ] = cql_encode_str_quoted
