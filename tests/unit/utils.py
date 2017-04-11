from concurrent.futures import Future
from functools import wraps
from cassandra.cluster import Session
from mock import patch

def mock_session_pools(f):
    """
    Helper decorator that allows tests to initialize :class:.`Session` objects
    without actually connecting to a Cassandra cluster.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.object(Session, "add_or_renew_pool") as mocked_add_or_renew_pool:
            future = Future()
            future.set_result(object())
            mocked_add_or_renew_pool.return_value = future
            f(*args, **kwargs)
    return wrapper
