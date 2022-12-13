import os
import logging

import pytest
from ccmlib.cluster_factory import ClusterFactory as CCMClusterFactory

from . import CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME
from . import path as ccm_path


@pytest.fixture(scope="session", autouse=True)
def cleanup_clusters():

    yield

    if not os.environ.get('DISABLE_CLUSTER_CLEANUP'):
        for cluster_name in [CLUSTER_NAME, SINGLE_NODE_CLUSTER_NAME, MULTIDC_CLUSTER_NAME, 'shared_aware', 'sni_proxy']:
            try:
                cluster = CCMClusterFactory.load(ccm_path, cluster_name)
                logging.debug("Using external CCM cluster {0}".format(cluster.name))
                cluster.clear()
            except FileNotFoundError:
                pass
