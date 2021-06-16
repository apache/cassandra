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

import os
import random
import six
import stat
from cassandra.cluster import Cluster
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy, RoundRobinPolicy
from cassandra.pool import HostDistance


class EndpointWhiteListRoundRobinPolicy(WhiteListRoundRobinPolicy):
    """
    WhileListRoundRobinPolicy dedicated for the cloud connection.

    It is using endpoint instead of host address.

    Note:
    we want to override _allowed_hosts and _allowed_hosts_resolved which is the reason
    to not call direct super class init but rather the one from RoundRobinPolicy.
    """

    def __init__(self, hosts):
        self._allowed_hosts = self._allowed_hosts_resolved = tuple(hosts)
        RoundRobinPolicy.__init__(self)

    def populate(self, cluster, hosts):
        self._live_hosts = frozenset(h for h in hosts if h.endpoint in self._allowed_hosts_resolved)

        if len(hosts) <= 1:
            self._position = 0
        else:
            self._position = random.randint(0, len(hosts) - 1)

    def distance(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            return HostDistance.LOCAL
        else:
            return HostDistance.IGNORED

    def on_up(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_up(self, host)

    def on_add(self, host):
        if host.endpoint in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_add(self, host)


def cluster_factory(host, whitelist_lbp=True, **kwargs):
    """
    Cluster factory to create a cassandra or dse cluster for cqlsh.

    :param host: the host for the connection. This can be the hostname string or an EndPoint instance.
    :param whitelist_lbp: Specify if a WhiteListRoundRobinPolicy should be applied. This can be set to True,
                          False or an EndPoint instance, which is used to configure the policy when
                          connecting to a cloud cluster. On a cloud cluster, the LBP has to be set after the
                          nodes discovery. Otherwise, the LBP is set as usual to the execution profile.
                          Default to True.
    :kwargs: All other keyword arguments are passed to the Cluster constructor.
    :return: A Cluster instance.
    """

    is_cloud_cluster = False
    is_unix_socket_endpoint = False
    endpoint = host
    options = kwargs.copy()

    # Configure the cluster contact point type and address.
    if is_unix_socket(host):
        # update endpoint and load balancing policy for unix socket
        endpoint = UnixSocketEndPoint(host)
        is_unix_socket_endpoint = True

    # Determine if we are trying to connect to a cloud cluster
    secure_connect_bundle = options.pop('secure_connect_bundle', None)
    if secure_connect_bundle:
        is_cloud_cluster = True
        options['cloud'] = {'secure_connect_bundle': secure_connect_bundle}
    elif 'cloud' in options:
        is_cloud_cluster = options['cloud'] is not None

    # Build the Cluster instance
    if is_cloud_cluster:
        return _cloud_cluster_factory(whitelist_lbp, **options)

    if is_unix_socket_endpoint and 'port' in options:
        del options['port']

    contact_points = (endpoint,)

    if whitelist_lbp:
        lbp_class = WhiteListRoundRobinPolicy
        if is_unix_socket_endpoint:
            lbp_class = EndpointWhiteListRoundRobinPolicy

        whitelist = [endpoint] if whitelist_lbp is True else [whitelist_lbp]
        options['load_balancing_policy'] = lbp_class(whitelist)

    return Cluster(contact_points=contact_points, **options)


def is_unix_socket(hostname):
    if isinstance(hostname, six.string_types) and os.path.exists(hostname):
        mode = os.stat(hostname).st_mode
        return stat.S_ISSOCK(mode)
    return False


def _cloud_cluster_factory(whitelist_lbp, **kwargs):
    """
    Create cloud cluster from given options.
    Please notice that:
    - cloud should be present in option
    - contact_points, endpoint_factory, ssl_context, and ssl_options cannot be specified with a cloud configuration (will be removed)
    - whitelist_lbp can be True (random contact point), False (no policy set), or specific endpoint
    """
    options = kwargs.copy()
    assert 'cloud' in options

    cloud_prohibited_options = ['contact_points', 'endpoint_factory', 'ssl_context', 'ssl_options']
    for opt_name in cloud_prohibited_options:
        options.pop(opt_name, None)

    cluster = Cluster(**options)

    if whitelist_lbp:

        if whitelist_lbp is True:
            # applying load balancing policy as we now know the contact points
            contact_points = [random.choice(cluster.contact_points)]
        else:
            # An explicit host was specified
            contact_points = [whitelist_lbp]

        for execution_profile in cluster.profile_manager.profiles.values():
            execution_profile.load_balancing_policy = EndpointWhiteListRoundRobinPolicy(contact_points)
        cluster.endpoints_resolved = contact_points

    return cluster
