/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tcm;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ClusterMetadataService}.
 */
public class ClusterMetadataServiceTest
{
    private static final Location LOCATION = new Location("datacenter1", "rack1");

    private static Set<NodeId> resolveIgnored(ClusterMetadata metadata, String... ignores)
    {
        return ClusterMetadataService.instance().resolveIgnoredEndpoints(metadata, Arrays.asList(ignores));
    }

    private static ClusterMetadata metadataWithNodes(int... lastOctets)
    {
        String[] addresses = new String[lastOctets.length];
        for (int i = 0; i < lastOctets.length; i++)
            addresses[i] = "127.0.0." + lastOctets[i];
        return metadataWithAddresses(addresses);
    }

    private static ClusterMetadata metadataWithAddresses(String... addresses)
    {
        Directory directory = new Directory();
        for (String address : addresses)
            directory = directory.with(new NodeAddresses(InetAddressAndPort.getByNameUnchecked(address)), LOCATION);
        return metadataWith(directory);
    }

    private static ClusterMetadata metadataWith(Directory directory)
    {
        return ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                        .transformer().with(directory).build().metadata;
    }

    private static Set<NodeId> nodeIds(ClusterMetadata metadata, int... lastOctets)
    {
        String[] addresses = new String[lastOctets.length];
        for (int i = 0; i < lastOctets.length; i++)
            addresses[i] = "127.0.0." + lastOctets[i];
        return nodeIdsFor(metadata, addresses);
    }

    private static Set<NodeId> nodeIdsFor(ClusterMetadata metadata, String... addresses)
    {
        Set<NodeId> ids = new HashSet<>();
        for (String address : addresses)
            ids.add(metadata.directory.peerId(InetAddressAndPort.getByNameUnchecked(address)));
        return ids;
    }

    @SuppressWarnings("SameParameterValue")
    private static InetAddressAndPort addr(int lastOctet)
    {
        return InetAddressAndPort.getByNameUnchecked("127.0.0." + lastOctet);
    }

    @Before
    public void setup()
    {
        DatabaseDescriptor.toolInitialization();
        ClusterMetadataService.initializeForTools(true);
    }

    @Test
    public void testResolveIgnoredEndpointsReturnsNothingForEmptyList()
    {
        // No node is ignored, it should return empty set
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3);
        assertThat(resolveIgnored(metadata)).isEmpty();
    }

    @Test
    public void testResolveIgnoredEndpointsResolvesSingleAddress()
    {
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3, 4, 5);
        assertThat(resolveIgnored(metadata, "127.0.0.2")).isEqualTo(nodeIds(metadata, 2));
    }

    @Test
    public void testResolveIgnoredEndpointsResolvesMultipleAddresses()
    {
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3, 4, 5);
        assertThat(resolveIgnored(metadata, "127.0.0.2", "127.0.0.4")).isEqualTo(nodeIds(metadata, 2, 4));
    }

    @Test
    public void testResolveIgnoredEndpointsResolvesHostnameViaDns() throws UnknownHostException
    {
        // Whatever localhost resolves to is registered as a node, so ignoring "localhost" must map to that node. This
        // exercises the name-resolution path without assuming localhost is 127.0.0.1 (it might be IPv6 on some hosts).
        InetAddressAndPort local = InetAddressAndPort.getByName("localhost");
        Directory directory = new Directory().with(new NodeAddresses(local), LOCATION)
                                             .with(new NodeAddresses(addr(2)), LOCATION);
        ClusterMetadata metadata = metadataWith(directory);

        assertThat(resolveIgnored(metadata, "localhost")).isEqualTo(Collections.singleton(metadata.directory.peerId(local)));
    }

    @Test
    public void testResolveIgnoredEndpointsRejectsAddressNotInCluster()
    {
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3);
        assertThatThrownBy(() -> resolveIgnored(metadata, "127.0.0.99"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("don't exist in the cluster")
        .hasMessageContaining("127.0.0.99");
    }

    @Test
    public void testResolveIgnoredEndpointsRejectsUnresolvableHost()
    {
        // .invalid is reserved to never resolve (RFC 6761), so getByName is guaranteed to throw.
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3);
        assertThatThrownBy(() -> resolveIgnored(metadata, "no-such-host.invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown host in ignore list: no-such-host.invalid");
    }

    @Test
    public void testResolveIgnoredEndpointsMatchesSingleNodeForSlash32Cidr()
    {
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3, 4, 5);
        assertThat(resolveIgnored(metadata, "127.0.0.3/32")).isEqualTo(nodeIds(metadata, 3));
    }

    @Test
    public void testResolveIgnoredEndpointsMatchesEveryNodeInSubnet()
    {
        // 127.0.0.0/30 covers 127.0.0.0 - 127.0.0.3, so it matches nodes .1, .2 and .3 but not .4 or .5.
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3, 4, 5);
        assertThat(resolveIgnored(metadata, "127.0.0.0/30")).isEqualTo(nodeIds(metadata, 1, 2, 3));
    }

    @Test
    public void testResolveIgnoredEndpointsSkipsSubnetMatchingNoNode()
    {
        // A valid subnet which covers none of the cluster's addresses is not an error: it simply matches nothing.
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3);
        assertThat(resolveIgnored(metadata, "127.0.1.0/24")).isEmpty();
    }

    @Test
    public void testResolveIgnoredEndpointsRejectsInvalidCidr()
    {
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3);
        assertThatThrownBy(() -> resolveIgnored(metadata, "127.0.0.0/33"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CIDR in ignore list: 127.0.0.0/33");

        // should throw exception when malformed CIDR is passed
        assertThatThrownBy(() -> resolveIgnored(metadata, "127.0.0.0/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CIDR in ignore list: 127.0.0.0/");
    }

    @Test
    public void testResolveIgnoredEndpointsCombinesAndDeduplicatesAddressesAndCidrs()
    {
        // .4 given explicitly, the /30 covering .1, .2 and .3, and a /32 that overlaps .2 again: the result is their
        // union with no duplicates.
        ClusterMetadata metadata = metadataWithNodes(1, 2, 3, 4, 5);
        assertThat(resolveIgnored(metadata, "127.0.0.4", "127.0.0.0/30", "127.0.0.2/32"))
        .isEqualTo(nodeIds(metadata, 1, 2, 3, 4));
    }

    @Test
    public void testResolveIgnoredEndpointsResolvesIpv6Address()
    {
        ClusterMetadata metadata = metadataWithAddresses("fe80::1", "fe80::2", "fe80::3");
        assertThat(resolveIgnored(metadata, "fe80::2")).isEqualTo(nodeIdsFor(metadata, "fe80::2"));
    }

    @Test
    public void testResolveIgnoredEndpointsMatchesIpv6Cidr()
    {
        // fe80::/126 covers fe80::0 - fe80::3, so it matches nodes ::1, ::2 and ::3 but not ::10.
        ClusterMetadata metadata = metadataWithAddresses("fe80::1", "fe80::2", "fe80::3", "fe80::10");
        assertThat(resolveIgnored(metadata, "fe80::/126")).isEqualTo(nodeIdsFor(metadata, "fe80::1", "fe80::2", "fe80::3"));
    }

    @Test
    public void testResolveIgnoredEndpointsMatchesOnlyIpv6NodesForIpv6Cidr()
    {
        // In a mixed cluster an IPv6 subnet must match only the IPv6 nodes, leaving the IPv4 node untouched.
        ClusterMetadata metadata = metadataWithAddresses("127.0.0.1", "fe80::1", "fe80::2");
        assertThat(resolveIgnored(metadata, "fe80::/64")).isEqualTo(nodeIdsFor(metadata, "fe80::1", "fe80::2"));
    }
}
