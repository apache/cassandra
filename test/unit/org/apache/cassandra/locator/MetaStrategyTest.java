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

package org.apache.cassandra.locator;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;

public class MetaStrategyTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static class NodeConfiguration
    {
        final NodeAddresses addresses;
        final Location location;
        final long token;

        NodeConfiguration(NodeAddresses addresses, Location location, long token)
        {
            this.addresses = addresses;
            this.location = location;
            this.token = token;
        }
    }

    public static NodeConfiguration node(NodeAddresses addresses, Location location, long token)
    {
        return new NodeConfiguration(addresses, location, token);
    }

    public static ClusterMetadata metadata(NodeConfiguration... configurations)
    {
        Directory directory = new Directory();
        TokenMap tokenMap = new TokenMap(Murmur3Partitioner.instance);
        for (NodeConfiguration configuration : configurations)
        {
            directory = directory.with(configuration.addresses, configuration.location);
            directory = directory.withRackAndDC(directory.peerId(configuration.addresses.broadcastAddress));
            tokenMap = tokenMap.assignTokens(directory.peerId(configuration.addresses.broadcastAddress), Collections.singleton(new Murmur3Partitioner.LongToken(configuration.token)));
        }

        return new ClusterMetadata(Epoch.EMPTY,
                                   Murmur3Partitioner.instance,
                                   DistributedSchema.empty(),
                                   directory,
                                   tokenMap,
                                   DataPlacements.EMPTY,
                                   LockedRanges.EMPTY,
                                   InProgressSequences.EMPTY,
                                   ImmutableMap.of());
    }

    @Test
    public void testDatacenterAware() throws Throwable
    {
        ClusterMetadata metadata = metadata(node(addr(1), location("dc1", "rack1"), 1),
                                            node(addr(2), location("dc1", "rack1"), 2),
                                            node(addr(3), location("dc1", "rack1"), 3),
                                            node(addr(4), location("dc2", "rack2"), 4),
                                            node(addr(5), location("dc2", "rack2"), 5),
                                            node(addr(6), location("dc2", "rack2"), 6),
                                            node(addr(7), location("dc3", "rack3"), 7),
                                            node(addr(8), location("dc3", "rack3"), 8),
                                            node(addr(9), location("dc3", "rack3"), 9));

        Map<String, Integer> rf = new HashMap<>();
        rf.put("dc1", 2);
        rf.put("dc2", 2);
        rf.put("dc3", 2);

        CMSPlacementStrategy placementStrategy = new CMSPlacementStrategy(rf, (cd, n) -> true);
        Assert.assertEquals(nodeIds(metadata.directory,
                                    1, 2, 4, 5, 7, 8),
                            placementStrategy.reconfigure(metadata));

        Assert.assertEquals(nodeIds(metadata.directory,
                                    1, 2, 4, 5, 7, 8),
                            placementStrategy.reconfigure(metadata));

        placementStrategy = new CMSPlacementStrategy(rf, (cd, n) -> !n.equals(metadata.directory.peerId(addr(2).broadcastAddress)) &&
                                                                    !n.equals(metadata.directory.peerId(addr(2).broadcastAddress)));
        Assert.assertEquals(nodeIds(metadata.directory,
                                    1, 3, 4, 5, 7, 8),
                            placementStrategy.reconfigure(metadata));

        Assert.assertEquals(nodeIds(metadata.directory,
                                    1, 3, 4, 5, 7, 8),
                            placementStrategy.reconfigure(metadata));
    }

    public static Set<NodeId> nodeIds(Directory directory, int... addrs) throws UnknownHostException
    {
        Set<NodeId> nodeIds = new HashSet<>();
        for (int addr : addrs)
            nodeIds.add(directory.peerId(InetAddressAndPort.getByName("127.0.0." + addr)));
        return nodeIds;
    }

    public static NodeAddresses addr(int i)
    {
        try
        {
            InetAddressAndPort inetAddressAndPort = InetAddressAndPort.getByName("127.0.0." + i);
            return new NodeAddresses(inetAddressAndPort);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Location location(String dc, String rack)
    {
        return new Location(dc, rack);
    }
}