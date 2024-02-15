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

package org.apache.cassandra.service.accord;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import accord.local.Node;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;

public class AccordTopologyUtils
{
    public static final Node.Id ID1 = new Node.Id(1);
    public static final Node.Id ID2 = new Node.Id(2);
    public static final Node.Id ID3 = new Node.Id(3);
    public static final List<Node.Id> NODE_LIST = ImmutableList.of(ID1, ID2, ID3);
    public static final Set<Node.Id> NODE_SET = ImmutableSet.copyOf(NODE_LIST);

    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static final Location LOCATION = new Location("DC1", "RACK1");

    public static InetAddressAndPort ep(int i)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(new byte[]{ 127, 0, 0, (byte)i}), 7012);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static NodeId nodeId(int id)
    {
        return new NodeId(id);
    }

    static void addNode(ClusterMetadata.Transformer transformer, int node, Token token)
    {
        NodeId nodeId = nodeId(node);
        InetAddressAndPort ep = ep(node);
        NodeAddresses addresses = new NodeAddresses(nodeId.toUUID(), ep, ep, ep);
        transformer.register(nodeId, addresses, LOCATION, NodeVersion.CURRENT);
        transformer.withNodeState(nodeId, NodeState.JOINED);
        transformer.proposeToken(nodeId, Collections.singleton(token));
        transformer.addToRackAndDC(nodeId);
    }

    public static ClusterMetadata configureCluster(List<Range<Token>> ranges, Keyspaces keyspaces)
    {
        assert ranges.size() == 3;

        IPartitioner partitioner = Murmur3Partitioner.instance;
        ClusterMetadata empty = new ClusterMetadata(partitioner);
        ClusterMetadata.Transformer transformer = empty.transformer();
        transformer.with(new DistributedSchema(keyspaces));
        addNode(transformer, 1, ranges.get(0).right);
        addNode(transformer, 2, ranges.get(1).right);
        addNode(transformer, 3, ranges.get(2).right);
        ClusterMetadata metadata = transformer.build().metadata;

        for (KeyspaceMetadata keyspace : keyspaces)
        {
            ReplicationParams replication = keyspace.params.replication;
            AbstractReplicationStrategy strategy = AbstractReplicationStrategy.createReplicationStrategy(keyspace.name, replication);
            DataPlacements.Builder placements = metadata.placements.unbuild();
            DataPlacement placement = strategy.calculateDataPlacement(Epoch.EMPTY, metadata.tokenMap.toRanges(), metadata);
            placements.with(replication, placement);
            metadata = transformer.with(placements.build()).build().metadata;
        }

        return metadata;
    }

    static Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(Token left, Token right)
    {
        return new Range<>(left, right);
    }

    public static Range<Token> range(long left, long right)
    {
        return range(token(left), token(right));
    }

}
