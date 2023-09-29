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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class AccordTopologyTest
{
    private static final Id ID1 = new Id(1);
    private static final Id ID2 = new Id(2);
    private static final Id ID3 = new Id(3);
    private static final List<Id> NODE_LIST = ImmutableList.of(ID1, ID2, ID3);
    private static final Set<Id> NODE_SET = ImmutableSet.copyOf(NODE_LIST);

    private static final InetAddressAndPort EP1 = ep(1);
    private static final InetAddressAndPort EP2 = ep(2);
    private static final InetAddressAndPort EP3 = ep(3);

    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static Tables tables = null;
    private static KeyspaceMetadata keyspace = null;
    private static final Location LOCATION = new Location("DC1", "RACK1");

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        tables = Tables.of(parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks").build());
        keyspace = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3), tables);
    }

    private static InetAddressAndPort ep(int i)
    {
        try
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(new byte[]{127, 0, 0, (byte)i}), 7012);
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

    private static void addNode(ClusterMetadata.Transformer transformer, int node, Token token)
    {
        NodeId nodeId = nodeId(node);
        InetAddressAndPort ep = ep(node);
        NodeAddresses addresses = new NodeAddresses(nodeId.toUUID(), ep, ep, ep);
        transformer.register(nodeId, addresses, LOCATION, NodeVersion.CURRENT);
        transformer.withNodeState(nodeId, NodeState.JOINED);
        transformer.proposeToken(nodeId, Collections.singleton(token));
    }

    private static ClusterMetadata configureCluster(List<Range<Token>> ranges, Keyspaces keyspaces)
    {
        assert ranges.size() == 3;

        IPartitioner partitioner = Murmur3Partitioner.instance;
        ClusterMetadata empty = new ClusterMetadata(partitioner);
        ClusterMetadata.Transformer transformer = empty.transformer();
        transformer.with(new DistributedSchema(Keyspaces.of(keyspace)));
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

    private static Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    private static Range<Token> range(Token left, Token right)
    {
        return new Range<>(left, right);
    }

    private static Range<Token> range(long left, long right)
    {
        return range(token(left), token(right));
    }

    /**
     * Check converter does the right thing if the ring is constructed with min and max tokens
     */
    @Test
    public void minMaxTokens()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(partitioner.getMinimumToken(), token(-100)),
                                                     range(-100, 100),
                                                     range(token(100), partitioner.getMaximumToken()));
        Assert.assertEquals(partitioner.getMinimumToken(), ranges.get(0).left);
        Assert.assertEquals(partitioner.getMaximumToken(), ranges.get(2).right);
        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));

        Topology topology = AccordTopologyUtils.createAccordTopology(metadata, ks -> true);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopologyUtils.minRange("ks", ranges.get(0).right), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.range("ks", ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.range("ks", ranges.get(2)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.maxRange("ks", ranges.get(2).right), NODE_LIST, NODE_SET));

        Assert.assertEquals(expected, topology);
    }

    @Test
    public void wrapAroundRanges()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(-100, 0),
                                                     range(0, 100),
                                                     range(100, -100));

        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));
        Topology topology = AccordTopologyUtils.createAccordTopology(metadata, ks -> true);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopologyUtils.minRange("ks", ranges.get(0).left), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.range("ks", ranges.get(0)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.range("ks", ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopologyUtils.maxRange("ks", ranges.get(2).left), NODE_LIST, NODE_SET));

        Assert.assertEquals(expected, topology);
    }
}
