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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTopologyUtils.*;

public class AccordTopologyTest
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static TableId tableId = null;
    private static KeyspaceMetadata keyspace = null;
    private static final Location LOCATION = new Location("DC1", "RACK1");

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        TableMetadata table = parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks").build();
        tableId = table.id;
        keyspace = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3), Tables.of(table));
    }

    /**
     * Check converter does the right thing if the ring is constructed with min and max tokens
     */
    @Test
    public void minMaxTokens()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(partitioner.getMinimumToken(), token(-100)),
                                                     range(-100, 100),
                                                     range(token(100), partitioner.getMaximumTokenForSplitting()));
        Assert.assertEquals(partitioner.getMinimumToken(), ranges.get(0).left);
        Assert.assertEquals(partitioner.getMaximumTokenForSplitting(), ranges.get(2).right);
        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));

        Topology topology = AccordTopology.createAccordTopology(metadata);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopology.minRange(tableId, ranges.get(0).right), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(2)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.maxRange(tableId, ranges.get(2).right), NODE_LIST, NODE_SET));

        Assert.assertEquals(expected, topology);
    }

    @Test
    public void wrapAroundRanges()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(-100, 0),
                                                     range(0, 100),
                                                     range(100, -100));

        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));
        Topology topology = AccordTopology.createAccordTopology(metadata);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopology.minRange(tableId, ranges.get(0).left), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(0)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.maxRange(tableId, ranges.get(2).left), NODE_LIST, NODE_SET));

        Assert.assertEquals(expected, topology);
    }

    @Test
    public void fastPath()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(partitioner.getMinimumToken(), token(-100)),
                                                     range(-100, 100),
                                                     range(token(100), partitioner.getMaximumTokenForSplitting()));
        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));
        Topology topology = AccordTopology.createAccordTopology(metadata);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopology.minRange(tableId, ranges.get(0).right), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(2)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.maxRange(tableId, ranges.get(2).right), NODE_LIST, NODE_SET));
        Assert.assertEquals(expected, topology);

        topology = AccordTopology.createAccordTopology(metadata.transformer().withFastPathStatusSince(new Id(1), AccordFastPath.Status.UNAVAILABLE, 1, 1).build().metadata);

        Set<Node.Id> fastPath = new HashSet<>(NODE_SET);
        fastPath.remove(new Node.Id(1));

        expected = new Topology(2,
                                new Shard(AccordTopology.minRange(tableId, ranges.get(0).right), NODE_LIST, fastPath),
                                new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, fastPath),
                                new Shard(AccordTopology.range(tableId, ranges.get(2)), NODE_LIST, fastPath),
                                new Shard(AccordTopology.maxRange(tableId, ranges.get(2).right), NODE_LIST, fastPath));
        Assert.assertEquals(expected, topology);
    }

    /**
     * Even if there are too many failures to reach quorum, fast path size shouldn't go below quorum size
     */
    @Test
    public void fastPathWithMoreThanMinimumFailedNodes()
    {
        List<Range<Token>> ranges = ImmutableList.of(range(partitioner.getMinimumToken(), token(-100)),
                                                     range(-100, 100),
                                                     range(token(100), partitioner.getMaximumTokenForSplitting()));
        ClusterMetadata metadata = configureCluster(ranges, Keyspaces.of(keyspace));
        Topology topology = AccordTopology.createAccordTopology(metadata);
        Topology expected = new Topology(1,
                                         new Shard(AccordTopology.minRange(tableId, ranges.get(0).right), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.range(tableId, ranges.get(2)), NODE_LIST, NODE_SET),
                                         new Shard(AccordTopology.maxRange(tableId, ranges.get(2).right), NODE_LIST, NODE_SET));
        Assert.assertEquals(expected, topology);

        metadata = metadata.transformer()
                           .withFastPathStatusSince(new Id(1), AccordFastPath.Status.UNAVAILABLE, 1, 1)
                           .withFastPathStatusSince(new Id(2), AccordFastPath.Status.UNAVAILABLE, 1, 1)
                           .build().metadata;
        topology = AccordTopology.createAccordTopology(metadata);

        Set<Node.Id> fastPath = new HashSet<>(NODE_SET);
        fastPath.remove(new Node.Id(1));

        expected = new Topology(2,
                                new Shard(AccordTopology.minRange(tableId, ranges.get(0).right), NODE_LIST, fastPath),
                                new Shard(AccordTopology.range(tableId, ranges.get(1)), NODE_LIST, fastPath),
                                new Shard(AccordTopology.range(tableId, ranges.get(2)), NODE_LIST, fastPath),
                                new Shard(AccordTopology.maxRange(tableId, ranges.get(2).right), NODE_LIST, fastPath));
        Assert.assertEquals(expected, topology);
    }
}
