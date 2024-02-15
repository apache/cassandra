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

package org.apache.cassandra.service.accord.repair;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.TopologySorter;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.topology.Topologies;
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
import org.apache.cassandra.service.accord.AccordTopology;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.service.accord.AccordTopologyUtils.*;

public class RequiredResponseTrackerTest
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static TableId tableId = null;
    private static KeyspaceMetadata keyspace = null;
    private static Topology topology;
    private static final Location LOCATION = new Location("DC1", "RACK1");

    private static final List<Range<Token>> RANGES = ImmutableList.of(range(-100, 0), range(0, 100), range(100, -100));
    private static final TopologySorter TOPOLOGY_SORTER = (node1, node2, shards) -> node1.compareTo(node2);

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        TableMetadata table = parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c)) WITH transactional_mode='full'", "ks").build();
        tableId = table.id;
        keyspace = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3), Tables.of(table));

        ClusterMetadata metadata = configureCluster(RANGES, Keyspaces.of(keyspace));
        topology = AccordTopology.createAccordTopology(metadata);

    }

    @Test
    public void successCase()
    {
        Set<Node.Id> nodes = topology.nodes();
        Assert.assertEquals(NODE_SET, nodes);
        RequiredResponseTracker tracker = new RequiredResponseTracker(nodes, new Topologies.Single(TOPOLOGY_SORTER, topology));
        Assert.assertEquals(RequestStatus.NoChange, tracker.recordSuccess(ID1));
        Assert.assertEquals(RequestStatus.NoChange, tracker.recordSuccess(ID2));
        Assert.assertEquals(RequestStatus.Success, tracker.recordSuccess(ID3));
    }

    @Test
    public void failureCase()
    {
        Set<Node.Id> nodes = topology.nodes();
        Assert.assertEquals(NODE_SET, nodes);
        RequiredResponseTracker tracker = new RequiredResponseTracker(nodes, new Topologies.Single(TOPOLOGY_SORTER, topology));
        Assert.assertEquals(RequestStatus.NoChange, tracker.recordSuccess(ID1));
        Assert.assertEquals(RequestStatus.Failed, tracker.recordFailure(ID2));
    }
}
