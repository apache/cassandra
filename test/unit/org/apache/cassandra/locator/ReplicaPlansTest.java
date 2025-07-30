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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.StubClusterMetadataService;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaUtils.EP1;
import static org.apache.cassandra.locator.ReplicaUtils.EP2;
import static org.apache.cassandra.locator.ReplicaUtils.EP3;
import static org.apache.cassandra.locator.ReplicaUtils.EP4;
import static org.apache.cassandra.locator.ReplicaUtils.EP5;
import static org.apache.cassandra.locator.ReplicaUtils.EP6;
import static org.apache.cassandra.locator.ReplicaUtils.R1;
import static org.apache.cassandra.locator.ReplicaUtils.assertEquals;
import static org.apache.cassandra.locator.ReplicaUtils.tk;
import static org.apache.cassandra.locator.ReplicaUtils.trans;

public class ReplicaPlansTest
{
    static Keyspace ks;
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Keyspace ks(Set<InetAddressAndPort> dc1, Map<String, String> replication)
    {
        return ks(dc1, replication, "NetworkTopologyStrategy");
    }

    private static Keyspace ks(Set<InetAddressAndPort> dc1, Map<String, String> replication, String strategy)
    {
        replication = ImmutableMap.<String, String>builder().putAll(replication).put("class", strategy).build();
        Keyspace keyspace = Keyspace.mockKS(KeyspaceMetadata.create("blah", KeyspaceParams.create(false, replication)));
        return keyspace;
    }

    @Before
    public void setup()
    {
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(StubClusterMetadataService.forTesting());
        ClusterMetadataTestHelper.register(EP1, "DC1", "R1");
        ClusterMetadataTestHelper.register(EP2, "DC1", "R1");
        ClusterMetadataTestHelper.register(EP3, "DC1", "R1");
        ClusterMetadataTestHelper.register(EP4, "DC2", "R2");
        ClusterMetadataTestHelper.register(EP5, "DC2", "R2");
        ClusterMetadataTestHelper.register(EP6, "DC2", "R2");
    }

    private static Replica full(InetAddressAndPort ep) { return fullReplica(ep, R1); }

    @Test
    public void testWriteEachQuorum()
    {
        final Token token = tk(1L);
        {
            // all full natural
            Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
            EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
            EndpointsForToken pending = EndpointsForToken.empty(token);
            ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, (cm) -> natural, (cm) -> pending, null, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
            assertEquals(natural, plan.liveAndDown);
            assertEquals(natural, plan.live);
            assertEquals(natural, plan.contacts());
        }
        {
            // all natural and up, one transient in each DC
            // Note: this is confusing because it looks misconfigured as the Keyspace has never been setup with any
            // transient replicas in its replication params.
            Keyspace ks = ks(ImmutableSet.of(EP1, EP2, EP3), ImmutableMap.of("DC1", "3", "DC2", "3"));
            EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
            EndpointsForToken pending = EndpointsForToken.empty(token);
            ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(ks, ConsistencyLevel.EACH_QUORUM, (cm) -> natural, (cm) -> pending, Epoch.FIRST, Predicates.alwaysTrue(), ReplicaPlans.writeNormal);
            assertEquals(natural, plan.liveAndDown);
            assertEquals(natural, plan.live);
            EndpointsForToken expectContacts = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP4), full(EP5));
            assertEquals(expectContacts, plan.contacts());
        }
    }

    @Test
    public void testContactForReadUsingNetworkTopologyStrategy()
    {
        final Token token = tk(1L);

        Map<String, String> datacenters = ImmutableMap.of("DC1", "3", "DC2", "3");
        Keyspace keyspace = ks(ImmutableSet.of(EP1, EP2, EP3), datacenters);
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        List<InetAddressAndPort> nodes = Lists.newArrayList(EP1, EP2, EP3, EP4, EP5, EP6);
        Locator locator = generateLocator(datacenters, nodes);

        EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
        EndpointsForToken contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP1, EP2, EP4, EP5), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), trans(EP2), trans(EP3), trans(EP4), trans(EP5), trans(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP1, EP2, EP4, EP5), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), full(EP2), trans(EP3), trans(EP4), full(EP5), trans(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP2, EP1, EP4, EP5), contacts.endpoints());

        natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP1, EP2, EP4, EP5), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), trans(EP2), trans(EP3), trans(EP4), full(EP5), trans(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP5, EP1, EP2, EP4), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), trans(EP2), trans(EP3), full(EP4), full(EP5), full(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.EACH_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP5, EP1, EP2, EP4), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), full(EP2), full(EP3), full(EP4), trans(EP5), full(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.LOCAL_QUORUM, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP2, EP1), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), full(EP2), full(EP3), full(EP4), trans(EP5), full(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.LOCAL_ONE, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP2), contacts.endpoints());

        natural = EndpointsForToken.of(token, trans(EP1), full(EP2), full(EP3), full(EP4), trans(EP5), full(EP6));
        contacts = ReplicaPlans.contactForRead(locator, strategy, ConsistencyLevel.THREE, false, natural);
        Assert.assertEquals(Sets.newHashSet(EP2, EP1, EP3), contacts.endpoints());
    }

    @Test
    public void testContactForReadUsingSimpleStrategy()
    {
        final Token token = tk(1L);

        Map<String, String> oneDC = ImmutableMap.of("replication_factor", "3");
        Keyspace keyspace = ks(ImmutableSet.of(EP1, EP2, EP3), oneDC, "SimpleStrategy");
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        List<InetAddressAndPort> nodes = Lists.newArrayList(EP1, EP2, EP3);
        Locator locator = generateLocator(ImmutableMap.of("DC1", "3"), nodes);

        for (ConsistencyLevel consistencyLevel : ConsistencyLevel.values())
        {
            if (consistencyLevel == ConsistencyLevel.NODE_LOCAL)
                continue;

            EndpointsForToken natural = EndpointsForToken.of(token, full(EP1), full(EP2), trans(EP3));
            EndpointsForToken contacts = ReplicaPlans.contactForRead(locator, strategy, consistencyLevel, false, natural);
            assertContacts(contacts, consistencyLevel.blockFor(strategy), EP1);

            natural = EndpointsForToken.of(token, full(EP1), full(EP2), full(EP3));
            contacts = ReplicaPlans.contactForRead(locator, strategy, consistencyLevel, false, natural);
            assertContacts(contacts, consistencyLevel.blockFor(strategy), EP1);

            natural = EndpointsForToken.of(token, trans(EP1), full(EP2), trans(EP3));
            contacts = ReplicaPlans.contactForRead(locator, strategy, consistencyLevel, false, natural);
            assertContacts(contacts, consistencyLevel.blockFor(strategy), EP2);

            natural = EndpointsForToken.of(token, trans(EP1), trans(EP2), full(EP3));
            contacts = ReplicaPlans.contactForRead(locator, strategy, consistencyLevel, false, natural);
            assertContacts(contacts, consistencyLevel.blockFor(strategy), EP3);

            natural = EndpointsForToken.of(token, trans(EP1), trans(EP2), trans(EP3));
            contacts = ReplicaPlans.contactForRead(locator, strategy, consistencyLevel, false, natural);
            Assert.assertEquals(consistencyLevel.blockFor(strategy), contacts.size());
            Assert.assertTrue(contacts.get(0).isTransient());
            Assert.assertEquals(EP1, contacts.get(0).endpoint());
        }
    }

    private static void assertContacts(EndpointsForToken contacts, int expectSize, InetAddressAndPort expectedFirstEndpoint)
    {
        Assert.assertEquals(expectSize, contacts.size());
        Assert.assertTrue(contacts.get(0).isFull());
        Assert.assertEquals(expectedFirstEndpoint, contacts.get(0).endpoint());
    }

    private Locator generateLocator(Map<String, String> datacenters, Collection<InetAddressAndPort> nodes)
    {
        final Map<NodeId, String> nodeToRack = new HashMap<>();
        final Map<NodeId, String> nodeToDC = new HashMap<>();
        final Map<InetAddressAndPort, NodeId> epToId = new HashMap<>();

        List<InetAddressAndPort> nodeList = new ArrayList<>(nodes);
        Iterator<InetAddressAndPort> nodeIter = nodeList.iterator();
        int id = 0;

        for (Map.Entry<String, String> entry : datacenters.entrySet())
        {
            String dc = entry.getKey();
            int count;
            try
            {
                count = Integer.parseInt(entry.getValue());
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Invalid number of nodes for DC " + dc + ": " + entry.getValue());
            }

            for (int i = 0; i < count; i++)
            {
                if (!nodeIter.hasNext())
                    throw new IllegalArgumentException("Not enough nodes for datacenter assignment");

                InetAddressAndPort node = nodeIter.next();
                NodeId nodeId = new NodeId(++id);
                epToId.put(node, nodeId);
                nodeToDC.put(nodeId, dc);
                nodeToRack.put(nodeId, "rack" + i);
            }
        }

        if (nodeIter.hasNext())
            throw new IllegalArgumentException("Too many nodes for datacenter assignment");

        Directory dir = new Directory()
        {
            @Override
            public NodeId peerId(InetAddressAndPort endpoint)
            {
                return epToId.get(endpoint);
            }

            @Override
            public Location location(NodeId id)
            {
                return new Location(nodeToDC.get(id), nodeToRack.get(id));
            }
        };

        return Locator.usingDirectory(dir);
    }
}
