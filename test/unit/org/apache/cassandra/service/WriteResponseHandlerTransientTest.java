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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.locator.ReplicaUtils.trans;

public class WriteResponseHandlerTransientTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;

    static final InetAddressAndPort EP1;
    static final InetAddressAndPort EP2;
    static final InetAddressAndPort EP3;
    static final InetAddressAndPort EP4;
    static final InetAddressAndPort EP5;
    static final InetAddressAndPort EP6;

    static final String DC1 = "datacenter1";
    static final String DC2 = "datacenter2";
    static Token dummy;
    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.1.0.1");
            EP2 = InetAddressAndPort.getByName("127.1.0.2");
            EP3 = InetAddressAndPort.getByName("127.1.0.3");
            EP4 = InetAddressAndPort.getByName("127.2.0.4");
            EP5 = InetAddressAndPort.getByName("127.2.0.5");
            EP6 = InetAddressAndPort.getByName("127.2.0.6");
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setupClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.1"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.1"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.getAddress().getAddress();
                if (address[1] == 1)
                    return DC1;
                else
                    return DC2;
            }

            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
            {
                return unsortedAddress;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
            {
                return false;
            }
        });

        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("ks", KeyspaceParams.nts(DC1, "3/1", DC2, "3/1"), SchemaLoader.standardCFMD("ks", "tbl"));
        ks = Keyspace.open("ks");
        cfs = ks.getColumnFamilyStore("tbl");
        dummy = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0));
    }

    @Test
    public void checkPendingReplicasAreNotFiltered()
    {
        EndpointsForToken natural = EndpointsForToken.of(dummy.getToken(), full(EP1), full(EP2), trans(EP3), full(EP5));
        EndpointsForToken pending = EndpointsForToken.of(dummy.getToken(), full(EP4), trans(EP6));
        ReplicaLayout.ForTokenWrite layout = new ReplicaLayout.ForTokenWrite(ks.getReplicationStrategy(), natural, pending);
        ReplicaPlan.ForWrite replicaPlan = ReplicaPlans.forWrite(ks, ConsistencyLevel.QUORUM, layout, layout, ReplicaPlans.writeAll);

        Assert.assertTrue(Iterables.elementsEqual(EndpointsForRange.of(full(EP4), trans(EP6)),
                                                  replicaPlan.pending()));
    }

    private static ReplicaPlan.ForWrite expected(EndpointsForToken natural, EndpointsForToken selected)
    {
        return new ReplicaPlan.ForWrite(ks, ks.getReplicationStrategy(), ConsistencyLevel.QUORUM, EndpointsForToken.empty(dummy.getToken()), natural, natural, selected);
    }

    private static ReplicaPlan.ForWrite getSpeculationContext(EndpointsForToken natural, Predicate<InetAddressAndPort> livePredicate)
    {
        ReplicaLayout.ForTokenWrite liveAndDown = new ReplicaLayout.ForTokenWrite(ks.getReplicationStrategy(), natural, EndpointsForToken.empty(dummy.getToken()));
        ReplicaLayout.ForTokenWrite live = new ReplicaLayout.ForTokenWrite(ks.getReplicationStrategy(), natural.filter(r -> livePredicate.test(r.endpoint())), EndpointsForToken.empty(dummy.getToken()));
        return ReplicaPlans.forWrite(ks, ConsistencyLevel.QUORUM, liveAndDown, live, ReplicaPlans.writeNormal);
    }

    private static void assertSpeculationReplicas(ReplicaPlan.ForWrite expected, EndpointsForToken replicas, Predicate<InetAddressAndPort> livePredicate)
    {
        ReplicaPlan.ForWrite actual = getSpeculationContext(replicas, livePredicate);
        assertEquals(expected.pending(), actual.pending());
        assertEquals(expected.live(), actual.live());
        assertEquals(expected.contacts(), actual.contacts());
    }

    private static void assertEquals(ReplicaCollection<?> a, ReplicaCollection<?> b)
    {
        if (!Iterables.elementsEqual(a, b))
            Assert.assertTrue(a + " vs " + b, false);
    }

    private static Predicate<InetAddressAndPort> dead(InetAddressAndPort... endpoints)
    {
        Set<InetAddressAndPort> deadSet = Sets.newHashSet(endpoints);
        return ep -> !deadSet.contains(ep);
    }

    private static EndpointsForToken replicas(Replica... rr)
    {
        return EndpointsForToken.of(dummy.getToken(), rr);
    }

    @Test
    public void checkSpeculationContext()
    {
        EndpointsForToken all = replicas(full(EP1), full(EP2), trans(EP3), full(EP4), full(EP5), trans(EP6));
        // in happy path, transient replica should be classified as a backup
        assertSpeculationReplicas(expected(all, replicas(full(EP1), full(EP2), full(EP4), full(EP5))),
                                  all,
                                  dead());

        // full replicas must always be in the contact list, and will occur first
        assertSpeculationReplicas(expected(replicas(full(EP1), trans(EP3), full(EP4), trans(EP6)), replicas(full(EP1), full(EP2), full(EP4), full(EP5), trans(EP3), trans(EP6))),
                                  all,
                                  dead(EP2, EP5));

        // only one transient used as backup
        assertSpeculationReplicas(expected(replicas(full(EP1), trans(EP3), full(EP4), full(EP5), trans(EP6)), replicas(full(EP1), full(EP2), full(EP4), full(EP5), trans(EP3))),
                all,
                dead(EP2));
    }

    @Test (expected = UnavailableException.class)
    public void noFullReplicas()
    {
        getSpeculationContext(replicas(full(EP1), trans(EP2), trans(EP3)), dead(EP1));
    }

    @Test (expected = UnavailableException.class)
    public void notEnoughTransientReplicas()
    {
        getSpeculationContext(replicas(full(EP1), trans(EP2), trans(EP3)), dead(EP2, EP3));
    }
}
