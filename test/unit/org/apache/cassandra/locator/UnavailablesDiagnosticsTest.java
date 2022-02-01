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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.NeverSpeculativeRetryPolicy;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UnavailablesDiagnosticsTest
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
    static Token token;
    static EndpointsForToken natural;

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
    public static void setUpClass() throws Throwable
    {
        OverrideConfigurationLoader.override((config) -> {
            config.diagnostic_events_enabled = true;
        });

        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // create snitch for DC
        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.addressBytes;
                return "rake" + address[1];
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.getAddress().getAddress();
                if (address[1] == 1)
                    return DC1;
                else
                    return DC2;
            }
        });

        // create token ring and make endpoints available in gossiper
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        List<InetAddressAndPort> instances = ImmutableList.of(EP1, EP2, EP3, EP4, EP5, EP6);
        for (int i = 0; i < instances.size(); i++)
        {
            InetAddressAndPort ip = instances.get(i);
            metadata.updateHostId(UUID.randomUUID(), ip);
            Murmur3Partitioner.LongToken token = new Murmur3Partitioner.LongToken(i);
            metadata.updateNormalToken(token, ip);

            Gossiper.instance.initializeNodeUnsafe(ip, metadata.getHostId(ip), MessagingService.current_version, 1);
            Gossiper.instance.injectApplicationState(ip, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance).tokens(Collections.singleton(token)));
        }

        // local DC
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));

        // create keyspace
        SchemaLoader.createKeyspace("ks", KeyspaceParams.nts(DC1, "3", DC2, "3"), SchemaLoader.standardCFMD("ks", "tbl"));
        ks = Keyspace.open("ks");
        cfs = ks.getColumnFamilyStore("tbl");
        token = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0));
        natural = EndpointsForToken.of(token.getToken(), full(EP1), full(EP2), full(EP3), full(EP4), full(EP5), full(EP6));
    }

    @After
    public void unsubscribeAll()
    {
        DiagnosticEventService.instance().cleanup();
        Gossiper.runInGossipStageBlocking(() -> ImmutableList.of(EP1, EP2, EP3, EP4, EP5, EP6)
                                                             .forEach(ep -> Gossiper.instance.realMarkAlive(ep, Gossiper.instance.getEndpointStateForEndpoint(ep))));
    }

    @AfterClass
    public static void cleanup()
    {
        Gossiper.instance.clearUnsafe();
    }


    @Test(expected=UnavailableException.class)
    public void testReadLocalOneUnavailable()
    {
        testForRead(downReplicas(EP1, EP2, EP3), ImmutableList.of(), ConsistencyLevel.LOCAL_ONE, 1, 1);
    }

    @Test
    public void testReadLocalOneAvailable()
    {
        testForRead(downReplicas(EP1, EP2), ImmutableList.of(EP3), ConsistencyLevel.LOCAL_ONE, 1, 1);
    }

    @Test(expected=UnavailableException.class)
    public void testReadEachQuorumUnavailable()
    {
        testForRead(downReplicas(EP1, EP2, EP6), ImmutableList.of(EP3, EP4, EP5), ConsistencyLevel.EACH_QUORUM, 4, 2);
    }

    @Test
    public void testReadEachQuorum()
    {
        testForRead(downReplicas(EP1, EP4), ImmutableList.of(EP2, EP3, EP5, EP6), ConsistencyLevel.EACH_QUORUM, 4, 2);
    }

    @Test(expected=UnavailableException.class)
    public void testReadQuorumUnavailable()
    {
        testForRead(downReplicas(EP4, EP5, EP6), ImmutableList.of(EP1, EP2, EP3), ConsistencyLevel.QUORUM, 4, 4);
    }

    @Test
    public void testReadQuorum()
    {
        testForRead(downReplicas(EP1, EP4), ImmutableList.of(EP2, EP3, EP5, EP6), ConsistencyLevel.QUORUM, 4, 4);
    }


    @Test(expected=UnavailableException.class)
    public void testReadRangeQuorumUnavailable()
    {
        testForRangeRead(downReplicas(EP4, EP5, EP6), ImmutableList.of(EP1, EP2, EP3), ConsistencyLevel.QUORUM, 4, 4);
    }

    @Test
    public void testReadRangeQuorum()
    {
        testForRangeRead(downReplicas(EP1, EP4), ImmutableList.of(EP2, EP3, EP5, EP6), ConsistencyLevel.QUORUM, 4, 4);
    }

    @Test(expected=UnavailableException.class)
    public void testWriteLocalOneUnavailable()
    {
        testForWrite(downReplicas(EP1, EP2, EP3), ConsistencyLevel.LOCAL_ONE, 1, 1);
    }

    @Test
    public void testWriteLocalOneAvailable()
    {
        testForWrite(downReplicas(EP1, EP2), ConsistencyLevel.LOCAL_ONE, 1, 1);
    }

    @Test(expected=UnavailableException.class)
    public void testWriteQuorumUnavailable()
    {
        testForWrite(downReplicas(EP4, EP5, EP6), ConsistencyLevel.QUORUM, 4, 4);
    }

    @Test
    public void testWriteQuorum()
    {
        testForWrite(downReplicas(EP1, EP4), ConsistencyLevel.QUORUM, 4, 4);
    }

    private void testForRead(Set<Replica> down, List<InetAddressAndPort> contacts, ConsistencyLevel cl, int blockFor, int required)
    {
        List<InetAddressAndPort> downEPs = down.stream().sorted().map(Replica::endpoint).collect(Collectors.toList());
        AtomicInteger wasCalled = new AtomicInteger(0);
        DiagnosticEventService.instance().subscribe(UnavailableEvent.class, (ev) -> {
            Assert.assertEquals(UnavailableEvent.UnavailableEventType.FOR_READ, ev.getType());
            assertEvent(ev, required, contacts, token);
            assertMap(ev, blockFor, contacts, downEPs, token);
            wasCalled.incrementAndGet();
        });
        try
        {
            ReplicaPlans.forRead(ks, token, cl, NeverSpeculativeRetryPolicy.INSTANCE);
        }
        catch (UnavailableException e)
        {
            Assert.assertEquals(1, wasCalled.get());
            throw e;
        }
        Assert.assertEquals(0, wasCalled.get());
    }

    private void testForRangeRead(Set<Replica> down, List<InetAddressAndPort> contacts, ConsistencyLevel cl, int blockFor, int required)
    {
        List<InetAddressAndPort> downEPs = down.stream().sorted().map(Replica::endpoint).collect(Collectors.toList());
        AtomicInteger wasCalled = new AtomicInteger(0);
        DiagnosticEventService.instance().subscribe(UnavailableEvent.class, (ev) -> {
            Assert.assertEquals(UnavailableEvent.UnavailableEventType.FOR_RANGE_READ, ev.getType());
            assertEvent(ev, required, contacts, null);
            assertNotNull(ev.getBounds());
            assertMap(ev, blockFor, contacts, downEPs, null);
            wasCalled.incrementAndGet();
        });
        try
        {
            Range<PartitionPosition> range = Range.makeRowRange(new Murmur3Partitioner.LongToken(1), new Murmur3Partitioner.LongToken(10));
            ReplicaPlans.forRangeRead(ks, cl, range, 1);
        }
        catch (UnavailableException e)
        {
            Assert.assertEquals(1, wasCalled.get());
            throw e;
        }
        Assert.assertEquals(0, wasCalled.get());
    }

    private void testForWrite(Set<Replica> down, ConsistencyLevel cl, int blockFor, int required)
    {
        List<InetAddressAndPort> downEPs = down.stream().sorted().map(Replica::endpoint).collect(Collectors.toList());
        ReplicaLayout.ForTokenWrite live = new ReplicaLayout.ForTokenWrite(ks.getReplicationStrategy(), natural.filter(r -> !down.contains(r)), EndpointsForToken.empty(token.getToken()));
        Set<InetAddressAndPort> liveAddr = live.all().list.stream().map(Replica::endpoint).collect(Collectors.toSet());
        AtomicInteger wasCalled = new AtomicInteger(0);
        DiagnosticEventService.instance().subscribe(UnavailableEvent.class, (ev) -> {
            Assert.assertEquals(UnavailableEvent.UnavailableEventType.FOR_WRITE, ev.getType());
            List<InetAddressAndPort> contacts = new ArrayList<>();
            contacts.addAll(liveAddr);
            contacts.addAll(downEPs);
            contacts.sort(Comparator.naturalOrder());
            assertEvent(ev, required, contacts, null);
            assertMap(ev, blockFor, contacts, downEPs, null);
            wasCalled.incrementAndGet();
        });

        try
        {
            ReplicaLayout.ForTokenWrite tokenWrite = ReplicaLayout.forTokenWriteLiveAndDown(ks, token);
            ReplicaPlans.forWrite(ks, cl, tokenWrite, ReplicaPlans.writeNormal);
        }
        catch (UnavailableException e)
        {
            Assert.assertEquals(1, wasCalled.get());
            throw e;
        }
        Assert.assertEquals(0, wasCalled.get());
    }

    private void assertEvent(UnavailableEvent ev, int required, List<InetAddressAndPort> contacts, @Nullable Token token)
    {
        Assert.assertEquals(token, ev.getToken());
        Assert.assertNotNull(ev.getContacts());
        List<InetAddressAndPort> clist = new ArrayList<>(ev.getContacts());
        clist.sort(Comparator.naturalOrder());
        Assert.assertEquals(contacts, clist);
        Assert.assertEquals(required, ev.getException().required);
    }

    @SuppressWarnings("unchecked")
    private void assertMap(UnavailableEvent ev, int blockFor, List<InetAddressAndPort> contacts, List<InetAddressAndPort> downEPs, @Nullable Token token)
    {
        Map<String, Serializable> mev = ev.toMap();
        Map<String, Serializable> mex = (Map<String, Serializable>) mev.get("exception");
        Assert.assertEquals(ev.getException().alive, mex.get("alive"));
        Assert.assertEquals(ev.getException().required, mex.get("required"));
        Assert.assertEquals(ev.getException().consistency.name(), mex.get("consistency"));
        Assert.assertEquals(ks.getName(), mev.get("keyspaceName"));
        Assert.assertEquals(token, mev.get("token"));
        Assert.assertEquals(DatabaseDescriptor.getLocalDataCenter(), mev.get("localDC"));
        Assert.assertEquals(blockFor, mev.get("blockFor"));
        List<String> sdown = downEPs.stream().map(i -> i.toString(true)).sorted().collect(Collectors.toList());
        Assert.assertEquals(sdown, mev.get("downInstances"));
        List<String> scontacts = contacts.stream().map(i -> i.toString(true)).sorted().collect(Collectors.toList());
        Assert.assertEquals(scontacts, mev.get("contacts"));
        Map<String, Serializable> mrs = (Map<String, Serializable>) mev.get("replicationStrategy");
        assertEquals(ImmutableMap.of(DC1, 3, DC2, 3), mrs.get("datacenters"));
        assertEquals(ks.getName(), mrs.get("keyspaceName"));
        assertEquals(6, mrs.get("rf"));
        assertEquals("NetworkTopologyStrategy", mrs.get("class"));
    }

    private Set<Replica> downReplicas(InetAddressAndPort ... ep)
    {
        Set<Replica> ret = Arrays.stream(ep).map(ReplicaUtils::full).collect(Collectors.toSet());
        Gossiper.runInGossipStageBlocking(() -> ret.forEach(r -> Gossiper.instance.markDead(r.endpoint(), Gossiper.instance.getEndpointStateForEndpoint(r.endpoint()))));
        return ret;
    }

}
