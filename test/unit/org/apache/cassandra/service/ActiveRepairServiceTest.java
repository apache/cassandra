/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.*;

import javax.xml.crypto.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.messages.RepairOption.DATACENTERS_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.FORCE_REPAIR_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.HOSTS_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.INCREMENTAL_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.RANGES_KEY;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.service.ActiveRepairService.getRepairedAt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";

    public String cfname;
    public ColumnFamilyStore store;
    public InetAddress LOCAL, REMOTE;

    private boolean initialized;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE5,
                                    KeyspaceParams.simple(2),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_COUNTER),
                                    SchemaLoader.standardCFMD(KEYSPACE5, CF_STANDARD1));
    }

    @Before
    public void prepare() throws Exception
    {
        if (!initialized)
        {
            SchemaLoader.startGossiper();
            initialized = true;

            LOCAL = FBUtilities.getBroadcastAddress();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddress.getByName("127.0.0.2");
        }

        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        StorageService.instance.setTokens(Collections.singleton(tmd.partitioner.getRandomToken()));
        tmd.updateNormalToken(tmd.partitioner.getMinimumToken(), REMOTE);
        assert tmd.isMember(REMOTE);
    }

    @Test
    public void testGetNeighborsPlusOne() throws Throwable
    {
        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, null, null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddress> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddress> expected = new HashSet<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }
        expected.remove(FBUtilities.getBroadcastAddress());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddress> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        Set<InetAddress> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null));
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        List<InetAddress> expected = new ArrayList<>();
        for (Range<Token> replicaRange : ars.getAddressRanges().get(FBUtilities.getBroadcastAddress()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicaRange));
        }

        expected.remove(FBUtilities.getBroadcastAddress());
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddress().getCanonicalHostName(),expected.get(0).getCanonicalHostName());
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);

        assertEquals(expected.get(0), ActiveRepairService.getNeighbors(KEYSPACE5, ranges,
                                                                       ranges.iterator().next(),
                                                                       null, hosts).iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor());
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(KEYSPACE5);
        ActiveRepairService.getNeighbors(KEYSPACE5, ranges, ranges.iterator().next(), null, hosts);
    }


    @Test
    public void testParentRepairStatus() throws Throwable
    {
        ActiveRepairService.instance.recordRepairStatus(1, ActiveRepairService.ParentRepairStatus.COMPLETED, ImmutableList.of("foo", "bar"));
        List<String> res = StorageService.instance.getParentRepairStatus(1);
        assertNotNull(res);
        assertEquals(ActiveRepairService.ParentRepairStatus.COMPLETED, ActiveRepairService.ParentRepairStatus.valueOf(res.get(0)));
        assertEquals("foo", res.get(1));
        assertEquals("bar", res.get(2));

        List<String> emptyRes = StorageService.instance.getParentRepairStatus(44);
        assertNull(emptyRes);

        ActiveRepairService.instance.recordRepairStatus(3, ActiveRepairService.ParentRepairStatus.FAILED, ImmutableList.of("some failure message", "bar"));
        List<String> failed = StorageService.instance.getParentRepairStatus(3);
        assertNotNull(failed);
        assertEquals(ActiveRepairService.ParentRepairStatus.FAILED, ActiveRepairService.ParentRepairStatus.valueOf(failed.get(0)));

    }

    Set<InetAddress> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddress> endpoints = new HashSet<>();
        for (int i = 1; i <= max; i++)
        {
            InetAddress endpoint = InetAddress.getByName("127.0.0." + i);
            tmd.updateNormalToken(tmd.partitioner.getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    @Test
    public void testSnapshotAddSSTables() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        UUID prsId = UUID.randomUUID();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        ActiveRepairService.instance.registerParentRepairSession(prsId, FBUtilities.getBroadcastAddress(), Collections.singletonList(store),
                                                                 Collections.singleton(new Range<>(store.getPartitioner().getMinimumToken(),
                                                                                                   store.getPartitioner().getMinimumToken())),
                                                                 true, System.currentTimeMillis(), true, PreviewKind.NONE);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.id, prsId);

        UUID prsId2 = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId2, FBUtilities.getBroadcastAddress(),
                                                                 Collections.singletonList(store),
                                                                 Collections.singleton(new Range<>(store.getPartitioner().getMinimumToken(),
                                                                                                   store.getPartitioner().getMinimumToken())),
                                                                 true, System.currentTimeMillis(),
                                                                 true, PreviewKind.NONE);
        createSSTables(store, 2);
        ActiveRepairService.instance.getParentRepairSession(prsId).maybeSnapshot(store.metadata.id, prsId);
        try (Refs<SSTableReader> refs = store.getSnapshotSSTableReaders(prsId.toString()))
        {
            assertEquals(original, Sets.newHashSet(refs.iterator()));
        }
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE5);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.truncateBlocking();
        store.disableAutoCompaction();
        createSSTables(store, 10);
        return store;
    }

    private void createSSTables(ColumnFamilyStore cfs, int count)
    {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(cfs.metadata(), timestamp, Integer.toString(j))
                .clustering("c")
                .add("val", "val")
                .build()
                .applyUnsafe();
            }
            cfs.forceBlockingFlush();
        }
    }

    private static RepairOption opts(String... params)
    {
        assert params.length % 2 == 0 : "unbalanced key value pairs";
        Map<String, String> opt = new HashMap<>();
        for (int i=0; i<(params.length >> 1); i++)
        {
            int idx = i << 1;
            opt.put(params[idx], params[idx+1]);
        }
        return RepairOption.parse(opt, DatabaseDescriptor.getPartitioner());
    }

    private static String b2s(boolean b)
    {
        return Boolean.toString(b);
    }

    /**
     * Tests the expected repairedAt value is returned, based on different RepairOption
     */
    @Test
    public void repairedAt() throws Exception
    {
        // regular incremental repair
        Assert.assertNotEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(true))));
        // subrange incremental repair
        Assert.assertNotEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                      RANGES_KEY, "1:2")));

        // hosts incremental repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                   HOSTS_KEY, "127.0.0.1")));
        // dc incremental repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                   DATACENTERS_KEY, "DC2")));
        // forced incremental repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                   FORCE_REPAIR_KEY, b2s(true))));

        // full repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, getRepairedAt(opts(INCREMENTAL_KEY, b2s(false))));
    }
}
