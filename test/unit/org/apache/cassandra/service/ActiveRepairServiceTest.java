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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.repair.messages.RepairOption.DATACENTERS_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.FORCE_REPAIR_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.HOSTS_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.INCREMENTAL_KEY;
import static org.apache.cassandra.repair.messages.RepairOption.RANGES_KEY;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class ActiveRepairServiceTest
{
    public static final String KEYSPACE5 = "Keyspace5";
    public static final String CF_STANDARD1 = "Standard1";
    public static final String CF_COUNTER = "Counter1";
    public static final int TASK_SECONDS = 10;

    public String cfname;
    public ColumnFamilyStore store;
    public InetAddressAndPort LOCAL, REMOTE;

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

            LOCAL = FBUtilities.getBroadcastAddressAndPort();
            // generate a fake endpoint for which we can spoof receiving/sending trees
            REMOTE = InetAddressAndPort.getByName("127.0.0.2");
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
        Set<InetAddressAndPort> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        expected.remove(FBUtilities.getBroadcastAddressAndPort());
        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();
        Set<InetAddressAndPort> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges, range, null, null).endpoints());
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwo() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddressAndPort> expected = new HashSet<>();
        for (Replica replica : ars.getAddressReplicas().get(FBUtilities.getBroadcastAddressAndPort()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replica.range()).endpoints());
        }
        expected.remove(FBUtilities.getBroadcastAddressAndPort());
        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();
        Set<InetAddressAndPort> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges, range, null, null).endpoints());
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsPlusOneInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf+1 nodes, and ensure that all nodes are returned
        Set<InetAddressAndPort> expected = addTokens(1 + Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        expected.remove(FBUtilities.getBroadcastAddressAndPort());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddressAndPort> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();
        Set<InetAddressAndPort> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null).endpoints());
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInLocalDC() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the ARS are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        Set<InetAddressAndPort> expected = new HashSet<>();
        for (Replica replica : ars.getAddressReplicas().get(FBUtilities.getBroadcastAddressAndPort()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replica.range()).endpoints());
        }
        expected.remove(FBUtilities.getBroadcastAddressAndPort());
        // remove remote endpoints
        TokenMetadata.Topology topology = tmd.cloneOnlyTokenMap().getTopology();
        HashSet<InetAddressAndPort> localEndpoints = Sets.newHashSet(topology.getDatacenterEndpoints().get(DatabaseDescriptor.getLocalDataCenter()));
        expected = Sets.intersection(expected, localEndpoints);

        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();
        Set<InetAddressAndPort> neighbors = new HashSet<>();
        for (Range<Token> range : ranges)
        {
            neighbors.addAll(ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges, range, Arrays.asList(DatabaseDescriptor.getLocalDataCenter()), null).endpoints());
        }
        assertEquals(expected, neighbors);
    }

    @Test
    public void testGetNeighborsTimesTwoInSpecifiedHosts() throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();

        // generate rf*2 nodes, and ensure that only neighbors specified by the hosts are returned
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        AbstractReplicationStrategy ars = Keyspace.open(KEYSPACE5).getReplicationStrategy();
        List<InetAddressAndPort> expected = new ArrayList<>();
        for (Replica replicas : ars.getAddressReplicas().get(FBUtilities.getBroadcastAddressAndPort()))
        {
            expected.addAll(ars.getRangeAddresses(tmd.cloneOnlyTokenMap()).get(replicas.range()).endpoints());
        }

        expected.remove(FBUtilities.getBroadcastAddressAndPort());
        Collection<String> hosts = Arrays.asList(FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort(),expected.get(0).getHostAddressAndPort());
        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();

        assertEquals(expected.get(0), ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges,
                                                                       ranges.iterator().next(),
                                                                       null, hosts).endpoints().iterator().next());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetNeighborsSpecifiedHostsWithNoLocalHost() throws Throwable
    {
        addTokens(2 * Keyspace.open(KEYSPACE5).getReplicationStrategy().getReplicationFactor().allReplicas);
        //Dont give local endpoint
        Collection<String> hosts = Arrays.asList("127.0.0.3");
        Iterable<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE5).ranges();
        ActiveRepairService.instance().getNeighbors(KEYSPACE5, ranges, ranges.iterator().next(), null, hosts);
    }


    @Test
    public void testParentRepairStatus() throws Throwable
    {
        ActiveRepairService.instance().recordRepairStatus(1, ActiveRepairService.ParentRepairStatus.COMPLETED, ImmutableList.of("foo", "bar"));
        List<String> res = StorageService.instance.getParentRepairStatus(1);
        assertNotNull(res);
        assertEquals(ActiveRepairService.ParentRepairStatus.COMPLETED, ActiveRepairService.ParentRepairStatus.valueOf(res.get(0)));
        assertEquals("foo", res.get(1));
        assertEquals("bar", res.get(2));

        List<String> emptyRes = StorageService.instance.getParentRepairStatus(44);
        assertNull(emptyRes);

        ActiveRepairService.instance().recordRepairStatus(3, ActiveRepairService.ParentRepairStatus.FAILED, ImmutableList.of("some failure message", "bar"));
        List<String> failed = StorageService.instance.getParentRepairStatus(3);
        assertNotNull(failed);
        assertEquals(ActiveRepairService.ParentRepairStatus.FAILED, ActiveRepairService.ParentRepairStatus.valueOf(failed.get(0)));

    }

    Set<InetAddressAndPort> addTokens(int max) throws Throwable
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        Set<InetAddressAndPort> endpoints = new HashSet<>();
        for (int i = 1; i <= max; i++)
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("127.0.0." + i);
            tmd.updateNormalToken(tmd.partitioner.getRandomToken(), endpoint);
            endpoints.add(endpoint);
        }
        return endpoints;
    }

    @Test
    public void testSnapshotAddSSTables() throws Exception
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        TimeUUID prsId = nextTimeUUID();
        Set<SSTableReader> original = Sets.newHashSet(store.select(View.select(SSTableSet.CANONICAL, (s) -> !s.isRepaired())).sstables);
        Collection<Range<Token>> ranges = Collections.singleton(new Range<>(store.getPartitioner().getMinimumToken(), store.getPartitioner().getMinimumToken()));
        ActiveRepairService.instance().registerParentRepairSession(prsId, FBUtilities.getBroadcastAddressAndPort(), Collections.singletonList(store),
                                                                   ranges, true, System.currentTimeMillis(), true, PreviewKind.NONE);
        store.getRepairManager().snapshot(prsId.toString(), ranges, false);

        TimeUUID prsId2 = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(prsId2, FBUtilities.getBroadcastAddressAndPort(),
                                                                   Collections.singletonList(store),
                                                                   ranges,
                                                                   true, System.currentTimeMillis(),
                                                                   true, PreviewKind.NONE);
        createSSTables(store, 2);
        store.getRepairManager().snapshot(prsId.toString(), ranges, false);
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
            Util.flush(cfs);
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
        Assert.assertNotEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true)), false));
        // subrange incremental repair
        Assert.assertNotEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                      RANGES_KEY, "1:2"), false));

        // hosts incremental repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                   HOSTS_KEY, "127.0.0.1"), false));
        // dc incremental repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                   DATACENTERS_KEY, "DC2"), false));
        // forced incremental repair
        Assert.assertNotEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                      FORCE_REPAIR_KEY, b2s(true)), false));
        Assert.assertEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(true),
                                                                      FORCE_REPAIR_KEY, b2s(true)), true));

        // full repair
        Assert.assertEquals(UNREPAIRED_SSTABLE, ActiveRepairService.instance().getRepairedAt(opts(INCREMENTAL_KEY, b2s(false)), false));
    }

    @Test
    public void testRejectWhenPoolFullStrategy() throws InterruptedException
    {
        // Using RepairCommandPoolFullStrategy.reject, new threads are spawned up to
        // repair_command_pool_size, at which point futher submissions are rejected
        ExecutorService validationExecutor = ActiveRepairService.initializeExecutor(2, Config.RepairCommandPoolFullStrategy.reject);
        try
        {
            Condition blocked = newOneTimeCondition();
            CountDownLatch completed = new CountDownLatch(2);

            /*
             * CASSANDRA-16685 This is a Java bug. When the underlying executor's queue is a SynchronousQueue, there can
             * be races just after the ThreadPool's initialization while juggling and spinning up threads internally
             * leading to false rejections. That queue needs a thread ready to pick up the task immediately or it will
             * produce a reject exception upon 'offer()' method call on the executor's code. If the executor is still
             * initializing or threads are not ready to take work you can get false rejections.
             *
             * A sleep has been added to give time to the thread pool to be ready to get work.
             */
            Thread.sleep(250);
            validationExecutor.submit(new Task(blocked, completed));
            validationExecutor.submit(new Task(blocked, completed));

            try
            {
                validationExecutor.submit(new Task(blocked, completed));
                Assert.fail("Expected task submission to be rejected");
            }
            catch (RejectedExecutionException e)
            {
                // expected
            }

            // allow executing tests to complete
            blocked.signalAll();
            completed.await(TASK_SECONDS + 1, TimeUnit.SECONDS);

            // Submission is unblocked
            Thread.sleep(250);
            validationExecutor.submit(() -> {});
        }
        finally
        {
            // necessary to unregister mbean
            validationExecutor.shutdownNow();
        }
    }

    @Test
    public void testQueueWhenPoolFullStrategy() throws InterruptedException
    {
        // Using RepairCommandPoolFullStrategy.queue, the pool is initialized to
        // repair_command_pool_size and any tasks which cannot immediately be
        // serviced are queued
        ExecutorService validationExecutor = ActiveRepairService.initializeExecutor(2, Config.RepairCommandPoolFullStrategy.queue);
        try
        {
            Condition allSubmitted = newOneTimeCondition();
            Condition blocked = newOneTimeCondition();
            CountDownLatch completed = new CountDownLatch(5);
            ExecutorService testExecutor = Executors.newSingleThreadExecutor();
            for (int i = 0; i < 5; i++)
            {
                if (i < 4)
                    testExecutor.submit(() -> validationExecutor.submit(new Task(blocked, completed)));
                else
                    testExecutor.submit(() -> {
                        validationExecutor.submit(new Task(blocked, completed));
                        allSubmitted.signalAll();
                    });
            }

            // Make sure all tasks have been submitted to the validation executor
            allSubmitted.await(TASK_SECONDS + 1, TimeUnit.SECONDS);

            // Give the tasks we expect to execute immediately chance to be scheduled
            Util.spinAssertEquals(2 , ((ExecutorPlus) validationExecutor)::getActiveTaskCount, 1);
            Util.spinAssertEquals(3 , ((ExecutorPlus) validationExecutor)::getPendingTaskCount, 1);

            // verify that we've reached a steady state with 2 threads actively processing and 3 queued tasks
            Assert.assertEquals(2, ((ExecutorPlus) validationExecutor).getActiveTaskCount());
            Assert.assertEquals(3, ((ExecutorPlus) validationExecutor).getPendingTaskCount());
            // allow executing tests to complete
            blocked.signalAll();
            completed.await(TASK_SECONDS + 1, TimeUnit.SECONDS);
        }
        finally
        {
            // necessary to unregister mbean
            validationExecutor.shutdownNow();
        }
    }

    @Test
    public void testRepairSessionSpaceInMiB()
    {
        ActiveRepairService activeRepairService = ActiveRepairService.instance();
        int previousSize = activeRepairService.getRepairSessionSpaceInMiB();
        try
        {
            Assert.assertEquals((Runtime.getRuntime().maxMemory() / (1024 * 1024) / 16),
                                activeRepairService.getRepairSessionSpaceInMiB());

            int targetSize = (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024) / 4) + 1;

            activeRepairService.setRepairSessionSpaceInMiB(targetSize);
            Assert.assertEquals(targetSize, activeRepairService.getRepairSessionSpaceInMiB());

            activeRepairService.setRepairSessionSpaceInMiB(10);
            Assert.assertEquals(10, activeRepairService.getRepairSessionSpaceInMiB());

            try
            {
                activeRepairService.setRepairSessionSpaceInMiB(0);
                fail("Should have received an IllegalArgumentException for depth of 0");
            }
            catch (IllegalArgumentException ignored) { }

            Assert.assertEquals(10, activeRepairService.getRepairSessionSpaceInMiB());
        }
        finally
        {
            activeRepairService.setRepairSessionSpaceInMiB(previousSize);
        }
    }

    private static class Task implements Runnable
    {
        private final Condition blocked;
        private final CountDownLatch complete;

        Task(Condition blocked, CountDownLatch complete)
        {
            this.blocked = blocked;
            this.complete = complete;
        }

        public void run()
        {
            blocked.awaitUninterruptibly(TASK_SECONDS, TimeUnit.SECONDS);
            complete.countDown();
        }
    }
}
