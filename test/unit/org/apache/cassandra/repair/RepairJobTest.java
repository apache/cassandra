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

package org.apache.cassandra.repair;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class RepairJobTest
{
    private static final long TEST_TIMEOUT_S = 10;
    private static final long THREAD_TIMEOUT_MILLIS = 100;
    private static final IPartitioner PARTITIONER = ByteOrderedPartitioner.instance;
    private static final IPartitioner MURMUR3_PARTITIONER = Murmur3Partitioner.instance;
    private static final String KEYSPACE = "RepairJobTest";
    private static final String CF = "Standard1";
    private static final Object messageLock = new Object();

    private static final Range<Token> range1 = range(0, 1);
    private static final Range<Token> range2 = range(2, 3);
    private static final Range<Token> range3 = range(4, 5);
    private static final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), KEYSPACE, CF, Arrays.asList());
    private static final List<Range<Token>> fullRange = Collections.singletonList(new Range<>(MURMUR3_PARTITIONER.getMinimumToken(),
                                                                                              MURMUR3_PARTITIONER.getMaximumToken()));
    private static InetAddressAndPort addr1;
    private static InetAddressAndPort addr2;
    private static InetAddressAndPort addr3;
    private static InetAddressAndPort addr4;
    private static InetAddressAndPort addr5;
    private RepairSession session;
    private RepairJob job;
    private RepairJobDesc sessionJobDesc;

    // So that threads actually get recycled and we can have accurate memory accounting while testing
    // memory retention from CASSANDRA-14096
    private static class MeasureableRepairSession extends RepairSession
    {
        public MeasureableRepairSession(UUID parentRepairSession, UUID id, CommonRange commonRange, String keyspace,
                                        RepairParallelism parallelismDegree, boolean isIncremental, boolean pullRepair,
                                        boolean force, PreviewKind previewKind, boolean optimiseStreams, String... cfnames)
        {
            super(parentRepairSession, id, commonRange, keyspace, parallelismDegree, isIncremental, pullRepair, force, previewKind, optimiseStreams, cfnames);
        }

        protected DebuggableThreadPoolExecutor createExecutor()
        {
            DebuggableThreadPoolExecutor executor = super.createExecutor();
            executor.setKeepAliveTime(THREAD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return executor;        }
    }
    @BeforeClass
    public static void setupClass() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF));
        addr1 = InetAddressAndPort.getByName("127.0.0.1");
        addr2 = InetAddressAndPort.getByName("127.0.0.2");
        addr3 = InetAddressAndPort.getByName("127.0.0.3");
        addr4 = InetAddressAndPort.getByName("127.0.0.4");
        addr5 = InetAddressAndPort.getByName("127.0.0.5");
    }

    @Before
    public void setup()
    {
        Set<InetAddressAndPort> neighbors = new HashSet<>(Arrays.asList(addr2, addr3));

        UUID parentRepairSession = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddressAndPort(),
                                                                 Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)), fullRange, false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false, PreviewKind.NONE);

        this.session = new MeasureableRepairSession(parentRepairSession, UUIDGen.getTimeUUID(),
                                                    new CommonRange(neighbors, Collections.emptySet(), fullRange),
                                                    KEYSPACE, RepairParallelism.SEQUENTIAL,
                                                    false, false, false,
                                                    PreviewKind.NONE, false, CF);

        this.job = new RepairJob(session, CF);
        this.sessionJobDesc = new RepairJobDesc(session.parentRepairSession, session.getId(),
                                                session.keyspace, CF, session.ranges());

        FBUtilities.setBroadcastInetAddress(addr1.address);
    }

    @After
    public void reset()
    {
        ActiveRepairService.instance.terminateSessions();
        MessagingService.instance().clearMessageSinks();
        FBUtilities.reset();
    }

    /**
     * Ensure RepairJob issues the right messages in an end to end repair of consistent data
     */
    @Test
    public void testEndToEndNoDifferences() throws InterruptedException, ExecutionException, TimeoutException
    {
        Map<InetAddressAndPort, MerkleTrees> mockTrees = new HashMap<>();
        mockTrees.put(FBUtilities.getBroadcastAddressAndPort(), createInitialTree(false));
        mockTrees.put(addr2, createInitialTree(false));
        mockTrees.put(addr3, createInitialTree(false));

        List<MessageOut> observedMessages = new ArrayList<>();
        interceptRepairMessages(mockTrees, observedMessages);

        job.run();

        RepairResult result = job.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        // Since there are no differences, there should be nothing to sync.
        assertEquals(0, result.stats.size());

        // RepairJob should send out SNAPSHOTS -> VALIDATIONS -> done
        List<RepairMessage.Type> expectedTypes = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            expectedTypes.add(RepairMessage.Type.SNAPSHOT);
        for (int i = 0; i < 3; i++)
            expectedTypes.add(RepairMessage.Type.VALIDATION_REQUEST);

        assertEquals(expectedTypes, observedMessages.stream()
                                                    .map(k -> ((RepairMessage) k.payload).messageType)
                                                    .collect(Collectors.toList()));
    }

    /**
     * Regression test for CASSANDRA-14096. We should not retain memory in the RepairSession once the
     * ValidationTask -> SyncTask transform is done.
     */
    @Test
    public void testNoTreesRetainedAfterDifference() throws Throwable
    {
        Map<InetAddressAndPort, MerkleTrees> mockTrees = new HashMap<>();
        mockTrees.put(addr1, createInitialTree(true));
        mockTrees.put(addr2, createInitialTree(false));
        mockTrees.put(addr3, createInitialTree(false));

        List<TreeResponse> mockTreeResponses = mockTrees.entrySet().stream()
                                                        .map(e -> new TreeResponse(e.getKey(), e.getValue()))
                                                        .collect(Collectors.toList());
        List<MessageOut> messages = new ArrayList<>();
        interceptRepairMessages(mockTrees, messages);

        long singleTreeSize = ObjectSizes.measureDeep(mockTrees.get(addr1));

        // Use addr4 instead of one of the provided trees to force everything to be remote sync tasks as
        // LocalSyncTasks try to reach over the network.
        List<SyncTask> syncTasks = RepairJob.createStandardSyncTasks(sessionJobDesc, mockTreeResponses,
                                                                     addr4, // local
                                                                     noTransient(),
                                                                     session.isIncremental,
                                                                     session.pullRepair,
                                                                     session.previewKind);

        // SyncTasks themselves should not contain significant memory
        assertTrue(ObjectSizes.measureDeep(syncTasks) < 0.2 * singleTreeSize);

        ListenableFuture<List<SyncStat>> syncResults = job.executeTasks(syncTasks);

        // Immediately following execution the internal execution queue should still retain the trees
        assertTrue(ObjectSizes.measureDeep(session) > singleTreeSize);

        // The session retains memory in the contained executor until the threads expire, so we wait for the threads
        // that ran the Tree -> SyncTask conversions to die and release the memory
        int millisUntilFreed;
        for (millisUntilFreed = 0; millisUntilFreed < TEST_TIMEOUT_S * 1000; millisUntilFreed += THREAD_TIMEOUT_MILLIS)
        {
            // The measured size of the syncingTasks, and result of the computation should be much smaller
            TimeUnit.MILLISECONDS.sleep(THREAD_TIMEOUT_MILLIS);
            if (ObjectSizes.measureDeep(session) < 0.8 * singleTreeSize)
                break;
        }

        assertTrue(millisUntilFreed < TEST_TIMEOUT_S * 1000);

        List<SyncStat> results = syncResults.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        assertTrue(ObjectSizes.measureDeep(results) < 0.2 * singleTreeSize);

        assertEquals(2, results.size());
        assertEquals(0, session.getSyncingTasks().size());
        assertTrue(results.stream().allMatch(s -> s.numberOfDifferences == 1));

        assertEquals(2, messages.size());
        assertTrue(messages.stream().allMatch(m -> ((RepairMessage) m.payload).messageType == RepairMessage.Type.SYNC_REQUEST));
    }

    @Test
    public void testCreateStandardSyncTasks()
    {
        testCreateStandardSyncTasks(false);
    }

    @Test
    public void testCreateStandardSyncTasksPullRepair()
    {
        testCreateStandardSyncTasks(true);
    }

    public static void testCreateStandardSyncTasks(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same",      range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"),
                                                         treeResponse(addr3, range1, "same",      range2, "same", range3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    noTransient(), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(2, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertTrue(((LocalSyncTask) task).requestRanges);
        Assert.assertEquals(!pullRepair, ((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);

        task = tasks.get(pair(addr2, addr3));
        Assert.assertFalse(task.isLocal());
        Assert.assertTrue(task instanceof SymmetricRemoteSyncTask);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);

        Assert.assertNull(tasks.get(pair(addr1, addr3)));
    }

    @Test
    public void testStandardSyncTransient()
    {
        // Do not stream towards transient nodes
        testStandardSyncTransient(true);
        testStandardSyncTransient(false);
    }

    public void testStandardSyncTransient(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr2),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(1, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertTrue(((LocalSyncTask) task).requestRanges);
        Assert.assertFalse(((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);
    }

    @Test
    public void testStandardSyncLocalTransient()
    {
        // Do not stream towards transient nodes
        testStandardSyncLocalTransient(true);
        testStandardSyncLocalTransient(false);
    }

    public void testStandardSyncLocalTransient(boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr1),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        if (pullRepair)
        {
            Assert.assertTrue(tasks.isEmpty());
            return;
        }

        Assert.assertEquals(1, tasks.size());

        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertFalse(((LocalSyncTask) task).requestRanges);
        Assert.assertTrue(((LocalSyncTask) task).transferRanges);
        Assert.assertEquals(Arrays.asList(range1, range3), task.rangesToSync);
    }

    @Test
    public void testEmptyDifference()
    {
        // one of the nodes is a local coordinator
        testEmptyDifference(addr1, noTransient(), true);
        testEmptyDifference(addr1, noTransient(), false);
        testEmptyDifference(addr2, noTransient(), true);
        testEmptyDifference(addr2, noTransient(), false);
        testEmptyDifference(addr1, transientPredicate(addr1), true);
        testEmptyDifference(addr2, transientPredicate(addr1), true);
        testEmptyDifference(addr1, transientPredicate(addr1), false);
        testEmptyDifference(addr2, transientPredicate(addr1), false);
        testEmptyDifference(addr1, transientPredicate(addr2), true);
        testEmptyDifference(addr2, transientPredicate(addr2), true);
        testEmptyDifference(addr1, transientPredicate(addr2), false);
        testEmptyDifference(addr2, transientPredicate(addr2), false);

        // nonlocal coordinator
        testEmptyDifference(addr3, noTransient(), true);
        testEmptyDifference(addr3, noTransient(), false);
        testEmptyDifference(addr3, noTransient(), true);
        testEmptyDifference(addr3, noTransient(), false);
        testEmptyDifference(addr3, transientPredicate(addr1), true);
        testEmptyDifference(addr3, transientPredicate(addr1), true);
        testEmptyDifference(addr3, transientPredicate(addr1), false);
        testEmptyDifference(addr3, transientPredicate(addr1), false);
        testEmptyDifference(addr3, transientPredicate(addr2), true);
        testEmptyDifference(addr3, transientPredicate(addr2), true);
        testEmptyDifference(addr3, transientPredicate(addr2), false);
        testEmptyDifference(addr3, transientPredicate(addr2), false);
    }

    public void testEmptyDifference(InetAddressAndPort local, Predicate<InetAddressAndPort> isTransient, boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same", range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "same", range2, "same", range3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient,
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        Assert.assertTrue(tasks.isEmpty());
    }

    @Test
    public void testCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    ep -> ep.equals(addr3), // transient
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        Assert.assertEquals(3, tasks.size());
        SyncTask task = tasks.get(pair(addr1, addr2));
        Assert.assertTrue(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);

        task = tasks.get(pair(addr2, addr3));
        Assert.assertFalse(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);

        task = tasks.get(pair(addr1, addr3));
        Assert.assertTrue(task.isLocal());
        Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);
    }

    @Test
    public void testCreate5NodeStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four",  range2, "four",  range3, "four"),
                                                         treeResponse(addr5, range1, "five",  range2, "five",  range3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    isTransient, // transient
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        SyncNodePair[] pairs = new SyncNodePair[] {pair(addr1, addr2),
                                                   pair(addr1, addr3),
                                                   pair(addr1, addr4),
                                                   pair(addr1, addr5),
                                                   pair(addr2, addr4),
                                                   pair(addr2, addr4),
                                                   pair(addr2, addr5),
                                                   pair(addr3, addr4),
                                                   pair(addr3, addr5)};

        for (SyncNodePair pair : pairs)
        {
            SyncTask task = tasks.get(pair);
            // Local only if addr1 is a coordinator
            assertEquals(task.isLocal(), pair.coordinator.equals(addr1));

            boolean isRemote = !pair.coordinator.equals(addr1) && !pair.peer.equals(addr1);
            boolean involvesTransient = isTransient.test(pair.coordinator) || isTransient.test(pair.peer);
            assertEquals(String.format("Coordinator: %s\n, Peer: %s\n",pair.coordinator, pair.peer),
                         isRemote && involvesTransient,
                         task instanceof AsymmetricRemoteSyncTask);

            // All ranges to be synchronised
            Assert.assertEquals(Arrays.asList(range1, range2, range3), task.rangesToSync);
        }
    }

    @Test
    public void testLocalSyncWithTransient()
    {
        for (InetAddressAndPort local : new InetAddressAndPort[]{ addr1, addr2, addr3 })
        {
            FBUtilities.reset();
            FBUtilities.setBroadcastInetAddress(local.address);
            testLocalSyncWithTransient(local, false);
        }
    }

    @Test
    public void testLocalSyncWithTransientPullRepair()
    {
        for (InetAddressAndPort local : new InetAddressAndPort[]{ addr1, addr2, addr3 })
        {
            FBUtilities.reset();
            FBUtilities.setBroadcastInetAddress(local.address);
            testLocalSyncWithTransient(local, true);
        }
    }

    public static void testLocalSyncWithTransient(InetAddressAndPort local, boolean pullRepair)
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four",  range2, "four",  range3, "four"),
                                                         treeResponse(addr5, range1, "five",  range2, "five",  range3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient, // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertEquals(9, tasks.size());
        for (InetAddressAndPort addr : new InetAddressAndPort[]{ addr1, addr2, addr3 })
        {
            if (local.equals(addr))
                continue;

            LocalSyncTask task = (LocalSyncTask) tasks.get(pair(local, addr));
            assertTrue(task.requestRanges);
            assertEquals(!pullRepair, task.transferRanges);
        }

        LocalSyncTask task = (LocalSyncTask) tasks.get(pair(local, addr4));
        assertTrue(task.requestRanges);
        assertFalse(task.transferRanges);

        task = (LocalSyncTask) tasks.get(pair(local, addr5));
        assertTrue(task.requestRanges);
        assertFalse(task.transferRanges);
    }

    @Test
    public void testLocalAndRemoteTransient()
    {
        testLocalAndRemoteTransient(false);
    }

    @Test
    public void testLocalAndRemoteTransientPullRepair()
    {
        testLocalAndRemoteTransient(true);
    }

    private static void testLocalAndRemoteTransient(boolean pullRepair)
    {
        FBUtilities.setBroadcastInetAddress(addr4.address);
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one", range2, "one", range3, "one"),
                                                         treeResponse(addr2, range1, "two", range2, "two", range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"),
                                                         treeResponse(addr4, range1, "four", range2, "four", range3, "four"),
                                                         treeResponse(addr5, range1, "five", range2, "five", range3, "five"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(desc,
                                                                                    treeResponses,
                                                                                    addr4, // local
                                                                                    ep -> ep.equals(addr4) || ep.equals(addr5), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertNull(tasks.get(pair(addr4, addr5)));
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one",   range3, "one"),
                                                         treeResponse(addr2, range1, "two",   range2, "two",   range3, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "three", range3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            noTransient(),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        for (SyncNodePair pair : new SyncNodePair[]{ pair(addr1, addr2),
                                                     pair(addr1, addr3),
                                                     pair(addr2, addr1),
                                                     pair(addr2, addr3),
                                                     pair(addr3, addr1),
                                                     pair(addr3, addr2) })
        {
            assertEquals(Arrays.asList(range1, range2, range3), tasks.get(pair).rangesToSync);
        }
    }

    @Test
    public void testOptimizedCreateStandardSyncTasks()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "one",   range2, "one"),
                                                         treeResponse(addr2, range1, "one",   range2, "two"),
                                                         treeResponse(addr3, range1, "three", range2, "two"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr4, // local
                                                                                            noTransient(),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        for (SyncTask task : tasks.values())
            assertTrue(task instanceof AsymmetricRemoteSyncTask);

        assertEquals(Arrays.asList(range1), tasks.get(pair(addr1, addr3)).rangesToSync);
        // addr1 can get range2 from either addr2 or addr3 but not from both
        assertStreamRangeFromEither(tasks, Arrays.asList(range2),
                                    addr1, addr2, addr3);

        assertEquals(Arrays.asList(range1), tasks.get(pair(addr2, addr3)).rangesToSync);
        assertEquals(Arrays.asList(range2), tasks.get(pair(addr2, addr1)).rangesToSync);

        // addr3 can get range1 from either addr1 or addr2 but not from both
        assertStreamRangeFromEither(tasks, Arrays.asList(range1),
                                    addr3, addr2, addr1);
        assertEquals(Arrays.asList(range2), tasks.get(pair(addr3, addr1)).rangesToSync);
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, range1, "same",      range2, "same", range3, "same"),
                                                         treeResponse(addr2, range1, "different", range2, "same", range3, "different"),
                                                         treeResponse(addr3, range1, "same",      range2, "same", range3, "same"));

        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), "ks", "cf", Arrays.asList());
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            ep -> ep.equals(addr3),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        assertEquals(3, tasks.size());
        SyncTask task = tasks.get(pair(addr1, addr2));
        assertTrue(task.isLocal());
        assertElementEquals(Arrays.asList(range1, range3), task.rangesToSync);
        assertTrue(((LocalSyncTask)task).requestRanges);
        assertFalse(((LocalSyncTask)task).transferRanges);

        assertStreamRangeFromEither(tasks, Arrays.asList(range3),
                                    addr2, addr1, addr3);

        assertStreamRangeFromEither(tasks, Arrays.asList(range1),
                                    addr2, addr1, addr3);
    }

    // Asserts that ranges are streamed from one of the nodes but not from the both
    public static void assertStreamRangeFromEither(Map<SyncNodePair, SyncTask> tasks, List<Range<Token>> ranges,
                                                   InetAddressAndPort target, InetAddressAndPort either, InetAddressAndPort or)
    {
        InetAddressAndPort streamsFrom;
        InetAddressAndPort doesntStreamFrom;
        if (tasks.containsKey(pair(target, either)) && tasks.get(pair(target, either)).rangesToSync.equals(ranges))
        {
            streamsFrom = either;
            doesntStreamFrom = or;
        }
        else
        {
            doesntStreamFrom = either;
            streamsFrom = or;
        }

        SyncTask task = tasks.get(pair(target, streamsFrom));
        assertTrue(task instanceof AsymmetricRemoteSyncTask);
        assertElementEquals(ranges, task.rangesToSync);
        assertDoesntStreamRangeFrom(tasks, ranges, target, doesntStreamFrom);
    }

    public static void assertDoesntStreamRangeFrom(Map<SyncNodePair, SyncTask> tasks, List<Range<Token>> ranges,
                                                   InetAddressAndPort target, InetAddressAndPort source)
    {
        Set<Range<Token>> rangeSet = new HashSet<>(ranges);
        SyncTask task = tasks.get(pair(target, source));
        if (task == null)
            return; // Doesn't stream anything

        for (Range<Token> range : task.rangesToSync)
        {
            assertFalse(String.format("%s shouldn't stream %s from %s",
                                      target, range, source),
                        rangeSet.contains(range));
        }
    }

    public static <T> void assertElementEquals(Collection<T> col1, Collection<T> col2)
    {
        Set<T> set1 = new HashSet<>(col1);
        Set<T> set2 = new HashSet<>(col2);
        Set<T> difference = Sets.difference(set1, set2);
        assertTrue("Expected empty difference but got: " + difference.toString(),
                   difference.isEmpty());
    }

    public static Token tk(int i)
    {
        return PARTITIONER.getToken(ByteBufferUtil.bytes(i));
    }

    public static Range<Token> range(int from, int to)
    {
        return new Range<>(tk(from), tk(to));
    }

    public static TreeResponse treeResponse(InetAddressAndPort addr, Object... rangesAndHashes)
    {
        MerkleTrees trees = new MerkleTrees(PARTITIONER);
        for (int i = 0; i < rangesAndHashes.length; i += 2)
        {
            Range<Token> range = (Range<Token>) rangesAndHashes[i];
            String hash = (String) rangesAndHashes[i + 1];
            MerkleTree tree = trees.addMerkleTree(2, MerkleTree.RECOMMENDED_DEPTH, range);
            tree.get(range.left).hash(hash.getBytes());
        }

        return new TreeResponse(addr, trees);
    }

    public static SyncNodePair pair(InetAddressAndPort node1, InetAddressAndPort node2)
    {
        return new SyncNodePair(node1, node2);
    }

    public static Map<SyncNodePair, SyncTask> toMap(List<SyncTask> tasks)
    {
        Map<SyncNodePair, SyncTask> map = new HashMap();
        for (SyncTask task : tasks)
        {
            SyncTask oldTask = map.put(task.nodePair, task);
            Assert.assertNull(String.format("\nNode pair: %s\nOld task:  %s\nNew task:  %s\n",
                                            task.nodePair,
                                            oldTask,
                                            task),
                              oldTask);
        }
        return map;
    }

    public static Predicate<InetAddressAndPort> transientPredicate(InetAddressAndPort... transientNodes)
    {
        Set<InetAddressAndPort> set = new HashSet<>();
        for (InetAddressAndPort node : transientNodes)
            set.add(node);

        return set::contains;
    }

    public static Predicate<InetAddressAndPort> noTransient()
    {
        return node -> false;
    }

    private MerkleTrees createInitialTree(boolean invalidate)
    {
        MerkleTrees tree = new MerkleTrees(MURMUR3_PARTITIONER);
        tree.addMerkleTrees((int) Math.pow(2, 15), fullRange);
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }

        if (invalidate)
        {
            // change a range in one of the trees
            Token token = MURMUR3_PARTITIONER.midpoint(fullRange.get(0).left, fullRange.get(0).right);
            tree.invalidate(token);
            tree.get(token).hash("non-empty hash!".getBytes());
        }

        return tree;
    }

    private void interceptRepairMessages(Map<InetAddressAndPort, MerkleTrees> mockTrees,
                                         List<MessageOut> messageCapture)
    {
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
            {
                if (message == null || !(message.payload instanceof RepairMessage))
                    return false;

                // So different Thread's messages don't overwrite each other.
                synchronized (messageLock)
                {
                    messageCapture.add(message);
                }

                RepairMessage rm = (RepairMessage) message.payload;
                switch (rm.messageType)
                {
                    case SNAPSHOT:
                        MessageIn<?> messageIn = MessageIn.create(to, null,
                                                                  Collections.emptyMap(),
                                                                  MessagingService.Verb.REQUEST_RESPONSE,
                                                                  MessagingService.current_version);
                        MessagingService.instance().receive(messageIn, id);
                        break;
                    case VALIDATION_REQUEST:
                        session.validationComplete(sessionJobDesc, to, mockTrees.get(to));
                        break;
                    case SYNC_REQUEST:
                        SyncRequest syncRequest = (SyncRequest) rm;
                        session.syncComplete(sessionJobDesc, new SyncNodePair(syncRequest.src, syncRequest.dst),
                                             true, Collections.emptyList());
                        break;
                    default:
                        break;
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return message.verb == MessagingService.Verb.REQUEST_RESPONSE;
            }
        });
    }
}
