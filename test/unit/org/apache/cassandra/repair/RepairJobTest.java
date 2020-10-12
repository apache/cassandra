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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncRequest;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionSummary;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.asserts.SyncTaskListAssert;

import static org.apache.cassandra.utils.asserts.SyncTaskAssert.assertThat;
import static org.apache.cassandra.utils.asserts.SyncTaskListAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class RepairJobTest
{
    private static final long TEST_TIMEOUT_S = 10;
    private static final long THREAD_TIMEOUT_MILLIS = 100;
    private static final IPartitioner PARTITIONER = ByteOrderedPartitioner.instance;
    private static final IPartitioner MURMUR3_PARTITIONER = Murmur3Partitioner.instance;
    private static final String KEYSPACE = "RepairJobTest";
    private static final String CF = "Standard1";
    private static final Object MESSAGE_LOCK = new Object();

    private static final Range<Token> RANGE_1 = range(0, 1);
    private static final Range<Token> RANGE_2 = range(2, 3);
    private static final Range<Token> RANGE_3 = range(4, 5);
    private static final RepairJobDesc JOB_DESC = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), KEYSPACE, CF, Collections.emptyList());
    private static final List<Range<Token>> FULL_RANGE = Collections.singletonList(new Range<>(MURMUR3_PARTITIONER.getMinimumToken(),
                                                                                               MURMUR3_PARTITIONER.getMaximumToken()));
    private static InetAddressAndPort addr1;
    private static InetAddressAndPort addr2;
    private static InetAddressAndPort addr3;
    private static InetAddressAndPort addr4;
    private static InetAddressAndPort addr5;
    private MeasureableRepairSession session;
    private RepairJob job;
    private RepairJobDesc sessionJobDesc;

    // So that threads actually get recycled and we can have accurate memory accounting while testing
    // memory retention from CASSANDRA-14096
    private static class MeasureableRepairSession extends RepairSession
    {
        private final List<Callable<?>> syncCompleteCallbacks = new ArrayList<>();

        public MeasureableRepairSession(UUID parentRepairSession, UUID id, CommonRange commonRange, String keyspace,
                                        RepairParallelism parallelismDegree, boolean isIncremental, boolean pullRepair,
                                        PreviewKind previewKind, boolean optimiseStreams, String... cfnames)
        {
            super(parentRepairSession, id, commonRange, keyspace, parallelismDegree, isIncremental, pullRepair, previewKind, optimiseStreams, cfnames);
        }

        protected DebuggableThreadPoolExecutor createExecutor()
        {
            DebuggableThreadPoolExecutor executor = super.createExecutor();
            executor.setKeepAliveTime(THREAD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            return executor;
        }

        @Override
        public void syncComplete(RepairJobDesc desc, SyncNodePair nodes, boolean success, List<SessionSummary> summaries)
        {
            for (Callable<?> callback : syncCompleteCallbacks)
            {
                try
                {
                    callback.call();
                }
                catch (Exception e)
                {
                    throw Throwables.cleaned(e);
                }
            }
            super.syncComplete(desc, nodes, success, summaries);
        }

        public void registerSyncCompleteCallback(Callable<?> callback)
        {
            syncCompleteCallbacks.add(callback);
        }
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
                                                                 Collections.singletonList(Keyspace.open(KEYSPACE).getColumnFamilyStore(CF)), FULL_RANGE, false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false, PreviewKind.NONE);

        this.session = new MeasureableRepairSession(parentRepairSession, UUIDGen.getTimeUUID(),
                                                    new CommonRange(neighbors, Collections.emptySet(), FULL_RANGE),
                                                    KEYSPACE, RepairParallelism.SEQUENTIAL, false, false,
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
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().inboundSink.clear();
        FBUtilities.reset();
    }

    /**
     * Ensure RepairJob issues the right messages in an end to end repair of consistent data
     */
    @Test
    public void testEndToEndNoDifferences() throws InterruptedException, ExecutionException, TimeoutException
    {
        Map<InetAddressAndPort, MerkleTrees> mockTrees = new HashMap<>();
        mockTrees.put(addr1, createInitialTree(false));
        mockTrees.put(addr2, createInitialTree(false));
        mockTrees.put(addr3, createInitialTree(false));

        List<Message<?>> observedMessages = new ArrayList<>();
        interceptRepairMessages(mockTrees, observedMessages);

        job.run();

        RepairResult result = job.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        // Since there are no differences, there should be nothing to sync.
        assertThat(result.stats).hasSize(0);

        // RepairJob should send out SNAPSHOTS -> VALIDATIONS -> done
        List<Verb> expectedTypes = new ArrayList<>();
        for (int i = 0; i < 3; i++)
            expectedTypes.add(Verb.SNAPSHOT_MSG);
        for (int i = 0; i < 3; i++)
            expectedTypes.add(Verb.VALIDATION_REQ);

        assertThat(observedMessages).extracting(Message::verb).containsExactlyElementsOf(expectedTypes);
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
        List<Message<?>> messages = new ArrayList<>();
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
        SyncTaskListAssert.assertThat(syncTasks).hasSizeLessThan(0.2 * singleTreeSize);

        // block syncComplete execution until test has verified session still retains the trees
        CompletableFuture<?> future = new CompletableFuture<>();
        session.registerSyncCompleteCallback(future::get);
        ListenableFuture<List<SyncStat>> syncResults = job.executeTasks(syncTasks);

        // Immediately following execution the internal execution queue should still retain the trees
        assertThat(ObjectSizes.measureDeep(session)).isGreaterThan(singleTreeSize);
        // unblock syncComplete callback, session should remove trees
        future.complete(null);

        // The session retains memory in the contained executor until the threads expire, so we wait for the threads
        // that ran the Tree -> SyncTask conversions to die and release the memory
        long millisUntilFreed;
        for (millisUntilFreed = 0; millisUntilFreed < TEST_TIMEOUT_S * 1000; millisUntilFreed += THREAD_TIMEOUT_MILLIS)
        {
            // The measured size of the syncingTasks, and result of the computation should be much smaller
            TimeUnit.MILLISECONDS.sleep(THREAD_TIMEOUT_MILLIS);
            if (ObjectSizes.measureDeep(session) < 0.8 * singleTreeSize)
                break;
        }

        assertThat(millisUntilFreed).isLessThan(TEST_TIMEOUT_S * 1000);

        List<SyncStat> results = syncResults.get(TEST_TIMEOUT_S, TimeUnit.SECONDS);

        assertThat(ObjectSizes.measureDeep(results)).isLessThan(Math.round(0.2 * singleTreeSize));
        assertThat(session.getSyncingTasks()).isEmpty();

        assertThat(results)
            .hasSize(2)
            .extracting(s -> s.numberOfDifferences)
            .containsOnly(1L);

        assertThat(messages)
            .hasSize(2)
            .extracting(Message::verb)
            .containsOnly(Verb.SYNC_REQ);
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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "different", RANGE_2, "same", RANGE_3, "different"),
                                                         treeResponse(addr3, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    noTransient(), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));
        assertThat(tasks).hasSize(2);

        assertThat(tasks.get(pair(addr1, addr2)))
                      .isLocal()
                      .isRequestRanges()
                      .hasTransferRanges(!pullRepair)
                      .hasRanges(RANGE_1, RANGE_3);

        assertThat(tasks.get(pair(addr2, addr3)))
            .isInstanceOf(SymmetricRemoteSyncTask.class)
            .isNotLocal()
            .hasRanges(RANGE_1, RANGE_3);

        assertThat(tasks.get(pair(addr1, addr3))).isNull();
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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "different", RANGE_2, "same", RANGE_3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr2),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertThat(tasks).hasSize(1);

        assertThat(tasks.get(pair(addr1, addr2)))
            .isLocal()
            .isRequestRanges()
            .hasTransferRanges(false)
            .hasRanges(RANGE_1, RANGE_3);
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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "different", RANGE_2, "same", RANGE_3, "different"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    transientPredicate(addr1),
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        if (pullRepair)
        {
            assertThat(tasks).isEmpty();
            return;
        }

        assertThat(tasks).hasSize(1);
        assertThat(tasks.get(pair(addr1, addr2)))
            .isLocal()
            .isNotRequestRanges()
            .hasTransferRanges(true)
            .hasRanges(RANGE_1, RANGE_3);

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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient,
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertThat(tasks).isEmpty();
    }

    @Test
    public void testCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr1, // local
                                                                                    ep -> ep.equals(addr3), // transient
                                                                                    false,
                                                                                    true,
                                                                                    PreviewKind.ALL));

        assertThat(tasks).hasSize(3);

        assertThat(tasks.get(pair(addr1, addr2)))
            .isLocal()
            .hasRanges(RANGE_1, RANGE_2, RANGE_3);
        assertThat(tasks.get(pair(addr2, addr3)))
            .isNotLocal()
            .hasRanges(RANGE_1, RANGE_2, RANGE_3);
        assertThat(tasks.get(pair(addr1, addr3)))
            .isLocal()
            .hasRanges(RANGE_1, RANGE_2, RANGE_3);
    }

    @Test
    public void testCreate5NodeStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"),
                                                         treeResponse(addr4, RANGE_1, "four", RANGE_2, "four", RANGE_3, "four"),
                                                         treeResponse(addr5, RANGE_1, "five", RANGE_2, "five", RANGE_3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
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
            assertThat(task)
                .hasLocal(pair.coordinator.equals(addr1))
                // All ranges to be synchronised
                .hasRanges(RANGE_1, RANGE_2, RANGE_3);

            boolean isRemote = !pair.coordinator.equals(addr1) && !pair.peer.equals(addr1);
            boolean involvesTransient = isTransient.test(pair.coordinator) || isTransient.test(pair.peer);

            assertThat(isRemote && involvesTransient)
                .withFailMessage("Coordinator: %s\n, Peer: %s\n", pair.coordinator, pair.peer)
                .isEqualTo(task instanceof AsymmetricRemoteSyncTask);
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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"),
                                                         treeResponse(addr4, RANGE_1, "four", RANGE_2, "four", RANGE_3, "four"),
                                                         treeResponse(addr5, RANGE_1, "five", RANGE_2, "five", RANGE_3, "five"));

        Predicate<InetAddressAndPort> isTransient = ep -> ep.equals(addr4) || ep.equals(addr5);
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    local, // local
                                                                                    isTransient, // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertThat(tasks).hasSize(9);
        for (InetAddressAndPort addr : new InetAddressAndPort[]{ addr1, addr2, addr3 })
        {
            if (local.equals(addr))
                continue;

            assertThat(tasks.get(pair(local, addr)))
                .isRequestRanges()
                .hasTransferRanges(!pullRepair);
        }

        assertThat(tasks.get(pair(local, addr4)))
            .isRequestRanges()
            .hasTransferRanges(false);

        assertThat(tasks.get(pair(local, addr5)))
            .isRequestRanges()
            .hasTransferRanges(false);
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
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"),
                                                         treeResponse(addr4, RANGE_1, "four", RANGE_2, "four", RANGE_3, "four"),
                                                         treeResponse(addr5, RANGE_1, "five", RANGE_2, "five", RANGE_3, "five"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createStandardSyncTasks(JOB_DESC,
                                                                                    treeResponses,
                                                                                    addr4, // local
                                                                                    ep -> ep.equals(addr4) || ep.equals(addr5), // transient
                                                                                    false,
                                                                                    pullRepair,
                                                                                    PreviewKind.ALL));

        assertThat(tasks.get(pair(addr4, addr5))).isNull();
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksAllDifferent()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one", RANGE_3, "one"),
                                                         treeResponse(addr2, RANGE_1, "two", RANGE_2, "two", RANGE_3, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "three", RANGE_3, "three"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(JOB_DESC,
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
            assertThat(tasks.get(pair)).hasRanges(RANGE_1, RANGE_2, RANGE_3);
        }
    }

    @Test
    public void testOptimizedCreateStandardSyncTasks()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "one", RANGE_2, "one"),
                                                         treeResponse(addr2, RANGE_1, "one", RANGE_2, "two"),
                                                         treeResponse(addr3, RANGE_1, "three", RANGE_2, "two"));

        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(JOB_DESC,
                                                                                            treeResponses,
                                                                                            addr4, // local
                                                                                            noTransient(),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        assertThat(tasks.values()).areAllInstanceOf(AsymmetricRemoteSyncTask.class);

        assertThat(tasks.get(pair(addr1, addr3)).rangesToSync).containsExactly(RANGE_1);
        // addr1 can get range2 from either addr2 or addr3 but not from both
        assertStreamRangeFromEither(tasks, RANGE_2, addr1, addr2, addr3);

        assertThat(tasks.get(pair(addr2, addr3)).rangesToSync).containsExactly(RANGE_1);
        assertThat(tasks.get(pair(addr2, addr1)).rangesToSync).containsExactly(RANGE_2);

        // addr3 can get range1 from either addr1 or addr2 but not from both
        assertStreamRangeFromEither(tasks, RANGE_1, addr3, addr2, addr1);

        assertThat(tasks.get(pair(addr3, addr1)).rangesToSync).containsExactly(RANGE_2);
    }

    @Test
    public void testOptimizedCreateStandardSyncTasksWithTransient()
    {
        List<TreeResponse> treeResponses = Arrays.asList(treeResponse(addr1, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"),
                                                         treeResponse(addr2, RANGE_1, "different", RANGE_2, "same", RANGE_3, "different"),
                                                         treeResponse(addr3, RANGE_1, "same", RANGE_2, "same", RANGE_3, "same"));

        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), "ks", "cf", Collections.emptyList());
        Map<SyncNodePair, SyncTask> tasks = toMap(RepairJob.createOptimisedSyncingSyncTasks(desc,
                                                                                            treeResponses,
                                                                                            addr1, // local
                                                                                            ep -> ep.equals(addr3),
                                                                                            addr -> "DC1",
                                                                                            false,
                                                                                            PreviewKind.ALL));

        assertThat(tasks).hasSize(3);
        SyncTask task = tasks.get(pair(addr1, addr2));

        assertThat(task)
            .isLocal()
            .hasRanges(RANGE_1, RANGE_3)
            .isRequestRanges()
            .hasTransferRanges(false);

        assertStreamRangeFromEither(tasks, RANGE_3, addr2, addr1, addr3);
        assertStreamRangeFromEither(tasks, RANGE_1, addr2, addr1, addr3);
    }

    // Asserts that ranges are streamed from one of the nodes but not from the both
    public static void assertStreamRangeFromEither(Map<SyncNodePair, SyncTask> tasks, Range<Token> range,
                                                   InetAddressAndPort target, InetAddressAndPort either, InetAddressAndPort or)
    {
        InetAddressAndPort streamsFrom;
        InetAddressAndPort doesntStreamFrom;
        if (tasks.containsKey(pair(target, either)) && tasks.get(pair(target, either)).rangesToSync.contains(range))
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
        assertThat(task).isInstanceOf(AsymmetricRemoteSyncTask.class);
        assertThat(task.rangesToSync).containsOnly(range);
        assertDoesntStreamRangeFrom(range, tasks.get(pair(target, doesntStreamFrom)));
    }

    public static void assertDoesntStreamRangeFrom(Range<Token> range, SyncTask task)
    {
        if (task == null)
            return; // Doesn't stream anything

        assertThat(task.rangesToSync).doesNotContain(range);
    }

    private static Token tk(int i)
    {
        return PARTITIONER.getToken(ByteBufferUtil.bytes(i));
    }

    private static Range<Token> range(int from, int to)
    {
        return new Range<>(tk(from), tk(to));
    }

    private static TreeResponse treeResponse(InetAddressAndPort addr, Object... rangesAndHashes)
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

    private static SyncNodePair pair(InetAddressAndPort node1, InetAddressAndPort node2)
    {
        return new SyncNodePair(node1, node2);
    }

    public static Map<SyncNodePair, SyncTask> toMap(List<SyncTask> tasks)
    {
        ImmutableMap.Builder<SyncNodePair, SyncTask> map = ImmutableMap.builder();
        tasks.forEach(t -> map.put(t.nodePair, t));
        return map.build();
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
        tree.addMerkleTrees((int) Math.pow(2, 15), FULL_RANGE);
        tree.init();

        if (invalidate)
        {
            // change a range in one of the trees
            Token token = MURMUR3_PARTITIONER.midpoint(FULL_RANGE.get(0).left, FULL_RANGE.get(0).right);
            tree.invalidate(token);
            tree.get(token).hash("non-empty hash!".getBytes());
        }

        return tree;
    }

    private void interceptRepairMessages(Map<InetAddressAndPort, MerkleTrees> mockTrees,
                                         List<Message<?>> messageCapture)
    {
        MessagingService.instance().inboundSink.add(message -> message.verb().isResponse());
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (message == null || !(message.payload instanceof RepairMessage))
                return false;

            // So different Thread's messages don't overwrite each other.
            synchronized (MESSAGE_LOCK)
            {
                messageCapture.add(message);
            }

            switch (message.verb())
            {
                case SNAPSHOT_MSG:
                    MessagingService.instance().callbacks.removeAndRespond(message.id(), to, message.emptyResponse());
                    break;
                case VALIDATION_REQ:
                    session.validationComplete(sessionJobDesc, to, mockTrees.get(to));
                    break;
                case SYNC_REQ:
                    SyncRequest syncRequest = (SyncRequest) message.payload;
                    session.syncComplete(sessionJobDesc, new SyncNodePair(syncRequest.src, syncRequest.dst),
                                         true, Collections.emptyList());
                    break;
                default:
                    break;
            }
            return false;
        });
    }
}
