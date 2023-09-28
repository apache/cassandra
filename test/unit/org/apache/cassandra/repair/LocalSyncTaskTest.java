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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalSyncTaskTest extends AbstractRepairTest
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    private static final InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
    public static final String KEYSPACE1 = "DifferencerTest";
    public static final String CF_STANDARD = "Standard1";
    public static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));

        TableId tid = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD).id;
        cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
    }

    /**
     * When there is no difference between two, SymmetricLocalSyncTask should return stats with 0 difference.
     */
    @Test
    public void testNoDifference() throws Throwable
    {
        final InetAddressAndPort ep2 = InetAddressAndPort.getByName("127.0.0.2");

        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        RepairJobDesc desc = new RepairJobDesc(nextTimeUUID(), nextTimeUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);

        MerkleTrees tree2 = createInitialTree(desc);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(local, tree1);
        TreeResponse r2 = new TreeResponse(ep2, tree2);
        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               NO_PENDING_REPAIR, true, true, PreviewKind.NONE);
        task.run();

        assertTrue(task.stat.differences.isEmpty());
    }

    @Test
    public void testDifference() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        TimeUUID parentRepairSession = nextTimeUUID();
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        ActiveRepairService.instance().registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddressAndPort(),
                                                                   Arrays.asList(cfs), Arrays.asList(range), false,
                                                                   ActiveRepairService.UNREPAIRED_SSTABLE, false,
                                                                   PreviewKind.NONE);

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, nextTimeUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);
        MerkleTrees tree2 = createInitialTree(desc);

        // change a range in one of the trees
        Token token = partitioner.midpoint(range.left, range.right);
        tree1.invalidate(token);
        MerkleTree.TreeRange changed = tree1.get(token);
        changed.hash("non-empty hash!".getBytes());

        Set<Range<Token>> interesting = new HashSet<>();
        interesting.add(changed);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(local, tree1);
        TreeResponse r2 = new TreeResponse(InetAddressAndPort.getByName("127.0.0.2"), tree2);
        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               NO_PENDING_REPAIR, true, true, PreviewKind.NONE);
        NettyStreamingConnectionFactory.MAX_CONNECT_ATTEMPTS = 1;
        try
        {
            task.run();
        }
        finally
        {
            NettyStreamingConnectionFactory.MAX_CONNECT_ATTEMPTS = 3;
        }

        // ensure that the changed range was recorded
        assertEquals("Wrong differing ranges", interesting.size(), task.stat.differences.size());
    }

    @Test
    public void fullRepairStreamPlan() throws Exception
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance().getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, nextTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(local, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               NO_PENDING_REPAIR, true, true, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan();

        assertEquals(NO_PENDING_REPAIR, plan.getPendingRepair());
        assertTrue(plan.getFlushBeforeTransfer());
    }

    private static void assertNumInOut(StreamPlan plan, int expectedIncoming, int expectedOutgoing)
    {
        StreamCoordinator coordinator = plan.getCoordinator();
        StreamSession session = Iterables.getOnlyElement(coordinator.getAllStreamSessions());
        assertEquals(expectedIncoming, session.getNumRequests());
        assertEquals(expectedOutgoing, session.getNumTransfers());
    }

    @Test
    public void incrementalRepairStreamPlan() throws Exception
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance().getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, nextTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(local, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               desc.parentSessionId, true, true, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan();

        assertEquals(desc.parentSessionId, plan.getPendingRepair());
        assertFalse(plan.getFlushBeforeTransfer());
        assertNumInOut(plan, 1, 1);
    }

    /**
     * Don't reciprocate streams if the other endpoint is a transient replica
     */
    @Test
    public void transientRemoteStreamPlan() throws NoSuchRepairSessionException
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance().getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, nextTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(local, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               desc.parentSessionId, true, false, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan();
        assertNumInOut(plan, 1, 0);
    }

    /**
     * Don't request streams if the other endpoint is a transient replica
     */
    @Test
    public void transientLocalStreamPlan() throws NoSuchRepairSessionException
    {
        TimeUUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance().getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, nextTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(local, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(SharedContext.Global.instance, desc, r1.endpoint, r2.endpoint, MerkleTrees.difference(r1.trees, r2.trees),
                                               desc.parentSessionId, false, true, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan();
        assertNumInOut(plan, 0, 1);
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc, IPartitioner partitioner)
    {
        MerkleTrees trees = new MerkleTrees(partitioner);
        trees.addMerkleTrees((int) Math.pow(2, 15), desc.ranges);
        trees.init();
        return trees;
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc)
    {
        return createInitialTree(desc, partitioner);

    }
}
