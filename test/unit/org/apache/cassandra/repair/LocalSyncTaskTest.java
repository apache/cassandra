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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Lists;
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
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalSyncTaskTest extends AbstractRepairTest
{
    private static final IPartitioner partitioner = Murmur3Partitioner.instance;
    public static final String KEYSPACE1 = "DifferencerTest";
    public static final String CF_STANDARD = "Standard1";
    public static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD));

        TableId tid = Schema.instance.getTableMetadata(KEYSPACE1, CF_STANDARD).id;
        cfs = Schema.instance.getColumnFamilyStoreInstance(tid);
    }

    /**
     * When there is no difference between two, LocalSyncTask should return stats with 0 difference.
     */
    @Test
    public void testNoDifference() throws Throwable
    {
        final InetAddress ep1 = InetAddress.getByName("127.0.0.1");
        final InetAddress ep2 = InetAddress.getByName("127.0.0.1");

        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

        MerkleTrees tree1 = createInitialTree(desc);

        MerkleTrees tree2 = createInitialTree(desc);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(ep1, tree1);
        TreeResponse r2 = new TreeResponse(ep2, tree2);
        LocalSyncTask task = new LocalSyncTask(desc, r1, r2, NO_PENDING_REPAIR, false, PreviewKind.NONE);
        task.run();

        assertEquals(0, task.get().numberOfDifferences);
    }

    @Test
    public void testDifference() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        UUID parentRepairSession = UUID.randomUUID();
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, FBUtilities.getBroadcastAddress(),
                                                                 Arrays.asList(cfs), Arrays.asList(range), false,
                                                                 ActiveRepairService.UNREPAIRED_SSTABLE, false,
                                                                 PreviewKind.NONE);

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), KEYSPACE1, "Standard1", Arrays.asList(range));

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
        TreeResponse r1 = new TreeResponse(InetAddress.getByName("127.0.0.1"), tree1);
        TreeResponse r2 = new TreeResponse(InetAddress.getByName("127.0.0.2"), tree2);
        LocalSyncTask task = new LocalSyncTask(desc, r1, r2, NO_PENDING_REPAIR, false, PreviewKind.NONE);
        task.run();

        // ensure that the changed range was recorded
        assertEquals("Wrong differing ranges", interesting.size(), task.getCurrentStat().numberOfDifferences);
    }

    @Test
    public void fullRepairStreamPlan() throws Exception
    {
        UUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, UUIDGen.getTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(PARTICIPANT1, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(desc, r1, r2, NO_PENDING_REPAIR, false, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan(PARTICIPANT1, PARTICIPANT2, Lists.newArrayList(RANGE1));

        assertEquals(NO_PENDING_REPAIR, plan.getPendingRepair());
        assertTrue(plan.getFlushBeforeTransfer());
    }

    @Test
    public void incrementalRepairStreamPlan() throws Exception
    {
        UUID sessionID = registerSession(cfs, true, true);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionID);
        RepairJobDesc desc = new RepairJobDesc(sessionID, UUIDGen.getTimeUUID(), KEYSPACE1, CF_STANDARD, prs.getRanges());

        TreeResponse r1 = new TreeResponse(PARTICIPANT1, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));
        TreeResponse r2 = new TreeResponse(PARTICIPANT2, createInitialTree(desc, DatabaseDescriptor.getPartitioner()));

        LocalSyncTask task = new LocalSyncTask(desc, r1, r2, desc.parentSessionId, false, PreviewKind.NONE);
        StreamPlan plan = task.createStreamPlan(PARTICIPANT1, PARTICIPANT2, Lists.newArrayList(RANGE1));

        assertEquals(desc.parentSessionId, plan.getPendingRepair());
        assertFalse(plan.getFlushBeforeTransfer());
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc, IPartitioner partitioner)
    {
        MerkleTrees tree = new MerkleTrees(partitioner);
        tree.addMerkleTrees((int) Math.pow(2, 15), desc.ranges);
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }
        return tree;
    }

    private MerkleTrees createInitialTree(RepairJobDesc desc)
    {
        return createInitialTree(desc, partitioner);

    }
}
