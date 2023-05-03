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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosRepairHistory;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CassandraRelevantProperties.FORCE_PAXOS_STATE_REBUILD;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosStateTracker.stateDirectory;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;
import static org.apache.cassandra.service.paxos.uncommitted.UncommittedTableDataTest.assertIteratorContents;
import static org.apache.cassandra.service.paxos.uncommitted.UncommittedTableDataTest.uncommitted;


public class PaxosStateTrackerTest
{
    private File directory1 = null;
    private File directory2 = null;
    private File[] directories = null;
    protected static String ks;
    protected static TableMetadata cfm1;
    protected static TableMetadata cfm2;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();

        ks = "coordinatorsessiontest";
        cfm1 = TableMetadata.builder(ks, "tbl1").addPartitionKeyColumn("k", Int32Type.instance).addRegularColumn("v", Int32Type.instance).build();
        cfm2 = TableMetadata.builder(ks, "tbl2").addPartitionKeyColumn("k", Int32Type.instance).addRegularColumn("v", Int32Type.instance).build();
        SchemaLoader.createKeyspace(ks, KeyspaceParams.simple(1), cfm1, cfm2);
    }

    @Before
    public void setUp() throws Exception
    {
        PAXOS_CFS.truncateBlocking();
        PAXOS_REPAIR_CFS.truncateBlocking();

        if (directory1 != null)
            FileUtils.deleteRecursive(directory1);
        if (directory2 != null)
            FileUtils.deleteRecursive(directory2);

        directory1 = new File(Files.createTempDir());
        directory2 = new File(Files.createTempDir());
        directories = new File[]{directory1, directory2};
    }

    private static PartitionUpdate update(TableMetadata cfm, int k, Ballot ballot)
    {
        ColumnMetadata col = cfm.getColumn(new ColumnIdentifier("v", false));
        Cell cell = BufferCell.live(col, ballot.unixMicros(), ByteBufferUtil.bytes(0));
        Row row = BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        return PartitionUpdate.singleRowUpdate(cfm, dk(k), row);
    }

    private static Commit commit(TableMetadata cfm, int k, Ballot ballot)
    {
        return new Commit(ballot, update(cfm, k, ballot));
    }

    private static void savePaxosRepair(TableMetadata cfm, Range<Token> range, Ballot lowBound)
    {
        PaxosRepairHistory current = SystemKeyspace.loadPaxosRepairHistory(cfm.keyspace, cfm.name);
        PaxosRepairHistory updated = PaxosRepairHistory.add(current, Collections.singleton(range), lowBound);
        SystemKeyspace.savePaxosRepairHistory(cfm.keyspace, cfm.name, updated, true);
    }

    private static void savePaxosRepair(TableMetadata cfm, int left, int right, Ballot lowBound)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Range<Token> range = new Range<>(partitioner.getToken(ByteBufferUtil.bytes(left)), partitioner.getToken(ByteBufferUtil.bytes(right)));
        savePaxosRepair(cfm, range, lowBound);
    }

    private static void initDirectory(File directory) throws IOException
    {
        PaxosStateTracker.create(new File[]{directory}).ballots().flush();
    }

    @Test
    public void autoRebuild() throws Throwable
    {
        Ballot[] ballots = createBallots(6);

        // save a promise, proposal, and commit to each table
        SystemKeyspace.savePaxosWritePromise(dk(0), cfm1, ballots[0]);
        SystemKeyspace.savePaxosWritePromise(dk(1), cfm2, ballots[1]);
        SystemKeyspace.savePaxosProposal(commit(cfm1, 2, ballots[2]));
        SystemKeyspace.savePaxosProposal(commit(cfm2, 3, ballots[3]));
        SystemKeyspace.savePaxosCommit(commit(cfm1, 4, ballots[4]));
        SystemKeyspace.savePaxosCommit(commit(cfm2, 5, ballots[5]));

        PaxosStateTracker tracker = PaxosStateTracker.create(directories);
        Assert.assertTrue(tracker.isRebuildNeeded());
        Assert.assertEquals(Sets.newHashSet(), tracker.uncommitted().tableIds());

        tracker.maybeRebuild();

        Assert.assertEquals(stateDirectory(directory1), tracker.uncommitted().getDirectory());

        Assert.assertEquals(Sets.newHashSet(cfm1.id, cfm2.id), tracker.uncommitted().tableIds());

        UncommittedTableData tableData1 = tracker.uncommitted().getTableState(cfm1.id);
        assertIteratorContents(tableData1.iterator(ALL_RANGES),  kl(uncommitted(0, ballots[0]),
                                                                    uncommitted(2, ballots[2])));

        UncommittedTableData tableData2 = tracker.uncommitted().getTableState(cfm2.id);
        assertIteratorContents(tableData2.iterator(ALL_RANGES),  kl(uncommitted(1, ballots[1]),
                                                                    uncommitted(3, ballots[3])));
    }

    @Test
    public void manualRebuild() throws Throwable
    {
        initDirectory(directory1);
        {
            PaxosStateTracker tracker = PaxosStateTracker.create(directories);
            Assert.assertFalse(tracker.isRebuildNeeded());
            Assert.assertEquals(Ballot.none(), tracker.ballots().getLowBound());
        }

        Ballot[] ballots = createBallots(4);
        savePaxosRepair(cfm1, 0, 10, ballots[0]);
        savePaxosRepair(cfm1, 10, 20, ballots[1]);
        SystemKeyspace.savePaxosWritePromise(dk(0), cfm1, ballots[2]);
        SystemKeyspace.savePaxosProposal(commit(cfm1, 2, ballots[3]));

        try (WithProperties with = new WithProperties().set(FORCE_PAXOS_STATE_REBUILD, true))
        {
            PaxosStateTracker tracker = PaxosStateTracker.create(directories);
            Assert.assertTrue(tracker.isRebuildNeeded());
            Assert.assertEquals(Ballot.none(), tracker.ballots().getLowBound());
            tracker.maybeRebuild();

            UncommittedTableData tableData1 = tracker.uncommitted().getTableState(cfm1.id);
            assertIteratorContents(tableData1.iterator(ALL_RANGES),  kl(uncommitted(0, ballots[2]),
                                                                        uncommitted(2, ballots[3])));
            Assert.assertEquals(ballots[1], tracker.ballots().getLowBound());
            Assert.assertEquals(ballots[3], tracker.ballots().getHighBound());
        }
    }

    // test we can find paxos data in any directory
    @Test
    public void testMultiDirectories() throws Throwable
    {
        initDirectory(directory2);
        PaxosStateTracker tracker = PaxosStateTracker.create(directories);
        Assert.assertFalse(tracker.isRebuildNeeded());
        Assert.assertEquals(stateDirectory(directory2), tracker.uncommitted().getDirectory());
    }

    // test multiple paxos directories throws exception
    @Test(expected=IllegalStateException.class)
    public void testConflictingDirectories() throws Throwable
    {
        initDirectory(directory1);
        initDirectory(directory2);
        PaxosStateTracker tracker = PaxosStateTracker.create(directories);
    }
}
