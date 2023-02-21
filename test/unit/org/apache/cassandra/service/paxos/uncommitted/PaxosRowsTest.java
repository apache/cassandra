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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.PAXOS_CFM;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.PAXOS_CFS;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.createBallots;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.dk;

public class PaxosRowsTest
{
    protected static String ks;
    protected static final String tbl = "tbl";
    protected static TableMetadata metadata;
    protected static TableId tableId;

    static Commit emptyCommitFor(Ballot ballot, DecoratedKey key)
    {
        return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, key));
    }

    static Commit nonEmptyCommitFor(Ballot ballot, DecoratedKey key)
    {
        return new Commit(ballot, nonEmptyUpdate(ballot, metadata, key));
    }

    static PartitionUpdate nonEmptyUpdate(Ballot ballot, TableMetadata cfm, DecoratedKey key)
    {
        ColumnMetadata valueColumn = cfm.getColumn(new ColumnIdentifier("v", false));
        return PartitionUpdate.singleRowUpdate(cfm, key, BTreeRow.create(Clustering.EMPTY, LivenessInfo.EMPTY, Row.Deletion.LIVE, BTree.singleton(new BufferCell(valueColumn, ballot.unixMicros(), Cell.NO_TTL, Cell.NO_DELETION_TIME, ByteBufferUtil.bytes(1), null))));
    }

    static Row paxosRowFor(DecoratedKey key)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(PAXOS_CFM,
                FBUtilities.nowInSeconds(),
                key,
                new BufferClustering(UUIDType.instance.decompose(tableId.asUUID())));
        try (ReadExecutionController opGroup = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(opGroup);
             UnfilteredRowIterator partition = Iterators.getOnlyElement(iterator))
        {
            return (Row) Iterators.getOnlyElement(partition);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        SchemaLoader.prepareServer();

        ks = "coordinatorsessiontest";
        metadata = CreateTableStatement.parse("CREATE TABLE tbl (k INT PRIMARY KEY, v INT)", ks).build();
        tableId = metadata.id;
    }

    @Before
    public void setUp() throws Exception
    {
        PAXOS_CFS.truncateBlocking();
    }

    @Test
    public void testRowInterpretation()
    {
        DecoratedKey key = dk(5);
        Ballot[] ballots = createBallots(3);

        SystemKeyspace.savePaxosWritePromise(key, metadata, ballots[0]);
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[0], false), PaxosRows.getCommitState(key, paxosRowFor(key), null));
        SystemKeyspace.savePaxosProposal(emptyCommitFor(ballots[0], key));
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[0], true), PaxosRows.getCommitState(key, paxosRowFor(key), null));

        SystemKeyspace.savePaxosWritePromise(key, metadata, ballots[1]);
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[1], false), PaxosRows.getCommitState(key, paxosRowFor(key), null));
        SystemKeyspace.savePaxosProposal(nonEmptyCommitFor(ballots[1], key));
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[1], false), PaxosRows.getCommitState(key, paxosRowFor(key), null));
        SystemKeyspace.savePaxosCommit(nonEmptyCommitFor(ballots[1], key));
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[1], true), PaxosRows.getCommitState(key, paxosRowFor(key), null));

        // test cfid filter mismatch
        Assert.assertNull(PaxosRows.getCommitState(key, paxosRowFor(key), TableId.fromUUID(UUID.randomUUID())));

        SystemKeyspace.savePaxosCommit(emptyCommitFor(ballots[2], key));
        Assert.assertEquals(new PaxosKeyState(tableId, key, ballots[2], true), PaxosRows.getCommitState(key, paxosRowFor(key), null));
    }

    @Test
    public void testIterator()
    {
        Ballot[] ballots = createBallots(10);
        List<PaxosKeyState> expected = new ArrayList<>(ballots.length);
        for (int i=0; i<ballots.length; i++)
        {
            Ballot ballot = ballots[i];
            DecoratedKey key = dk(i);

            if (i%2 == 0)
            {
                SystemKeyspace.savePaxosProposal(nonEmptyCommitFor(ballot, key));
                expected.add(new PaxosKeyState(tableId, key, ballot, false));
            }
            else
            {
                SystemKeyspace.savePaxosCommit(nonEmptyCommitFor(ballot, key));
                expected.add(new PaxosKeyState(tableId, key, ballot, true));
            }
        }

        PartitionRangeReadCommand command = PartitionRangeReadCommand.allDataRead(PAXOS_CFM, FBUtilities.nowInSeconds());
        try (ReadExecutionController opGroup = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(opGroup);
             CloseableIterator<PaxosKeyState> iterator = PaxosRows.toIterator(partitions, metadata.id, true))
        {
            Assert.assertEquals(expected, Lists.newArrayList(iterator));
        }
    }
}
