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

import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTests.*;

public class PaxosUncommittedTrackerTest
{
    private static final String KS = "ks";
    private static final String TBL = "tbl";
    private static TableId cfid;
    private File directory = null;
    private PaxosUncommittedTracker tracker;
    private PaxosMockUpdateSupplier updates;
    private UncommittedTableData state;

    @BeforeClass
    public static void setupClass()
    {
        SchemaLoader.prepareServer();
        TableMetadata tableMetadata = TableMetadata.builder("ks", "tbl")
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .addRegularColumn("v", Int32Type.instance)
                                                   .build();
        cfid = tableMetadata.id;
        SchemaLoader.createKeyspace(KS, KeyspaceParams.simple(1), tableMetadata);
    }

    @Before
    public void setUp()
    {
        if (directory != null)
            FileUtils.deleteRecursive(directory);

        directory = new File(Files.createTempDir());

        tracker = new PaxosUncommittedTracker(directory);
        tracker.start();
        updates = new PaxosMockUpdateSupplier();
        PaxosUncommittedTracker.unsafSetUpdateSupplier(updates);
        state = tracker.getOrCreateTableState(cfid);
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker, Collection<Range<Token>> ranges)
    {
        try (CloseableIterator<UncommittedPaxosKey> iterator = tracker.uncommittedKeyIterator(cfid, ranges))
        {
            return Lists.newArrayList(iterator);
        }
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker, Range<Token> range)
    {
        return uncommittedList(tracker, Collections.singleton(range));
    }

    private static List<UncommittedPaxosKey> uncommittedList(PaxosUncommittedTracker tracker)
    {
        return uncommittedList(tracker, FULL_RANGE);
    }

    @Test
    public void inmemory() throws Exception
    {
        Assert.assertEquals(0, state.numFiles());
        int size = 5;
        List<PaxosKeyState> expected = new ArrayList<>(size);
        int key = 0;
        for (Ballot ballot : createBallots(size))
        {
            DecoratedKey dk = dk(key++);
            updates.inProgress(cfid, dk, ballot);
            expected.add(new PaxosKeyState(cfid, dk, ballot, false));
        }

        Assert.assertEquals(expected, uncommittedList(tracker));
        Assert.assertEquals(0, state.numFiles());
    }

    @Test
    public void onDisk() throws Exception
    {
        Assert.assertEquals(0, state.numFiles());
        int size = 5;
        List<PaxosKeyState> expected = new ArrayList<>(size);
        int key = 0;
        for (Ballot ballot : createBallots(size))
        {
            DecoratedKey dk = dk(key++);
            updates.inProgress(cfid, dk, ballot);
            expected.add(new PaxosKeyState(cfid, dk, ballot, false));
        }
        tracker.flushUpdates(null);

        Assert.assertEquals(expected, uncommittedList(tracker));

        Assert.assertEquals(1, state.numFiles());
        Assert.assertEquals(expected, kl(state.iterator(Collections.singleton(FULL_RANGE))));
    }

    @Test
    public void mixed() throws Exception
    {
        Assert.assertEquals(0, state.numFiles());
        int size = 10;
        PaxosKeyState[] expectedArr = new PaxosKeyState[size];
        List<PaxosKeyState> inMemory = new ArrayList<>(size / 2);
        List<PaxosKeyState> onDisk = new ArrayList<>(size / 2);
        Ballot[] ballots = createBallots(size);

        for (int i=0; i<size; i+=2)
        {
            Ballot ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(cfid, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(cfid, dk, ballot, false);;
            onDisk.add(ballotState);
            expectedArr[i] = ballotState;
        }

        tracker.flushUpdates(null);

        for (int i=1; i<size; i+=2)
        {
            Ballot ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(cfid, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(cfid, dk, ballot, false);;
            inMemory.add(ballotState);
            expectedArr[i] = ballotState;
        }

        List<PaxosKeyState> expected = kl(expectedArr);

        Assert.assertEquals(expected, uncommittedList(tracker));

        Assert.assertEquals(1, state.numFiles());
        Assert.assertEquals(onDisk, kl(state.iterator(Collections.singleton(FULL_RANGE))));
    }

    @Test
    public void committed()
    {
        UncommittedTableData tableData = UncommittedTableData.load(directory, cfid);
        Assert.assertEquals(0, tableData.numFiles());
        Ballot ballot = createBallots(1)[0];

        DecoratedKey dk = dk(1);
        updates.inProgress(cfid, dk, ballot);

        Assert.assertEquals(kl(new PaxosKeyState(cfid, dk, ballot, false)), uncommittedList(tracker));

        updates.committed(cfid, dk, ballot);
        Assert.assertTrue(uncommittedList(tracker).isEmpty());
    }

    /**
     * Test that commits don't resolve in progress transactions with more recent ballots
     */
    @Test
    public void pastCommit()
    {
        Ballot[] ballots = createBallots(2);
        DecoratedKey dk = dk(1);
        Assert.assertTrue(ballots[1].uuidTimestamp() > ballots[0].uuidTimestamp());

        updates.inProgress(cfid, dk, ballots[1]);
        updates.committed(cfid, dk, ballots[0]);

        Assert.assertEquals(kl(new PaxosKeyState(cfid, dk, ballots[1], false)), uncommittedList(tracker));
    }

    @Test
    public void tokenRange() throws Exception
    {
        Assert.assertEquals(0, state.numFiles());
        int size = 10;
        PaxosKeyState[] expectedArr = new PaxosKeyState[size];
        Ballot[] ballots = createBallots(size);

        for (int i=0; i<size; i+=2)
        {
            Ballot ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(cfid, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(cfid, dk, ballot, false);;
            expectedArr[i] = ballotState;
        }

        tracker.flushUpdates(null);

        for (int i=1; i<size; i+=2)
        {
            Ballot ballot = ballots[i];
            DecoratedKey dk = dk(i);
            updates.inProgress(cfid, dk, ballot);
            PaxosKeyState ballotState = new PaxosKeyState(cfid, dk, ballot, false);;
            expectedArr[i] = ballotState;
        }

        List<PaxosKeyState> expected = kl(expectedArr);

        Assert.assertEquals(expected.subList(0, 5), uncommittedList(tracker, r(null, tk(4))));
        Assert.assertEquals(expected.subList(3, 7), uncommittedList(tracker, r(2, 6)));
        Assert.assertEquals(expected.subList(8, 10), uncommittedList(tracker, r(tk(7), null)));
        Assert.assertEquals(Lists.newArrayList(Iterables.concat(expected.subList(1, 5), expected.subList(6, 9))),
                                             uncommittedList(tracker, Lists.newArrayList(r(0, 4), r(5, 8))));
    }
}
