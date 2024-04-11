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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.sequences.SequencesUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LogStateTestBase
{
    static int SNAPSHOT_FREQUENCY = 5;
    static int NUM_SNAPSHOTS = 10;
    static int EXTRA_ENTRIES = 2;
    static Epoch CURRENT_EPOCH = Epoch.create((NUM_SNAPSHOTS * SNAPSHOT_FREQUENCY) + EXTRA_ENTRIES);
    static Epoch LATEST_SNAPSHOT_EPOCH = Epoch.create(NUM_SNAPSHOTS * SNAPSHOT_FREQUENCY);

    interface LogStateSUT
    {
        void cleanup() throws IOException;
        void insertRegularEntry() throws IOException;
        void snapshotMetadata() throws IOException;
        LogState getLogState(Epoch since);

        // just for manually checking the test data
        void dumpTables() throws IOException;
    }

    abstract LogStateSUT getSystemUnderTest(MetadataSnapshots snapshots);

    @Before
    public void initEntries() throws IOException
    {
        LogStateSUT sut = getSystemUnderTest(MetadataSnapshots.NO_OP);
        sut.cleanup();
        for (long i = 0; i < NUM_SNAPSHOTS; i++)
        {
            // for the very first snapshot we must write 1 fewer entries
            // as the pre-init entry is automatically inserted with Epoch.FIRST when the table is empty
            int entriesPerSnapshot = SNAPSHOT_FREQUENCY - (i == 0 ? 2 : 1);
            for (int j = 0; j < entriesPerSnapshot; j++)
                sut.insertRegularEntry();

            sut.snapshotMetadata();
        }

        for (int i = 0; i < 2; i++)
            sut.insertRegularEntry();

        sut.dumpTables();
    }

    static class TestSnapshots extends MetadataSnapshots.NoOp
    {

        Epoch[] expected;
        int idx;
        boolean corrupt;
        TestSnapshots(Epoch[] expected, boolean corrupt)
        {
            this.expected = expected;
            this.idx = 0;
            this.corrupt = corrupt;
        }

        @Override
        public ClusterMetadata getSnapshot(Epoch since)
        {
            if (idx >= expected.length)
                throw new AssertionError("Should not have gotten a query for "+since);
            assertEquals(expected[idx++], since);
            return corrupt ? null : ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance).forceEpoch(since);
        }

        @Override
        public List<Epoch> listSnapshotsSince(Epoch epoch)
        {
            List<Epoch> list = new ArrayList<>();
            for (Epoch e : expected)
                if (e.isAfter(epoch))
                    list.add(e);

            return list;
        }

    };

    static MetadataSnapshots withCorruptSnapshots(Epoch ... expected)
    {
        return new TestSnapshots(expected, true);
    }

    static MetadataSnapshots withAvailableSnapshots(Epoch ... expected)
    {
        return new TestSnapshots(expected, false);
    }

    static MetadataSnapshots throwing()
    {
        return new MetadataSnapshots.NoOp()
        {
            @Override
            public ClusterMetadata getSnapshot(Epoch epoch)
            {
                fail("Did not expect to request a snapshot");
                return null;
            }
        };
    }

    @Test
    public void sinceIsEmptyWithCorruptSnapshots()
    {
        Epoch [] queriedEpochs = new Epoch[NUM_SNAPSHOTS];
        for (int i = 0; i < NUM_SNAPSHOTS; i++)
            queriedEpochs[i] = SequencesUtils.epoch((NUM_SNAPSHOTS - i) * SNAPSHOT_FREQUENCY);
        MetadataSnapshots missingSnapshot = withCorruptSnapshots(queriedEpochs);

        LogState state = getSystemUnderTest(missingSnapshot).getLogState(Epoch.EMPTY);
        assertNull(state.baseState);
        assertEntries(state.entries, Epoch.FIRST, CURRENT_EPOCH);
    }

    @Test
    public void sinceIsEmptyWithValidSnapshots()
    {
        MetadataSnapshots withSnapshots = withAvailableSnapshots(LATEST_SNAPSHOT_EPOCH,
                                                                 SequencesUtils.epoch(((NUM_SNAPSHOTS - 1) * SNAPSHOT_FREQUENCY)),
                                                                 SequencesUtils.epoch(((NUM_SNAPSHOTS - 2) * SNAPSHOT_FREQUENCY)));
        LogState state = getSystemUnderTest(withSnapshots).getLogState(Epoch.EMPTY);
        assertEquals(LATEST_SNAPSHOT_EPOCH, state.baseState.epoch);
        assertEntries(state.entries, LATEST_SNAPSHOT_EPOCH.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsBeforeLastSnapshotWithCorruptSnapshot()
    {
        MetadataSnapshots missingSnapshot = withCorruptSnapshots(LATEST_SNAPSHOT_EPOCH,
                                                                 SequencesUtils.epoch(((NUM_SNAPSHOTS - 1) * SNAPSHOT_FREQUENCY)),
                                                                 SequencesUtils.epoch(((NUM_SNAPSHOTS - 2) * SNAPSHOT_FREQUENCY)));
        // an arbitrary epoch earlier than the last snapshot
        Epoch since = SequencesUtils.epoch(((NUM_SNAPSHOTS - 3) * SNAPSHOT_FREQUENCY) + 2);
        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsBeforeLastSnapshotWithValidSnapshot()
    {
        MetadataSnapshots withSnapshot = withAvailableSnapshots(LATEST_SNAPSHOT_EPOCH);
        // an arbitrary epoch earlier than the last snapshot
        Epoch since = SequencesUtils.epoch(((NUM_SNAPSHOTS - 3) * SNAPSHOT_FREQUENCY) + 2);
        LogState state = getSystemUnderTest(withSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsEqualLastSnapshotWithValidSnapshot()
    {
        // the max epoch in the last snapshot (but not the current highest epoch)
        final Epoch since = LATEST_SNAPSHOT_EPOCH;
        MetadataSnapshots withSnapshot = withAvailableSnapshots(since);
        LogState state = getSystemUnderTest(withSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsEqualLastSnapshotWithCorruptSnapshot()
    {
        // the max epoch in the last snapshot (but not the current highest epoch)
        final Epoch since = LATEST_SNAPSHOT_EPOCH;
        MetadataSnapshots missingSnapshot = withCorruptSnapshots(since);
        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsAfterLastSnapshot()
    {
        MetadataSnapshots snapshots = throwing();
        // an arbitrary epoch later than the last snapshot (but not the current highest epoch)
        Epoch since = Epoch.create(CURRENT_EPOCH.getEpoch() - 1);
        LogState state = getSystemUnderTest(snapshots).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsMaxAfterLastSnapshot()
    {
        MetadataSnapshots snapshots = throwing();
        // the current highest epoch, which is > the epoch of the last snapshot
        Epoch since = CURRENT_EPOCH;
        LogState state = getSystemUnderTest(snapshots).getLogState(since);
        assertNull(state.baseState);
        assertTrue(state.entries.isEmpty());
    }

    @Test
    public void sinceArbitraryEpochWithMultipleCorruptSnapshots()
    {
        Epoch since = Epoch.create(35);
        Epoch expected = LATEST_SNAPSHOT_EPOCH;
        MetadataSnapshots missingSnapshot = withCorruptSnapshots(expected,                                     // 50
                                                                 Epoch.create(expected.getEpoch() - SNAPSHOT_FREQUENCY),        // 45
                                                                 Epoch.create(expected.getEpoch() - SNAPSHOT_FREQUENCY * 2L));   // 40

        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertEntries(state.entries, since.nextEpoch(), CURRENT_EPOCH);
    }

    private void assertEntries(List<Entry> entries, Epoch min, Epoch max)
    {
        int idx = 0;
        for (long i = min.getEpoch(); i <= max.getEpoch(); i++)
        {
            Entry e = entries.get(idx);
            assertEquals(e.epoch.getEpoch(), i);
            idx++;
        }
        assertEquals(idx, entries.size());
    }
}
