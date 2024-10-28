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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.sequences.SequencesUtils;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;
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
    private static final Gen.LongGen EPOCH_GEN = rs -> rs.nextLong(0, CURRENT_EPOCH.getEpoch()) + 1;
    private static final Gen<Between> BETWEEN_GEN = rs -> {
        long a = EPOCH_GEN.nextLong(rs);
        long b = EPOCH_GEN.nextLong(rs);
        while (b == a)
            b = EPOCH_GEN.nextLong(rs);
        if (b < a)
        {
            long tmp = a;
            a = b;
            b = tmp;
        }
        return new Between(Epoch.create(a), Epoch.create(b));
    };
    private static final Gen<MetadataSnapshots> SNAPSHOTS_GEN = Gens.<MetadataSnapshots>oneOf()
                                                                    .add(i -> MetadataSnapshots.NO_OP)
                                                                    .add(i -> throwing())
                                                                    .add(rs -> rs.nextBoolean() ? withCorruptSnapshots(LATEST_SNAPSHOT_EPOCH) : withAvailableSnapshots(LATEST_SNAPSHOT_EPOCH))
                                                                    .add(rs -> {
                                                                        Epoch[] queriedEpochs = new Epoch[NUM_SNAPSHOTS];
                                                                        for (int i = 0; i < NUM_SNAPSHOTS; i++)
                                                                            queriedEpochs[i] = SequencesUtils.epoch((NUM_SNAPSHOTS - i) * SNAPSHOT_FREQUENCY);
                                                                        return rs.nextBoolean() ? withCorruptSnapshots(queriedEpochs) : withAvailableSnapshots(queriedEpochs);
                                                                    })
                                                                    .build();

    interface LogStateSUT
    {
        void cleanup() throws IOException;
        void insertRegularEntry() throws IOException;
        void snapshotMetadata() throws IOException;
        LogReader reader();
        default LogState getLogState(Epoch since)
        {
            return reader().getLogState(since);
        }

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

        @Override
        public String toString()
        {
            return (corrupt ? "Corrupted" : "") + "Snapshots{" + Arrays.toString(Stream.of(expected).mapToLong(e -> e.getEpoch()).toArray()) + '}';
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

            @Override
            public String toString()
            {
                return "Throwing";
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

    @Test
    public void getLogStateBetween()
    {
        qt().forAll(SNAPSHOTS_GEN, BETWEEN_GEN).check((snapshots, between) -> {
            LogStateSUT sut = getSystemUnderTest(snapshots);
            LogState state = sut.reader().getLogState(between.start, between.end, true);
            Assertions.assertThat(state.entries).describedAs("with and without snapshot should have the same entries").isEqualTo(sut.reader().getLogState(between.start, between.end, false).entries);
            Assertions.assertThat(state.baseState.epoch).isEqualTo(between.start);

            List<Entry> entries = state.entries;
            Assertions.assertThat(entries.size()).isEqualTo(between.end.getEpoch() - between.start.getEpoch());

            long expected = between.start.nextEpoch().getEpoch();
            for (Entry e : entries)
            {
                long actual = e.epoch.getEpoch();
                Assertions.assertThat(actual).describedAs("Unexpected epoch").isEqualTo(expected);
                expected++;
            }
        });
    }

    @Test
    public void getEntriesBetween()
    {
        qt().forAll(SNAPSHOTS_GEN, BETWEEN_GEN).check((snapshots, between) -> {
            LogStateSUT sut = getSystemUnderTest(snapshots);
            LogReader.EntryHolder entries = sut.reader().getEntries(between.start, between.end);
            Assertions.assertThat(entries.since).isEqualTo(between.start);
            Assertions.assertThat(entries.entries.size()).isEqualTo(between.end.getEpoch() - between.start.getEpoch());

            long expected = between.start.nextEpoch().getEpoch();
            for (Entry e : entries.entries)
            {
                long actual = e.epoch.getEpoch();
                Assertions.assertThat(actual).describedAs("Unexpected epoch").isEqualTo(expected);
                expected++;
            }
        });
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

    private static class Between
    {
        private final Epoch start, end;

        private Between(Epoch start, Epoch end)
        {
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Between between = (Between) o;
            return start.equals(between.start) && end.equals(between.end);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(start, end);
        }

        @Override
        public String toString()
        {
            return "Between{" +
                   "start=" + start.getEpoch() +
                   ", end=" + end.getEpoch() +
                   '}';
        }
    }
}
