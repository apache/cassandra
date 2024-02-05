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
import java.util.List;
import java.util.Objects;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LogStateTestBase
{
    static int PERIOD_SIZE = 5;
    static int NUM_PERIODS = 10;
    static int EXTRA_ENTRIES = 2;
    static int CURRENT_EPOCH = (NUM_PERIODS * PERIOD_SIZE) + EXTRA_ENTRIES;
    static Sealed REAL_LAST_SEALED = new Sealed(NUM_PERIODS, Epoch.create(NUM_PERIODS * PERIOD_SIZE));

    interface LogStateSUT
    {
        void cleanup() throws IOException;
        void insertRegularEntry() throws IOException;
        void sealPeriod() throws IOException;
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
        for (long i = 0; i < NUM_PERIODS; i++)
        {
            // for the very first period (partition in the log table) we must write 1 fewer entries
            // as the pre-init entry is automatically inserted with Epoch.FIRST when the table is empty
            int entriesPerPeriod = PERIOD_SIZE - (i == 0 ? 2 : 1);
            for (int j = 0; j < entriesPerPeriod; j++)
                sut.insertRegularEntry();

            sut.sealPeriod();
        }

        // Add 2 more Entries, which will be after the last sealed period. The point is to test what happens when
        // we have to use the period to epoch reverse index, and the epochs are beyond the max indexed
        for (int i = 0; i < 2; i++)
            sut.insertRegularEntry();

        sut.dumpTables();
    }

    static class TestSnapshots extends MetadataSnapshots.SystemKeyspaceMetadataSnapshots {};

    static MetadataSnapshots withMissingSnapshot(Epoch ... expected)
    {
        return new TestSnapshots()
        {
            int idx = 0;
            @Override
            public ClusterMetadata getSnapshot(Epoch since)
            {
                if (idx >= expected.length)
                    throw new AssertionError("Should not have gotten a query for "+since);
                assertEquals(expected[idx++], since);
                return null;
            }
        };
    }

    static MetadataSnapshots withAvailableSnapshot(Epoch expected)
    {
        return new TestSnapshots()
        {
            @Override
            public ClusterMetadata getSnapshot(Epoch since)
            {
                assertEquals(expected, since);
                return ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance).forceEpoch(expected);
            }
        };
    }

    static MetadataSnapshots throwing()
    {
        return new TestSnapshots()
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
    public void sinceIsEmptyWithMissingSnapshot()
    {
        Epoch [] queriedEpochs = new Epoch[NUM_PERIODS];
        for (int i = 0; i < NUM_PERIODS; i++)
            queriedEpochs[i] = Epoch.create((REAL_LAST_SEALED.period - i) * PERIOD_SIZE);
        MetadataSnapshots missingSnapshot = withMissingSnapshot(queriedEpochs);

        LogState state = getSystemUnderTest(missingSnapshot).getLogState(Epoch.EMPTY);
        assertNull(state.baseState);
        assertReplication(state.entries, 1, CURRENT_EPOCH);
    }

    @Test
    public void sinceIsEmptyWithAvailableSnapshot()
    {
        final Epoch expected = REAL_LAST_SEALED.epoch;
        MetadataSnapshots withSnapshot = withAvailableSnapshot(expected);
        LogState state = getSystemUnderTest(withSnapshot).getLogState(Epoch.EMPTY);
        assertEquals(expected, state.baseState.epoch);
        assertReplication(state.entries, expected.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsBeforeLastSealedButMissingSnapshot()
    {
        MetadataSnapshots missingSnapshot = withMissingSnapshot(REAL_LAST_SEALED.epoch,
                                                                Epoch.create(((REAL_LAST_SEALED.period - 1) * PERIOD_SIZE )),
                                                                Epoch.create(((REAL_LAST_SEALED.period - 2) * PERIOD_SIZE )));
        // an arbitrary epoch earlier than the last sealed
        Epoch since = Epoch.create(((REAL_LAST_SEALED.period - 3) * PERIOD_SIZE ) + 2);
        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertReplication(state.entries, since.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsBeforeLastSealedWithSnapshot()
    {
        final Epoch expected = REAL_LAST_SEALED.epoch;
        MetadataSnapshots withSnapshot = withAvailableSnapshot(expected);
        // an arbitrary epoch earlier than the last sealed
        Epoch since = Epoch.create(((REAL_LAST_SEALED.period - 3) * PERIOD_SIZE ) + 2);
        LogState state = getSystemUnderTest(withSnapshot).getLogState(since);
        assertEquals(expected, state.baseState.epoch);
        assertReplication(state.entries, expected.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsMaxInLastSealedWithSnapshot()
    {
        // the max epoch in the last sealed period(but not the current highest epoch)
        final Epoch since = REAL_LAST_SEALED.epoch;
        MetadataSnapshots withSnapshot = withAvailableSnapshot(since);
        LogState state = getSystemUnderTest(withSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertReplication(state.entries, since.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsMaxInLastSealedButMissingSnapshot()
    {
        // the max epoch in the last sealed period(but not the current highest epoch)
        final Epoch since = REAL_LAST_SEALED.epoch;
        MetadataSnapshots missingSnapshot = withMissingSnapshot(since);
        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertReplication(state.entries, since.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsAfterLastSealed()
    {
        MetadataSnapshots snapshots = throwing();
        // an arbitrary epoch later than the last sealed (but not the current highest epoch)
        Epoch since = Epoch.create(CURRENT_EPOCH - 1);
        LogState state = getSystemUnderTest(snapshots).getLogState(since);
        assertNull(state.baseState);
        assertReplication(state.entries, since.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    @Test
    public void sinceIsMaxAfterLastSealed()
    {
        MetadataSnapshots snapshots = throwing();
        // the current highest epoch, which > the max epoch in the last sealed period
        Epoch since = Epoch.create(CURRENT_EPOCH);
        LogState state = getSystemUnderTest(snapshots).getLogState(since);
        assertNull(state.baseState);
        assertTrue(state.entries.isEmpty());
    }

    @Test
    public void sinceArbitraryEpochWithSealedButMissingSnapshot()
    {
        Epoch since = Epoch.create(35);
        Epoch expected = REAL_LAST_SEALED.epoch;
        MetadataSnapshots missingSnapshot = withMissingSnapshot(expected,                                     // 50
                                                                Epoch.create(expected.getEpoch() - PERIOD_SIZE),        // 45
                                                                Epoch.create(expected.getEpoch() - PERIOD_SIZE * 2L));   // 40

        LogState state = getSystemUnderTest(missingSnapshot).getLogState(since);
        assertNull(state.baseState);
        assertReplication(state.entries, since.nextEpoch().getEpoch(), CURRENT_EPOCH);
    }

    private void assertReplication(List<Entry> entries, long min, long max)
    {
        int idx = 0;
        for (long i = min; i <= max; i++)
        {
            Entry e = entries.get(idx);
            assertEquals(e.epoch.getEpoch(), i);
            idx++;
        }
        assertEquals(idx, entries.size());
    }


    public static class Sealed implements Comparable<Sealed>
    {
        public static final Sealed EMPTY = new Sealed(Period.EMPTY, Epoch.EMPTY);
        public final long period;
        public final Epoch epoch;

        public Sealed(long period, Epoch epoch)
        {
            this.period = period;
            this.epoch = epoch;
        }

        @Override
        public String toString()
        {
            return "Sealed{" +
                   "period=" + period +
                   ", epoch=" + epoch +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Sealed)) return false;
            Sealed sealed = (Sealed) o;
            return period == sealed.period && epoch.equals(sealed.epoch);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(period, epoch);
        }

        @Override
        public int compareTo(Sealed o)
        {
            return Long.compare(this.epoch.getEpoch(), o.epoch.getEpoch());
        }
    }
}
