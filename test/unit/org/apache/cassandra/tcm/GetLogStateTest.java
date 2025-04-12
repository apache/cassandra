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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.listeners.SchemaListener;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.log.LogStorage;
import org.apache.cassandra.tcm.log.SystemKeyspaceStorage;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.psjava.util.AssertStatus.assertTrue;

public class GetLogStateTest
{
    Epoch maxEpoch = Epoch.EMPTY;
    NavigableMap<Epoch, ClusterMetadata> epochToSnapshot = new TreeMap<>();
    @BeforeClass
    public static void setup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.cleanupAndLeaveDirs();
        CommitLog.instance.start();
        LogStorage logStorage = LogStorage.SystemKeyspace;
        MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
        ClusterMetadata initial = new ClusterMetadata(DatabaseDescriptor.getPartitioner());
        LocalLog.LogSpec logSpec = LocalLog.logSpec()
                                           .sync()
                                           .withInitialState(initial)
                                           .withStorage(logStorage)
                                           .withLogListener(new MetadataSnapshotListener())
                                           .withListener(new SchemaListener(true));
        LocalLog log = logSpec.createLog();
        ClusterMetadataService cms = new ClusterMetadataService(new UniformRangePlacement(),
                                                                snapshots,
                                                                log,
                                                                new AtomicLongBackedProcessor(log),
                                                                Commit.Replicator.NO_OP,
                                                                false);
        ClusterMetadataService.unsetInstance();
        ClusterMetadataService.setInstance(cms);
        log.readyUnchecked();
        log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void testGetLogState()
    {
        clearAndPopulate();
        // Starting at the current epoch, iterate backwards. For each preceding epoch, fetch and verify a LogState
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(maxEpoch);
        assertEquals(logState, LogState.EMPTY);
        testGetLogStateHelper();
    }

    @Test
    public void testIncompleteLog1()
    {
        clearAndPopulate();
        // delete an entry from the log which precedes the latest snapshot but is after toQuery. In this case we would
        // prefer a simple list of entries, but will get the latest snapshot + subsequent entries instead.
        Epoch lastSnapshot = epochToSnapshot.lastKey();
        Epoch toDelete = Epoch.create(lastSnapshot.getEpoch() - 2);
        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, epochToSnapshot.lastEntry().getValue(), null, maxEpoch);
    }

    @Test
    public void testIncompleteLog2()
    {
        clearAndPopulate();
        // delete an entry from the log with multiple snapshots between toQuery and current. The deleted entry is one
        // which comes after all available snapshots. Without losing a log entry, we should expect a LogState containing
        // the most recent snapshot plus subsequent entries.
        ClusterMetadata lastSnapshot = epochToSnapshot.lastEntry().getValue();
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        for (int i=0; i<3; i++)
            snapshots.next();
        Epoch toQuery = Epoch.create(snapshots.next().getEpoch() - 1);
        Epoch toDelete = Epoch.create(maxEpoch.getEpoch() - 2);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);

        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertEquals(lastSnapshot, logState.baseState);
        boolean containsExpectedEntries = logState.entries.stream()
                                                          .map(entry -> entry.epoch.getEpoch())
                                                          .allMatch(e -> e > lastSnapshot.epoch.getEpoch()
                                                                         && e <= maxEpoch.getEpoch()
                                                                         && e != toDelete.getEpoch());
        assertTrue(containsExpectedEntries);
    }

    @Test
    public void testIncompleteLog3()
    {
        clearAndPopulate();
        // delete an entry from the log where we have multiple snapshots after the deleted entry. In this case we should
        // get the latest snapshot + subsequent entries and the deleted entry should not affect this.
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        ClusterMetadata lastSnapshot = epochToSnapshot.get(snapshots.next());
        // Jump back over a couple more snapshots to give us points to query for and delete
        snapshots.next();
        Epoch e = snapshots.next();
        Epoch toQuery = Epoch.create(e.getEpoch() + 1);
        Epoch toDelete = Epoch.create(toQuery.getEpoch() + 1);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);
    }

    @Test
    public void testIncompleteLog4()
    {
        clearAndPopulate();
        // delete an entry from the log which comes after the most recent snapshot, which itself comes after toQuery.
        // We would normally expect to skip over the intervening snapshot and return just a list of entries from toQuery
        // to current. Because we cannot make that list continuous, instead we get the most recent snapshot and an
        // incomplete list.
        ClusterMetadata lastSnapshot = epochToSnapshot.lastEntry().getValue();
        Epoch toQuery = Epoch.create(lastSnapshot.epoch.getEpoch() - 2);
        Epoch toDelete = Epoch.create(maxEpoch.getEpoch() - 1);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertEquals(lastSnapshot, logState.baseState);
        boolean containsExpectedEntries = logState.entries.stream()
                                                          .map(entry -> entry.epoch.getEpoch())
                                                          .allMatch(e -> e > lastSnapshot.epoch.getEpoch()
                                                                         && e <= maxEpoch.getEpoch()
                                                                         && e != toDelete.getEpoch());
        assertTrue(containsExpectedEntries);
    }

    @Test
    public void testCorruptSnapshot1()
    {
        clearAndPopulate();
        // with a single snapshot following toQuery, corrupt it so that it cannot be read. In this scenario, we already
        // prefer to return a list of consecutive entries and no base snapshot, so the corruption should not matter.
        Epoch lastSnapshot = epochToSnapshot.lastKey();

        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);

        QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", lastSnapshot.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
    }

    @Test
    public void testCorruptSnapshot2()
    {
        clearAndPopulate();
        // with a multiple snapshots following toQuery, corrupt the most recent. Ordinarily, we would expect the
        // returned LogState to include the most recent snapshot plus subsequent entries. With the corruption, the base
        // state should be the previous, valid snapshot plus all subsequent entries.
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        ClusterMetadata lastSnapshot = epochToSnapshot.get(snapshots.next());
        ClusterMetadata previousSnapshot = epochToSnapshot.get(snapshots.next());

        Epoch toQuery = Epoch.create(previousSnapshot.epoch.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);

        QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", lastSnapshot.epoch.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, previousSnapshot, previousSnapshot.epoch, maxEpoch);
    }

    @Test
    public void testCorruptSnapshot3()
    {
        clearAndPopulate();
        // with a multiple snapshots following toQuery, corrupt the n-most recent. Ordinarily, we would expect the
        // returned LogState to include the most recent snapshot plus subsequent entries. With the corruption, the base
        // state should be the first valid snapshot plus all subsequent entries.
        List<ClusterMetadata> toCorrupt = new ArrayList<>();
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        ClusterMetadata lastSnapshot = epochToSnapshot.get(snapshots.next());
        toCorrupt.add(lastSnapshot);
        for ( int i=0; i < 3; i++)
            toCorrupt.add(epochToSnapshot.get(snapshots.next()));
        ClusterMetadata validSnapshot = epochToSnapshot.get(snapshots.next());

        Epoch toQuery = Epoch.create(validSnapshot.epoch.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);

        toCorrupt.forEach(cm -> QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", cm.epoch.getEpoch()));
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, validSnapshot, validSnapshot.epoch, maxEpoch);
    }

    @Test
    public void testCorruptSnapshot4()
    {
        clearAndPopulate();
        // Corrupt every snapshot following toQuery, corrupt the n-most recent. Ordinarily, we would expect the
        // returned LogState to include the most recent snapshot plus subsequent entries. With the corruption, the log
        // state should contain just a list of all entries from toQuery to current.
        List<ClusterMetadata> toCorrupt = new ArrayList<>();
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        ClusterMetadata lastSnapshot = epochToSnapshot.get(snapshots.next());
        toCorrupt.add(lastSnapshot);
        for ( int i=0; i < 3; i++)
            toCorrupt.add(epochToSnapshot.get(snapshots.next()));
        ClusterMetadata validSnapshot = epochToSnapshot.get(snapshots.next());

        Epoch toQuery = Epoch.create(validSnapshot.epoch.getEpoch() + 1);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, lastSnapshot, lastSnapshot.epoch, maxEpoch);

        toCorrupt.forEach(cm -> QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", cm.epoch.getEpoch()));
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
    }

    @Test
    public void testIncompleteLogAndCorruptSnapshot1()
    {
        clearAndPopulate();
        // delete an entry from the log which precedes the last snapshot and also corrupt that snapshot. Both the deleted
        // entry and snapshot are after toQuery so it's not possible to construct a contiguous sequence from since to
        // current, so instead we get just the entries which follow the deleted one
        Epoch lastSnapshot = epochToSnapshot.lastKey();
        Epoch toDelete = Epoch.create(lastSnapshot.getEpoch() - 2);
        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", lastSnapshot.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toDelete, maxEpoch);
    }

    @Test
    public void testIncompleteLogAndCorruptSnapshot2()
    {
        clearAndPopulate();
        // delete an entry from the log which comes after the last snapshot and also corrupt that snapshot.
        // We would normally expect to skip over the intervening snapshot and return just a list of entries from toQuery
        // to current. Because we cannot make that list continuous, but we also cannot read the snapshot, instead we get
        // just the entries which we are present and have an epoch > toQuery.
        Epoch lastSnapshot = epochToSnapshot.lastKey();
        Epoch toDelete = Epoch.create(lastSnapshot.getEpoch() + 2);
        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, null, toQuery, maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", lastSnapshot.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertNull(logState.baseState);
        boolean containsExpectedEntries = logState.entries.stream()
                                                          .map(entry -> entry.epoch.getEpoch())
                                                          .allMatch(e -> e > toQuery.getEpoch()
                                                                         && e <= maxEpoch.getEpoch()
                                                                         && e != toDelete.getEpoch());
        assertTrue(containsExpectedEntries);
    }

    @Test
    public void testIncompleteLogAndCorruptSnapshot3()
    {
        clearAndPopulate();
        // delete an entry from the log which is followed by multiple corrupt snapshots. Without corruption, querying
        // from a point before the deleted entry would return a LogState with the most recent snapshot plus subsequent
        // entries. Where all the relevant snapshots are corrupt, expect a LogState with only the non-continuous list
        // of available entries between toQuery and current.
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        List<Epoch> toCorrupt = new ArrayList<>();
        for (int i=0; i<3; i++)
            toCorrupt.add(snapshots.next());
        Epoch toQuery = Epoch.create(snapshots.next().getEpoch() + 1);
        Epoch toDelete = Epoch.create(toQuery.getEpoch() + 2);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, epochToSnapshot.lastEntry().getValue(), epochToSnapshot.lastKey(), maxEpoch);
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        toCorrupt.forEach(e -> QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", e.getEpoch()));

        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertNull(logState.baseState);
        boolean containsExpectedEntries = logState.entries.stream()
                                                          .map(entry -> entry.epoch.getEpoch())
                                                          .allMatch(e -> e > toQuery.getEpoch()
                                                                         && e <= maxEpoch.getEpoch()
                                                                         && e != toDelete.getEpoch());
        assertTrue(containsExpectedEntries);
    }

    @Test
    public void testIncompleteLogAndCorruptSnapshot4()
    {
        clearAndPopulate();
        // Corrupt multiple snapshots and delete an entry which follows them in the log. Without corruption, querying
        // from an epoch before those snapshots deleted entry would return a LogState with the most recent snapshot plus
        // subsequent, non-consecutive entries. Where all the relevant snapshots are corrupt, expect a LogState with
        // only the list of available, and still non-consecutive entries between toQuery and current.
        Iterator<Epoch> snapshots = epochToSnapshot.descendingKeySet().iterator();
        List<Epoch> toCorrupt = new ArrayList<>();
        for (int i=0; i<3; i++)
            toCorrupt.add(snapshots.next());
        Epoch toQuery = Epoch.create(snapshots.next().getEpoch() + 1);
        Epoch toDelete = Epoch.create(maxEpoch.getEpoch() - 2);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertCorrectLogState(logState, epochToSnapshot.lastEntry().getValue(), epochToSnapshot.lastKey(), maxEpoch);

        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE epoch = ?", toDelete.getEpoch());
        toCorrupt.forEach(e -> QueryProcessor.executeInternal("DELETE snapshot FROM system.metadata_snapshots WHERE epoch = ?", e.getEpoch()));
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(toQuery);
        assertNull(logState.baseState);
        boolean containsExpectedEntries = logState.entries.stream()
                                                          .map(entry -> entry.epoch.getEpoch())
                                                          .allMatch(e -> e > toQuery.getEpoch()
                                                                         && e <= maxEpoch.getEpoch()
                                                                         && e != toDelete.getEpoch());
        assertTrue(containsExpectedEntries);
    }


    private void testGetLogStateHelper()
    {
        // If there is only a single snapshot between the starting point  and the current epoch, we prefer a LogState
        // with no base snapshot and just the consecutive entries. In all other cases, the LogState should include the
        // most recent snapshot that can be successfully read and was taken after the starting epoch along with any
        // subsequent entries.
        Epoch lastSnapshotAt = epochToSnapshot.lastKey();
        Epoch lastEligibleSnapshotAt = epochToSnapshot.lowerKey(lastSnapshotAt);
        for (int i = 1; i < 20; i++)
        {
            Epoch start = Epoch.create(maxEpoch.getEpoch() - i);
            LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(start);
            ClusterMetadata expectedBase;
            Epoch expectedStart;
            if (start.isEqualOrAfter(lastEligibleSnapshotAt))
            {
                expectedBase = null;
                expectedStart = start;
            }
            else
            {
                expectedBase = epochToSnapshot.lastEntry().getValue();
                expectedStart = null;
            }
            assertCorrectLogState(logState, expectedBase, expectedStart, maxEpoch);
        }
    }

    private static void assertCorrectLogState(LogState logState, ClusterMetadata expectedBase, Epoch expectedStart, Epoch endEpoch)
    {
        Epoch prev;
        if (expectedBase != null)
        {
            assertEquals(expectedBase, logState.baseState);
            prev = expectedBase.epoch;
        }
        else
        {
            // exclusive start of entries;
            assertEquals(expectedStart.nextEpoch(), logState.entries.get(0).epoch);
            prev = expectedStart;
        }

        for (Entry e : logState.entries)
        {
            assertEquals(prev.nextEpoch(), e.epoch);
            prev = e.epoch;
        }
        assertEquals(prev, endEpoch);
    }

    private void clearAndPopulate()
    {
        epochToSnapshot.clear();
        for (int i = 0; i < 10; i++)
        {
            ClusterMetadata metadata = ClusterMetadataService.instance().triggerSnapshot();
            epochToSnapshot.put(metadata.epoch, metadata);
            for (int j = 0; j < 4; j++)
            {
                Transformation transform = new CustomTransformation(CustomTransformation.PokeInt.NAME,
                                                                    new CustomTransformation.PokeInt((int) ClusterMetadata.current().epoch.getEpoch()));
                maxEpoch = ClusterMetadataService.instance().commit(transform).epoch;
            }
        }
    }
}
