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

public class GetLogStateTest
{
    NavigableMap<Epoch, Long> epochToPeriod = new TreeMap<>();
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
        log.bootstrap(FBUtilities.getBroadcastAddressAndPort());
    }

    @Test
    public void testGetLogState()
    {
        clearAndPopulate();
        // 1. `since` = max epoch
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(ClusterMetadata.current().period, epochToPeriod.lastKey());
        assertEquals(logState, LogState.EMPTY);
        testGetLogStateHelper();
    }

    @Test
    public void testLostSnapshot()
    {
        clearAndPopulate();
        QueryProcessor.executeInternal("DELETE FROM system.metadata_snapshots WHERE epoch = ?", epochToSnapshot.lastKey().getEpoch());
        epochToSnapshot.pollLastEntry();
        testGetLogStateHelper();
    }

    @Test
    public void testIncompleteLog()
    {
        clearAndPopulate();
        // delete an entry in the log from the previous period
        Epoch lastSnapshot = epochToSnapshot.lastKey();
        Epoch toDelete = Epoch.create(lastSnapshot.getEpoch() - 2);
        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(ClusterMetadata.current().period, toQuery);
        assertCorrectLogState(logState, null, toQuery, epochToPeriod.lastKey());
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE period = ? and epoch = ?", epochToPeriod.get(toDelete), toDelete.getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(ClusterMetadata.current().period, toQuery);
        assertCorrectLogState(logState, epochToSnapshot.lastEntry().getValue(), null, epochToPeriod.lastKey());
    }

    @Test
    public void testIncompleteLogAndLostSnapshot()
    {
        clearAndPopulate();
        // delete an entry in the log from the previous period as well as the most recent snapshot. Both the deleted
        // entry and snapshot are after 'since' so it's not possible to construct a contiguous sequence from since to
        // current
        Epoch lastSnapshot = epochToSnapshot.lastKey();
        Epoch toDelete = Epoch.create(lastSnapshot.getEpoch() - 2);
        Epoch toQuery = Epoch.create(lastSnapshot.getEpoch() - 3);
        LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(ClusterMetadata.current().period, toQuery);
        assertCorrectLogState(logState, null, toQuery, epochToPeriod.lastKey());
        QueryProcessor.executeInternal("DELETE FROM system.local_metadata_log WHERE period = ? and epoch = ?", epochToPeriod.get(toDelete), toDelete.getEpoch());
        QueryProcessor.executeInternal("DELETE FROM system.metadata_snapshots WHERE epoch = ?", epochToSnapshot.lastKey().getEpoch());
        logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(ClusterMetadata.current().period, toQuery);
        assertCorrectLogState(logState, null, toDelete, epochToPeriod.lastKey());
    }

    private void testGetLogStateHelper()
    {
        for (int i = 1; i < 20; i++)
        {
            long currentPeriod = ClusterMetadata.current().period;
            Epoch start = Epoch.create(epochToPeriod.lastKey().getEpoch() - i);
            LogState logState = SystemKeyspaceStorage.SystemKeyspace.getLogState(currentPeriod, start);
            ClusterMetadata expectedBase;
            Epoch expectedStart;
            // start.nextEpoch here because start is exclusive - so if we can fill the `since` without a snapshot within
            // the previous two periods we do so without a snapshot.
            if (epochToPeriod.get(start.nextEpoch()) == currentPeriod || epochToPeriod.get(start.nextEpoch()) == currentPeriod - 1)
            {
                expectedBase = null;
                expectedStart = start;
            }
            else
            {
                expectedBase = epochToSnapshot.lastEntry().getValue();
                expectedStart = null;
            }
            assertCorrectLogState(logState, expectedBase, expectedStart, epochToPeriod.lastKey());
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
        epochToPeriod.clear();
        epochToSnapshot.clear();
        for (int i = 0; i < 10; i++)
        {
            ClusterMetadata metadata = ClusterMetadataService.instance().sealPeriod();
            epochToSnapshot.put(metadata.epoch, metadata);
            epochToPeriod.put(metadata.epoch, metadata.period);
            for (int j = 0; j < 4; j++)
            {
                metadata = ClusterMetadataService.instance()
                                                 .commit(new CustomTransformation(CustomTransformation.PokeInt.NAME,
                                                                                  new CustomTransformation.PokeInt((int) ClusterMetadata.current().epoch.getEpoch())));
                epochToPeriod.put(metadata.epoch, metadata.period);
            }
        }
    }
}
