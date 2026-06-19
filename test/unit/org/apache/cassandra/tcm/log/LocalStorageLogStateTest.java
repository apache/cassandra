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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.TriggerSnapshot;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocalStorageLogStateTest extends LogStateTestBase
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    public LocalStorageLogStateTest()
    {
    }

    @Override
    LogStateSUT getSystemUnderTest(MetadataSnapshots snapshots)
    {
        return new LogStateSUT()
        {
            SystemKeyspaceStorage storage = new SystemKeyspaceStorage(() -> snapshots);
            Epoch epoch = Epoch.FIRST;

            @Override
            public void cleanup()
            {
                ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, METADATA_LOG).truncateBlockingWithoutSnapshot();
            }

            @Override
            public void insertRegularEntry() throws IOException
            {
                // somewhat of a hack, but a "real" log as used by the DistributedMetadataKeyspace equivalent of this
                // test will bootstrap the PreInitialize entry at Epoch.FIRST. SystemKeyspaceStorage doesn't do that,
                // so fake an extra entry here to keep the test data in sync.
                if (epoch.is(Epoch.FIRST))
                {
                    storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                    epoch = epoch.nextEpoch();
                }
                storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, CustomTransformation.make((int) epoch.getEpoch())));
                epoch = epoch.nextEpoch();
            }

            @Override
            public void snapshotMetadata() throws IOException
            {
                storage.append(new Entry(new Entry.Id(epoch.getEpoch()), epoch, TriggerSnapshot.instance));
                epoch = epoch.nextEpoch();
            }

            @Override
            public LogState getLogState(Epoch since)
            {
                return storage.getLogState(since);
            }

            @Override
            public void dumpTables() throws IOException
            {
                UntypedResultSet r = executeInternal("SELECT epoch, entry_id, kind FROM system.local_metadata_log");
                r.forEach(row -> {
                    long e = row.getLong("epoch");
                    long i = row.getLong("entry_id");
                    String s = row.getString("kind");
                    System.out.println(String.format("(%d, %d, %s)", e, i, s));
                });
            }
        };
    }

    @Test
    public void catchUpViaForceSnapshotLeavesGappedLog() throws Exception
    {
        // Simulates a non-CMS node that caught up via a ForceSnapshot (real or synthetic).
        // LocalLog.append(LogState) converts the received baseState into a synthetic ForceSnapshot,
        // which is processed but not written to local_metadata_log.
        // The snapshot is stored in metadata_snapshots by MetadataSnapshotListener.
        // The resulting on-disk state is: entries 1..X in local_metadata_log, snapshot at S sometime after X in
        // metadata_snapshots, with no entries between X and S.
        // Any peer requesting log since epoch <= X must receive the snapshot — not just the continuous
        // run of entries up to X, which would leave it unable to advance past the gap.
        MetadataSnapshots realSnapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
        LogStateSUT sut = getSystemUnderTest(realSnapshots);
        sut.cleanup();
        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME)
                         .truncateBlockingWithoutSnapshot();

        // insertRegularEntry inserts 2 entries on the first call (epoch 1 and 2) due to the
        // Epoch.FIRST pre-init entry, then 1 per subsequent call. After 3 calls: epochs 1..4.
        sut.insertRegularEntry();
        sut.insertRegularEntry();
        sut.insertRegularEntry();

        // Simulate ForceSnapshot at epoch 50: snapshot stored, no intermediate log entries written
        Epoch gapSnapshotEpoch = Epoch.create(50);
        realSnapshots.storeSnapshot(ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance)
                                                             .forceEpoch(gapSnapshotEpoch));

        // A peer at epoch 3 sees entry [4] which is continuous, but does not bridge to epoch 50.
        // Must return the snapshot rather than just entry 4, which would leave the peer stuck.
        LogState state = sut.getLogState(Epoch.create(3));
        assertEquals(gapSnapshotEpoch, state.baseState.epoch);
        assertTrue(state.entries.isEmpty());

        // A peer already at epoch 4 (the last log entry) previously got an empty response and stalled.
        state = sut.getLogState(Epoch.create(4));
        assertEquals(gapSnapshotEpoch, state.baseState.epoch);
        assertTrue(state.entries.isEmpty());

        ColumnFamilyStore.getIfExists(SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME)
                         .truncateBlockingWithoutSnapshot();
    }

}
