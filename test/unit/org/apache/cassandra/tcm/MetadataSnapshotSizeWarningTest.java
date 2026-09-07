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

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.listeners.MetadataSnapshotListener;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the D1 fix: {@link MetadataSnapshots.SystemKeyspaceMetadataSnapshots#storeSnapshot} writes the whole
 * serialised {@link ClusterMetadata} as a single mutation, and that mutation is rejected with
 * {@link MutationExceededMaxSizeException} once it exceeds {@code max_mutation_size}. This test verifies:
 * <ul>
 *     <li>a small snapshot stores cleanly, with no warning-threshold side effects</li>
 *     <li>an oversized snapshot fails with {@link MutationExceededMaxSizeException}, and the attempted size and
 *     failure are still recorded via {@link TCMMetrics}</li>
 *     <li>{@link MetadataSnapshotListener#notify} swallows that failure (so the log-processing thread survives)
 *     while still incrementing {@link TCMMetrics#snapshotStoreFailures}</li>
 * </ul>
 * test/conf/cassandra.yaml sets {@code commitlog_segment_size: 5MiB}, which makes
 * {@code max_mutation_size = commitlog_segment_size / 2 = 2,621,440} bytes for this test JVM. A one
 * partition-key-int + one regular-text-column table serialises at roughly 527 bytes as part of a
 * {@link ClusterMetadata}, so 5,000 such tables (~2.6MB) reliably clears that limit.
 */
public class MetadataSnapshotSizeWarningTest
{
    private static final String KEYSPACE = "metadata_snapshot_size_warning_test_ks";

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.prepareServerNoRegister();
        CommitLog.instance.start();
    }

    private static DistributedSchema schemaWithTables(int tableCount)
    {
        List<TableMetadata> tables = new ArrayList<>(tableCount);
        for (int i = 0; i < tableCount; i++)
        {
            tables.add(TableMetadata.builder(KEYSPACE, "t" + i)
                                     .addPartitionKeyColumn("k", Int32Type.instance)
                                     .addRegularColumn("v", UTF8Type.instance)
                                     .build());
        }
        KeyspaceMetadata ksm = KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1), Tables.of(tables));
        return new DistributedSchema(Keyspaces.of(ksm));
    }

    private static void truncateSnapshotsTable()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.SNAPSHOT_TABLE_NAME);
        if (cfs != null)
            cfs.truncateBlockingWithoutSnapshot();
    }

    @Test
    public void smallSnapshotStoresCleanlyWithNoWarningOrFailure()
    {
        truncateSnapshotsTable();
        try
        {
            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata small = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(2), Murmur3Partitioner.instance, schemaWithTables(10));
            snapshots.storeSnapshot(small);

            assertEquals(small, snapshots.getSnapshot(small.epoch));
            assertEquals("A small snapshot must not increment the failure counter",
                         failuresBefore, TCMMetrics.instance.snapshotStoreFailures.getCount());
            assertTrue("A small snapshot must be well under the warning threshold",
                       TCMMetrics.instance.lastSnapshotSize.getValue() < DatabaseDescriptor.getMaxMutationSize() * 0.75);
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void oversizeSnapshotFailsWithMutationExceededMaxSizeExceptionAndIsRecorded()
    {
        truncateSnapshotsTable();
        try
        {
            MetadataSnapshots snapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            ClusterMetadata oversize = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(3), Murmur3Partitioner.instance, schemaWithTables(5000));

            try
            {
                snapshots.storeSnapshot(oversize);
                fail("Expected storeSnapshot to fail once the serialised size exceeds max_mutation_size");
            }
            catch (RuntimeException e)
            {
                Throwable cause = e instanceof MutationExceededMaxSizeException ? e : e.getCause();
                assertTrue("Expected MutationExceededMaxSizeException, got " + e,
                           cause instanceof MutationExceededMaxSizeException);
            }

            assertTrue("The attempted serialised size must be recorded even though the store failed",
                       TCMMetrics.instance.lastSnapshotSize.getValue() > DatabaseDescriptor.getMaxMutationSize() * 0.75);
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }

    @Test
    public void listenerSwallowsOversizeFailureButCountsIt()
    {
        truncateSnapshotsTable();
        try
        {
            MetadataSnapshots realSnapshots = new MetadataSnapshots.SystemKeyspaceMetadataSnapshots();
            StubClusterMetadataService service = StubClusterMetadataService.builder(Murmur3Partitioner.instance)
                                                                            .withSnapshots(realSnapshots)
                                                                            .build();
            ClusterMetadataService.unsetInstance();
            ClusterMetadataService.setInstance(service);

            long failuresBefore = TCMMetrics.instance.snapshotStoreFailures.getCount();

            ClusterMetadata oversize = ClusterMetadataTestHelper.minimalForTesting(Epoch.create(4), Murmur3Partitioner.instance, schemaWithTables(5000));
            Entry entry = new Entry(Entry.Id.NONE, oversize.epoch, new ForceSnapshot(oversize));
            ClusterMetadata previous = ClusterMetadataTestHelper.minimalForTesting(Murmur3Partitioner.instance);
            Transformation.Result result = entry.transform.execute(previous);

            MetadataSnapshotListener listener = new MetadataSnapshotListener();
            // Must not throw: the listener is required to swallow storeSnapshot failures so the log-processing
            // thread survives a snapshot that is too large to store.
            listener.notify(entry, result);

            assertEquals("The listener must increment snapshotStoreFailures on a caught storeSnapshot failure",
                         failuresBefore + 1, TCMMetrics.instance.snapshotStoreFailures.getCount());
        }
        finally
        {
            truncateSnapshotsTable();
        }
    }
}
