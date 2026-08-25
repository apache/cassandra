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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.replication.ActivatedTransfers;
import org.apache.cassandra.replication.ActivationRequest;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.replication.ActivationRequest.Phase.COMMIT;
import static org.apache.cassandra.replication.ActivationRequest.Phase.PREPARE;
import static org.assertj.core.api.Assertions.assertThat;

public class TrackedImportFailureTest extends TrackedTransferTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedImportFailureTest.class);

    @Test
    public void importMissedActivationPrepare() throws Throwable
    {
        importMissedActivation(PREPARE);
    }

    @Test
    public void importMissedActivationCommit() throws Throwable
    {
        importMissedActivation(COMMIT);
    }

    public void importMissedActivation(ActivationRequest.Phase phase) throws Throwable
    {
        int MISSED_ACTIVATION = 2;
        try (Cluster cluster = disableBackgroundReconciler(cluster(TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.install(MISSED_ACTIVATION))))
        {
            TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.setup(cluster, phase);
            createSchema(cluster);

            Set<IInvokableInstance> missed = Collections.singleton(cluster.get(MISSED_ACTIVATION));
            Iterable<IInvokableInstance> received = cluster.stream().filter(instance -> !missed.contains(instance)).collect(Collectors.toList());

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Failed streaming on 1 instance(s):")
                      .cause()
                      .hasMessageMatching("Tracked transfer failed during " + phase + " on " + cluster.get(MISSED_ACTIVATION).broadcastAddress() + " due to TIMEOUT")
                      .hasNoCause();

            assertSummary(received, summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 0;
                    assert s.unreconciledIds() == (phase == COMMIT ? 1 : 0);
                });
            });
            assertSummary(missed, summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 0;
                    assert s.unreconciledIds() == 0;
                });
            });

            switch (phase)
            {
                case PREPARE:
                    // Activation did not start, files should be cleaned up
                    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
                    assertPendingDirs(cluster, (File pendingUuidDir) -> {
                        Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
                    });
                    break;
                case COMMIT:
                    // Activation did not complete, files should still exist on all replicas
                    assertPendingDirs(cluster, (File pendingUuidDir) -> {
                        Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isNotEmpty();
                    });
                    break;
            }

            // If the activation is not everywhere, it shouldn't be purged on compaction
            assertCompaction(cluster, received, TRANSFERS_EXIST, TRANSFERS_EXIST);

            if (phase == PREPARE)
                return;

            // Permit activation of missed commits during read reconciliation
            TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.setup(cluster, null);

            // Use coordinated query rather to confirm read reconciliation triggers activation
            IInvokableInstance coordinator = cluster.get(3); // not initial transfer coordinator, but received activation
            assertCoordinatedRead(coordinator, rows -> {
                assertRows(rows, row(1, 1));
            });

            // Confirm others receive activation
            assertLocalSelect(missed, rows -> {
                assertRows(rows, row(1, 1));
            });

            assertCompaction(cluster, cluster, TRANSFERS_EXIST, TRANSFERS_EMPTY);

            // Activation completed, files should be removed
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });
        }
    }

    /**
     * If a replica is missing an activation, executes a data read, then discovers the missing transfer during read
     * reconciliation, it needs to augment the client response with the new transfer.
     */
    @Test
    public void importMissingOnDataReplicaDuringAugment() throws Throwable
    {
        ActivationRequest.Phase phase = COMMIT;
        int MISSED_ACTIVATION = 2;
        try (Cluster cluster = disableBackgroundReconciler(cluster(TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.install(MISSED_ACTIVATION))))
        {
            TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.setup(cluster, phase);
            createSchema(cluster);

            IInvokableInstance missed = cluster.get(MISSED_ACTIVATION);

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Failed streaming on 1 instance(s):")
                      .cause()
                      .hasMessageMatching("Tracked transfer failed during " + phase + " on " + missed.broadcastAddress() + " due to TIMEOUT")
                      .hasNoCause();

            assertSummary(Collections.singleton(missed), summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 0;
                    assert s.unreconciledIds() == 0;
                });
            });

            // Permit activation of missed commits during read reconciliation
            TrackedTransferTestBase.ByteBuddyInjections.SkipActivation.setup(cluster, null);

            // First read will fail due to failure to augment with transfer
            long mark = missed.logs().mark();
            Assertions.assertThatThrownBy(() -> {
                assertCoordinatedRead(missed, rows -> {
                    assertRows(rows, row(1, 1));
                });
            }).isInstanceOf(missed.callOnInstance(() -> ReadTimeoutException.class)); // use instance classloader

            List<String> logs = missed.logs().grep(mark, "Missing mutation ShortMutationId").getResult();
            Assertions.assertThat(logs).isNotEmpty();

            // Retry succeeds
            assertCoordinatedRead(missed, rows -> {
                assertRows(rows, row(1, 1));
            });
        }
    }

    /*
     * Ensure that activation IDs attached to SSTables aren't spread across Token boundaries by compaction.
     *
     * For example:
     * IMPORT_TOKEN is owned by replicas (A, B)
     * OUTSIDE_IMPORT_TOKEN is owned by replicas (B, C)
     * Execute import so (A, B) have IMPORT_TOKEN
     * Execute plain write so (B, C) have OUTSIDE_IMPORT_TOKEN
     * Do a major compaction on B so IMPORT_TOKEN and OUTSIDE_IMPORT_TOKEN are compacted together into the same SSTable
     * Execute a data read for OUTSIDE_IMPORT_TOKEN against B, ensure it doesn't contain any activation IDs
     */
    @Test
    public void importActivationMergedByCompaction() throws Throwable
    {
        try (Cluster cluster = cluster((cl, tg, instance, gen) -> ByteBuddyInjections.SkipPurgeTransfers.install().initialise(cl, tg, instance, gen)))
        {
            createSchema(cluster, 2);

            Set<IInvokableInstance> inImportRange = new HashSet<>();
            cluster.forEach(instance -> {
                logger.debug("Instance {} ring is {}", ClusterUtils.instanceId(instance), ClusterUtils.ring(instance));
                boolean isInRange = instance.callOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    DataPlacement placement = ClusterMetadata.current().placements.get(cfs.keyspace.getMetadata().params.replication);
                    return placement.writes.forToken(IMPORT_TOKEN).get().containsSelf();
                });
                if (isInRange)
                    inImportRange.add(instance);
            });
            Assertions.assertThat(inImportRange).hasSize(2);

            // Find a partition key that's not owned by the same replicas as the import
            Murmur3Partitioner.LongToken NON_IMPORT_TOKEN = new Murmur3Partitioner.LongToken(IMPORT_TOKEN.getLongValue() * 3);
            int NON_IMPORT_PK = Int32Type.instance.compose(Murmur3Partitioner.LongToken.keyForToken(NON_IMPORT_TOKEN));

            Set<IInvokableInstance> inNonImportRange = new HashSet<>();
            cluster.forEach(instance -> {
                boolean isInRange = instance.callOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    DataPlacement placement = ClusterMetadata.current().placements.get(cfs.keyspace.getMetadata().params.replication);
                    return placement.writes.forToken(NON_IMPORT_TOKEN).get().containsSelf();
                });
                if (isInRange)
                    inNonImportRange.add(instance);
            });
            Assertions.assertThat(inNonImportRange).hasSize(2);
            Assertions.assertThat(inNonImportRange).isNotEqualTo(inImportRange);

            // Import: (A, B)
            // Plain: (B, C)
            IInvokableInstance A = null;
            IInvokableInstance B = null;
            IInvokableInstance C = null;
            for (IInvokableInstance instance : cluster)
            {
                boolean isImport = inImportRange.contains(instance);
                boolean isNonImport = inNonImportRange.contains(instance);
                if (isImport && isNonImport)
                    B = instance;
                else if (isImport)
                    A = instance;
                else if (isNonImport)
                    C = instance;
            }
            Assertions.assertThat(A).isNotNull();
            Assertions.assertThat(B).isNotNull();
            Assertions.assertThat(C).isNotNull();

            doImport(cluster, A);
            assertLocalSelect(List.of(A, B), (IIsolatedExecutor.SerializableConsumer<Object[][]>) rows -> {
                assertRows(rows, row(IMPORT_PK, IMPORT_PK));
            });

            ShortMutationId importTransferId = callSerialized(A, () -> ShortMutationId.serializer, () -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    ActivatedTransfers transfers = sstable.getCoordinatorLogOffsets().transfers();
                    if (!transfers.isEmpty())
                        return transfers.iterator().next();
                }
                return null;
            });
            Assertions.assertThat(importTransferId).isNotNull();
            C.coordinator().execute(withKeyspace("INSERT INTO %s." + TABLE + "(k, v) VALUES (?, ?)"), ConsistencyLevel.ALL, NON_IMPORT_PK, NON_IMPORT_PK);
            assertCompaction(cluster, Collections.singleton(B), NOOP, NOOP);

            // Reading from B for a range that doesn't include the import shouldn't include any transfer IDs, even though they've been compacted together
            long mark = B.logs().mark();
            Object[][] rows = B.coordinator().execute(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = ?"), ConsistencyLevel.ALL, NON_IMPORT_PK);
            assertRows(rows, row(NON_IMPORT_PK, NON_IMPORT_PK));
            Assertions.assertThat(B.logs().grep(mark, "Found overlapping activation ID ").getResult()).isEmpty();

            // But if the read range does include a transfer ID, it should have been added
            mark = B.logs().mark();
            rows = B.coordinator().execute(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = ?"), ConsistencyLevel.ALL, IMPORT_PK);
            assertRows(rows, row(IMPORT_PK, IMPORT_PK));
            Assertions.assertThat(B.logs().grep(mark, "Found overlapping activation ID ").getResult()).isNotEmpty();
        }
    }

    @Test
    public void importFailedStreamCleanup() throws Throwable
    {
        int FAILED_STREAM = 3;
        try (Cluster cluster = cluster(ByteBuddyInjections.FailIncomingStream.install(FAILED_STREAM)))
        {
            createSchema(cluster);

            IInvokableInstance importer = cluster.get(1);
            IInvokableInstance missed = cluster.get(FAILED_STREAM);

            long mark = importer.logs().mark();
            Assertions.assertThatThrownBy(() -> doImport(cluster, importer))
                      .isInstanceOf(RuntimeException.class)
                      .cause()
                      .isInstanceOf(RuntimeException.class)
                      .cause()
                      .hasMessageContaining("Remote peer " + missed.broadcastAddress() + " failed stream session");
            List<String> logs = importer.logs().watchFor(mark, "Remote peer " + missed.broadcastAddress().toString() + " failed stream session").getResult();
            Assertions.assertThat(logs).isNotEmpty();

            // Await cleanup of failed stream
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

            // We exclude the missed instance because the SSTables streamed to the pending directory
            // are not linked to TransferTrackingService and hence cleanup does not know which SSTables to clean up
            assertPendingDirs(cluster.stream().filter(instance -> instance != missed).collect(Collectors.toList()), (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            // empty
            assertLocalSelect(cluster, AssertUtils::assertRows);
        }
    }

    @Test
    @Ignore("Reactivate this when we support CL < ALL for tracked imports")
    public void importReplicaDown() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            String keyspace = "replica_down";
            createSchema(cluster, keyspace);

            Iterable<IInvokableInstance> down = Collections.singleton(cluster.get(3));
            for (IInvokableInstance instance : down)
                instance.shutdown().get();

            Iterable<IInvokableInstance> up = cluster.stream().filter(instance -> !instance.isShutdown()).collect(Collectors.toList());

            doImport(cluster, keyspace);

            cluster.get(3).startup();

            // Transfers did not complete, files should still exist on up replicas
            assertPendingDirs(up, keyspace, (File pendingUuidDir) -> {
                assertThat(pendingUuidDir.listUnchecked(File::isFile)).isNotEmpty();
            });
            assertPendingDirs(down, keyspace, (File pendingUuidDir) -> {
                assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            // Transfers did not complete, transfer IDs should not be removed
            assertCompaction(cluster, keyspace, cluster, TRANSFERS_EXIST, TRANSFERS_EXIST);

            assertLocalSelect(up, keyspace, rows -> assertRows(rows, row(1, 1)));
        }
    }

    private static void assertCoordinatedRead(IInvokableInstance instance, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        ICoordinator coordinator = instance.coordinator();
        String cql = "SELECT * FROM %s." + TABLE + " WHERE k = 1";
        Object[][] rows = coordinator.execute(withKeyspace(cql), ConsistencyLevel.ALL);
        onRows.accept(rows);
    }

    private static <T> T callSerialized(IInvokableInstance instance, IIsolatedExecutor.SerializableSupplier<UnversionedSerializer<T>> serializer, IIsolatedExecutor.SerializableCallable<T> callable)
    {
        ByteBuffer serialized = instance.callOnInstance(() -> {
            T deserialized = callable.call();
            UnversionedSerializer<T> serialize = serializer.get();
            try
            {
                return serialize.serialize(deserialized);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });

        try
        {
            return serializer.get().deserialize(serialized);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
