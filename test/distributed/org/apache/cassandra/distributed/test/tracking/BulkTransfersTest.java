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
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.distributed.test.sai.SAIUtil;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.ActivatedTransfers;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.replication.TransferActivation;
import org.apache.cassandra.replication.UnknownShardException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.replication.TransferActivation.Phase.COMMIT;
import static org.apache.cassandra.replication.TransferActivation.Phase.PREPARE;

/**
 * For now, tracked import with a replica down is not supported. The intention is to support this scenario by allowing
 * users to provide a {@link ConsistencyLevel} for tracked import operations, where the import will complete if
 * sufficient replicas acknowledge the transfer and activate it.
 */
public class BulkTransfersTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(BulkTransfersTest.class);

    private static final String TABLE = "tbl";
    private static final String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
    private static final String TABLE_SCHEMA_CQL = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));

    private static final int IMPORT_PK = 1;
    private static final Token IMPORT_TOKEN = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(IMPORT_PK));
    private static final int NODES = 3;

    private static final IIsolatedExecutor.SerializableConsumer<SSTableReader> TRANSFERS_EXIST = sstable -> {
        Assertions.assertThat(sstable.getCoordinatorLogOffsets().transfers())
                  .isNotEmpty();
        Assertions.assertThat(sstable.isRepaired()).isFalse();
    };
    private static final IIsolatedExecutor.SerializableConsumer<SSTableReader> TRANSFERS_EMPTY = sstable -> {
        Assertions.assertThat(sstable.getCoordinatorLogOffsets().transfers())
                  .isEmpty();
        Assertions.assertThat(sstable.isRepaired()).isTrue();
    };
    private static final IIsolatedExecutor.SerializableConsumer<SSTableReader> NOOP = sstable -> {};

    @Test
    public void importHappyPath() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createSchema(cluster);
            doImport(cluster);

            // All pending/ dirs should be empty, should have no SSTables left if all the transfers completed
            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            // Verify transfer IDs exist before compaction, then compact, then verify they're removed
            assertCompaction(cluster, cluster, TRANSFERS_EXIST, TRANSFERS_EMPTY);

            // Run after compaction, to enforce offset persistence + broadcast
            assertSummary(cluster, summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 1;
                    assert s.unreconciledIds() == 0;
                });
            });

            assertLocalSelect(cluster, rows -> assertRows(rows, row(1, 1)));
        }
    }

    @Test
    public void importIndexAlreadyPresent() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createSchema(cluster);

            String indexName = "v_idx";
            String indexCql = withKeyspace("CREATE INDEX " + indexName + " ON %s." + TABLE + " (v) USING 'sai'");
            cluster.schemaChange(indexCql);

            // This will add an SSTable that already has an SAI index, that needs to be distributed alongside the SSTable on transfer
            IInvokableInstance importer = cluster.get(1);
            long mark = importer.logs().mark();
            doImport(cluster, importer, indexCql);
            List<String> logs = importer.logs().grep(mark, "Submitting incremental index build of " + indexName).getResult();
            Assertions.assertThat(logs).isEmpty();

            // Index should exist and be queryable on all replicas after import
            SAIUtil.assertIndexQueryable(cluster, KEYSPACE, indexName);

            // Validate queries using the index
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE v = 1"));
                assertRows(rows, row(1, 1));
            });
        }
    }

    @Test
    public void importBuildsIndex() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createSchema(cluster);

            String indexName = "v_idx";
            cluster.schemaChange(withKeyspace("CREATE INDEX " + indexName + " ON %s." + TABLE + " (v) USING 'sai'"));

            // This will add an SSTable that's missing an SAI index, and the index will be built during the import on
            // the coordinator
            IInvokableInstance importer = cluster.get(1);
            long mark = importer.logs().mark();
            doImport(cluster, importer);
            List<String> logs = importer.logs().watchFor(mark, Duration.ofMinutes(1), "Submitting incremental index build of " + indexName).getResult();
            Assertions.assertThat(logs).isNotEmpty();

            // Index should exist and be queryable on all replicas after import
            SAIUtil.assertIndexQueryable(cluster, KEYSPACE, indexName);

            // Validate queries using the index
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE v = 1"));
                assertRows(rows, row(1, 1));
            });
        }
    }

    @Test
    @Ignore
    public void importReplicaDown() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createSchema(cluster);

            Iterable<IInvokableInstance> down = Collections.singleton(cluster.get(3));
            Iterable<IInvokableInstance> up = cluster.stream().filter(instance -> instance != down).collect(Collectors.toList());
            for (IInvokableInstance instance : down)
                instance.shutdown().get();

            doImport(cluster);

            cluster.get(3).startup();

            // Transfers did not complete, files should still exist on up replicas
            assertPendingDirs(up, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isNotEmpty();
            });
            assertPendingDirs(down, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            // Transfers did not complete, transfer IDs should not be removed
            assertCompaction(cluster, cluster, TRANSFERS_EXIST, TRANSFERS_EXIST);

            assertLocalSelect(up, rows -> assertRows(rows, row(1, 1)));
        }
    }

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

    public void importMissedActivation(TransferActivation.Phase phase) throws Throwable
    {
        int MISSED_ACTIVATION = 2;
        try (Cluster cluster = cluster(ByteBuddyInjections.SkipActivation.install(MISSED_ACTIVATION)))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, phase);
            createSchema(cluster);

            Set<IInvokableInstance> missed = Collections.singleton(cluster.get(MISSED_ACTIVATION));
            Iterable<IInvokableInstance> received = cluster.stream().filter(instance -> !missed.contains(instance)).collect(Collectors.toList());

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Failed streaming on 1 instance(s):")
                      .cause()
                      .hasMessageMatching("Tracked import failed during " + phase + " on " + cluster.get(MISSED_ACTIVATION).broadcastAddress() + " due to TIMEOUT")
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
            ByteBuddyInjections.SkipActivation.setup(cluster, null);

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
        TransferActivation.Phase phase = COMMIT;
        int MISSED_ACTIVATION = 2;
        try (Cluster cluster = cluster(ByteBuddyInjections.SkipActivation.install(MISSED_ACTIVATION)))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, phase);
            createSchema(cluster);

            IInvokableInstance missed = cluster.get(MISSED_ACTIVATION);

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Failed streaming on 1 instance(s):")
                      .cause()
                      .hasMessageMatching("Tracked import failed during " + phase + " on " + missed.broadcastAddress() + " due to TIMEOUT")
                      .hasNoCause();

            assertSummary(Collections.singleton(missed), summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 0;
                    assert s.unreconciledIds() == 0;
                });
            });

            // Permit activation of missed commits during read reconciliation
            ByteBuddyInjections.SkipActivation.setup(cluster, null);

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
     * When an import fails, bounce must not move the pending SSTables into the live set.
     */
    @Test
    public void importBounceAfterPending() throws Throwable
    {
        IInstanceInitializer initializer = ByteBuddyInjections.SkipActivation.install(1, 2, 3);
        try (Cluster cluster = cluster(initializer))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, COMMIT);
            createSchema(cluster);

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                .hasMessageContaining("Failed adding SSTables")
                .cause()
                .hasMessageContaining("Tracked import failed during COMMIT");

            Runnable assertEmpty = () -> {
                // Activation did not complete, files should still exist on all replicas
                assertPendingDirs(cluster, (File pendingUuidDir) -> {
                    Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isNotEmpty();
                });

                // No one has activated, so should not be present in any summary
                assertSummary(cluster, summary -> {
                    Assertions.assertThat(summary).satisfies(s -> {
                        assert s.reconciledIds() == 0;
                        assert s.unreconciledIds() == 0;
                    });
                });

                assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
            };

            assertEmpty.run();

            bounce(cluster);

            assertEmpty.run();
        }
    }

    @Test
    public void importOutOfRange() throws Throwable
    {
        try (Cluster cluster = cluster())
        {
            createSchema(cluster, 1);

            Set<IInvokableInstance> inRange = new HashSet<>();
            Set<IInvokableInstance> outOfRange = new HashSet<>();
            cluster.forEach(instance -> {
                boolean importReplica = instance.callOnInstance(() -> {
                    ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                    DataPlacement placement = ClusterMetadata.current().placements.get(cfs.keyspace.getMetadata().params.replication);
                    return placement.writes.forToken(IMPORT_TOKEN).get().containsSelf();
                });
                (importReplica ? inRange : outOfRange).add(instance);
            });
            logger.info("inRange: {}, outOfRange: {}", inRange, outOfRange);

            Assertions.assertThat(inRange).hasSize(1);
            IInvokableInstance onlyInRange = inRange.iterator().next();

            // Reject import out of range
            for (IInvokableInstance instance : outOfRange)
            {
                long mark = instance.logs().mark();
                Consumer<List<String>> onResult = failedDirs -> Assertions.assertThat(failedDirs).hasSize(1);
                doImport(cluster, instance, onResult, null);
                instance.logs().grep(mark, "java.lang.RuntimeException: Key DecoratedKey(-4069959284402364209, 00000001) is not contained in the given ranges");
            }

            doImport(cluster, onlyInRange);

            assertSummary(Collections.singleton(onlyInRange), summary -> {
                Assertions.assertThat(summary).satisfies(s -> {
                    assert s.reconciledIds() == 1;
                    assert s.unreconciledIds() == 0;
                });
            });

            for (IInvokableInstance instance : outOfRange)
            {
                // Out of range shouldn't have any transfers
                assertCompaction(cluster, Collections.singleton(instance), TRANSFERS_EMPTY, TRANSFERS_EMPTY);

                // Run after compaction, to enforce offset persistence + broadcast
                assertSummary(Collections.singleton(instance), summary -> {
                    Assertions.assertThat(summary).isNull();
                });
            }
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
        IInstanceInitializer initializer = (cl, tg, instance, gen) -> {
            ByteBuddyInjections.SkipPurgeTransfers.install().initialise(cl, tg, instance, gen);
        };
        try (Cluster cluster = cluster(initializer))
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

            assertPendingDirs(cluster, (File pendingUuidDir) -> {
                Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isEmpty();
            });

            assertLocalSelect(cluster, rows -> {
                assertRows(rows); // empty
            });
        }
    }

    public static class ByteBuddyInjections
    {
        // Only skips direct transfer activation, not activation as part of read reconciliation
        public static class SkipActivation
        {
            // null to not skip
            public static volatile TransferActivation.Phase phase;

            public static IInstanceInitializer install(int...nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(TransferActivation.VerbHandler.class)
                                           .method(named("doVerb"))
                                           .intercept(MethodDelegation.to(ByteBuddyInjections.SkipActivation.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            // Need to set phase in each instance's classloader, otherwise assignment won't be visible to injected method body
            public static void setup(Cluster cluster, TransferActivation.Phase phase)
            {
                logger.debug("Setting up phase {}", phase);
                cluster.forEach(instance -> instance.runOnInstance(() -> ByteBuddyInjections.SkipActivation.phase = phase));
            }

            @SuppressWarnings("unused")
            public static void doVerb(Message<TransferActivation> msg, @SuperCall Callable<?> zuper)
            {
                if (phase != null && msg.payload.phase == SkipActivation.phase)
                {
                    logger.info("Skipping activation for test: {}", msg.payload);
                    return;
                }

                logger.info("Test running activation as usual: {}", msg.payload);

                try
                {
                    zuper.call();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        // ImmutableCoordinatorLogOffsets.Builder.purgeTransfers(Predicate)
        public static class SkipPurgeTransfers
        {
            public static IInstanceInitializer install()
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    new ByteBuddy().rebase(ImmutableCoordinatorLogOffsets.Builder.class)
                                   .method(named("purgeTransfers").and(takesArguments(Predicate.class)))
                                   .intercept(MethodDelegation.to(SkipPurgeTransfers.class))
                                   .make()
                                   .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static void purgeTransfers()
            {
                logger.debug("Skipping purgeTransfers for test");
            }
        }

        // CassandraStreamReceiver.finished
        public static class FailIncomingStream
        {
            @SuppressWarnings("unused")
            private static volatile boolean enabled = true;

            public static IInstanceInitializer install(int... nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(CassandraStreamReceiver.class)
                                           .method(named("finished").and(takesNoArguments()))
                                           .intercept(MethodDelegation.to(FailIncomingStream.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            @SuppressWarnings("unused")
            public static void finished(@This CassandraStreamReceiver self)
            {
                throw new RuntimeException("Failing incoming stream for test");
            }

            public static void toggle(Cluster cluster, boolean enable)
            {
                enabled = enable;
                cluster.forEach(instance -> instance.runOnInstance(() -> FailIncomingStream.enabled = enable));
            }
        }
    }

    private static Cluster cluster() throws IOException
    {
        return cluster((cl, tg, instance, generation) -> {});
    }

    private static Cluster cluster(IInstanceInitializer initializer) throws IOException
    {
        return Cluster.build(NODES)
                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                     .with(Feature.GOSSIP)
                                     .set("mutation_tracking_enabled", "true")
                                     .set("write_request_timeout", "1000ms")
                                     .set("autocompaction_on_startup_enabled", false)
                                     .set("repair_request_timeout", "2s")
                                     .set("stream_transfer_task_timeout", "10s"))
                      .withInstanceInitializer(initializer)
                      .start();
    }

    private static void createSchema(Cluster cluster)
    {
        createSchema(cluster, NODES);
    }

    private static void createSchema(Cluster cluster, int rf)
    {
        cluster.schemaChange(String.format(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                                        "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "} " +
                                                        "AND replication_type='tracked';")));
        cluster.schemaChange(TABLE_SCHEMA_CQL);
    }

    private static void doImport(Cluster cluster) throws IOException
    {
        doImport(cluster, cluster.get(1));
    }

    private static void doImport(Cluster cluster, IInvokableInstance target) throws IOException
    {
        doImport(cluster, target, failedDirs -> Assertions.assertThat(failedDirs).isEmpty(), null);
    }

    private static void doImport(Cluster cluster, IInvokableInstance target, @Nullable String createIndexCql) throws IOException
    {
        doImport(cluster, target, failedDirs -> Assertions.assertThat(failedDirs).isEmpty(), createIndexCql);
    }

    private static void doImport(Cluster cluster, IInvokableInstance target, Consumer<List<String>> onFailedDirs, @Nullable String createIndexCql) throws IOException
    {
        String file = Files.createTempDirectory(MutationTrackingTest.class.getSimpleName()).toString();

        // Needs to run outside of instance executor because creates schema
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(TABLE_SCHEMA_CQL)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)");

        if (createIndexCql != null)
        {
            builder.withIndexes(createIndexCql).withBuildIndexes(true);
        }

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(IMPORT_PK, 1);
        }

        assertLocalSelect(cluster, rows -> {
            assertRows(rows); // empty
        });

        List<String> failed = target.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
            Set<String> paths = Set.of(file);
            logger.info("Importing SSTables {}", paths);
            return cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
        });

        // Sleep for a while to make sure import completes
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
        onFailedDirs.accept(failed);
    }

    private static void assertCoordinatedRead(IInvokableInstance instance, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        ICoordinator coordinator = instance.coordinator();
        String cql = "SELECT * FROM %s." + TABLE + " WHERE k = 1";
        Object[][] rows = coordinator.execute(withKeyspace(cql), ConsistencyLevel.ALL);
        onRows.accept(rows);
    }

    private static void assertPendingDirs(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<File> forPendingUuidDir)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                Set<File> allPendingDirs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).getDirectories().getPendingLocations();
                for (File pendingDir : allPendingDirs)
                {
                    File[] pendingUuidDirs = pendingDir.listUnchecked(File::isDirectory);
                    for (File pendingUuidDir : pendingUuidDirs)
                    {
                        forPendingUuidDir.accept(pendingUuidDir);
                    }
                }
            });
        }
    }

    private static void assertSummary(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<MutationSummary> onSummary)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                TableId tableId = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).metadata().id;
                MutationSummary summary;
                try
                {
                    summary = MutationTrackingService.instance.createSummaryForKey(key, tableId, false);
                }
                catch (UnknownShardException e)
                {
                    summary = null;
                }
                logger.debug("Validating summary {}", summary);
                onSummary.accept(summary);
            });
        }
    }

    private static void assertCompaction(Cluster cluster, Iterable<IInvokableInstance> validate,
            IIsolatedExecutor.SerializableConsumer<SSTableReader> before,
            IIsolatedExecutor.SerializableConsumer<SSTableReader> after)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    logger.info("SSTable {} before compaction: {}", sstable.getFilename(), sstable.getCoordinatorLogOffsets());
                    before.accept(sstable);
                }
            });
        }

        // Activation ID  must be persisted and broadcast across all peers in the cluster for any to mark as persisted + reconciled
        cluster.forEach(i -> {
            i.runOnInstance(() -> {
                MutationTrackingService.instance.persistLogStateForTesting();
                MutationTrackingService.instance.broadcastOffsetsForTesting();
            });
        });

        // Broadcast is async, wait until completion
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                logger.info("Triggering compaction on instance {}", cfs.metadata.keyspace);
                CompactionManager.instance.performMaximal(cfs);

                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    logger.info("SSTable {} after compaction: {}", sstable.getFilename(), sstable.getCoordinatorLogOffsets());
                    after.accept(sstable);
                }
            });
        }
    }

    private static void assertLocalSelect(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        for (IInvokableInstance instance : validate)
        {
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = 1"));
                onRows.accept(rows);
            }
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                onRows.accept(rows);
            }
        }
    }

    private static void bounce(Cluster cluster)
    {
        cluster.forEach(instance -> {
            try
            {
                instance.shutdown().get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            instance.startup();
        });
    }

    private static <T> T callSerialized(IInvokableInstance instance, IIsolatedExecutor.SerializableSupplier<IVersionedSerializer<T>> serializer, IIsolatedExecutor.SerializableCallable<T> callable)
    {
        ByteBuffer serialized = instance.callOnInstance(() -> {
            T deserialized = callable.call();
            IVersionedSerializer<T> serialize = serializer.get();
            try
            {
                return serialize.serialize(deserialized, MessagingService.current_version);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        try
        {
            return serializer.get().deserialize(serialized, MessagingService.current_version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
