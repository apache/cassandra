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
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.replication.ActivationRequest;
import org.apache.cassandra.replication.ImmutableCoordinatorLogOffsets;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.UnknownShardException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.junit.Assert.assertEquals;

public abstract class TrackedTransferTestBase extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedTransferTestBase.class);

    protected static final Consumer<IInstanceConfig> CONFIG = cfg -> cfg.with(Feature.NETWORK)
                                                                     .with(Feature.GOSSIP)
                                                                     .set("write_request_timeout", "1000ms")
                                                                     .set("autocompaction_on_startup_enabled", false)
                                                                     .set("repair_request_timeout", "2s")
                                                                     .set("stream_transfer_task_timeout", "10s");

    protected static final Consumer<IInstanceConfig> ZCS_CONFIG = CONFIG.andThen(cfg -> cfg.set("stream_entire_sstables", true));
    protected static final Consumer<IInstanceConfig> NON_ZCS_CONFIG = CONFIG.andThen(cfg -> cfg.set("stream_entire_sstables", false));

    protected static final IIsolatedExecutor.SerializableConsumer<SSTableReader> TRANSFERS_EXIST = sstable -> {
        Assertions.assertThat(sstable.getCoordinatorLogOffsets().transfers()).isNotEmpty();
        Assertions.assertThat(sstable.isRepaired()).isFalse();
    };
    protected static final IIsolatedExecutor.SerializableConsumer<SSTableReader> TRANSFERS_EMPTY = sstable -> {
        Assertions.assertThat(sstable.getCoordinatorLogOffsets().transfers()).isEmpty();
        Assertions.assertThat(sstable.isRepaired()).isTrue();
    };
    protected static final IIsolatedExecutor.SerializableConsumer<SSTableReader> NOOP = sstable -> {};

    protected static final int NODES = 3;

    protected static final String TABLE = "tbl";

    protected static final int IMPORT_PK = 1;
    protected static final Token IMPORT_TOKEN = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(IMPORT_PK));

    // This should be aligned to a single shard: (min, -3074457345618258603]
    protected final static long TOKEN_VALUE_100 = -4074457345618258601L;
    protected final static Token TOKEN_100 = new Murmur3Partitioner.LongToken(TOKEN_VALUE_100);
    protected final static ByteBuffer KEY_100 = Murmur3Partitioner.LongToken.keyForToken(TOKEN_100.getLongValue());

    protected final static Range<Token> SHARD_ALIGNED_RANGE_1 = new Range<>(new Murmur3Partitioner.LongToken(TOKEN_VALUE_100 - 10), new Murmur3Partitioner.LongToken(TOKEN_VALUE_100 + 10));

    // This should be aligned to a single shard: (-3074457345618258603,3074457345618258601]
    protected final static long TOKEN_VALUE_200 = 1;
    protected final static Token TOKEN_200 = new Murmur3Partitioner.LongToken(TOKEN_VALUE_200);
    protected final static ByteBuffer KEY_200 = Murmur3Partitioner.LongToken.keyForToken(TOKEN_200.getLongValue());

    protected final static long TOKEN_VALUE_201 = 2;
    protected final static Token TOKEN_201 = new Murmur3Partitioner.LongToken(TOKEN_VALUE_201);
    protected final static ByteBuffer KEY_201 = Murmur3Partitioner.LongToken.keyForToken(TOKEN_201.getLongValue());

    protected final static Range<Token> SHARD_ALIGNED_RANGE_2 = new Range<>(new Murmur3Partitioner.LongToken(TOKEN_VALUE_200 - 10), new Murmur3Partitioner.LongToken(TOKEN_VALUE_200 + 10));

    static
    {
        DecoratedKey reversed = Murmur3Partitioner.instance.decorateKey(KEY_100);
        Assertions.assertThat(reversed.getToken()).isEqualTo(TOKEN_100);

        reversed = Murmur3Partitioner.instance.decorateKey(KEY_200);
        Assertions.assertThat(reversed.getToken()).isEqualTo(TOKEN_200);

        reversed = Murmur3Partitioner.instance.decorateKey(KEY_201);
        Assertions.assertThat(reversed.getToken()).isEqualTo(TOKEN_201);
    }

    protected static Cluster cluster() throws IOException
    {
        return cluster((cl, tg, instance, generation) -> {});
    }

    protected static Cluster cluster(Consumer<IInstanceConfig> config) throws IOException
    {
        return Cluster.build(NODES).withConfig(config).start();
    }

    protected static Cluster cluster(Consumer<IInstanceConfig> config, IInstanceInitializer initializer) throws IOException
    {
        return Cluster.build(NODES).withConfig(config).withInstanceInitializer(initializer).start();
    }

    protected static Cluster cluster(IInstanceInitializer initializer) throws IOException
    {
        return Cluster.build(NODES).withConfig(CONFIG).withInstanceInitializer(initializer).start();
    }

    protected static Cluster cluster(BiConsumer<ClassLoader, Integer> initializer) throws IOException
    {
        return Cluster.build(NODES).withConfig(CONFIG).withInstanceInitializer(initializer).start();
    }

    protected static void assertPendingActivation(Cluster cluster)
    {
        assertPendingActivation(cluster, KEYSPACE);
    }

    protected static void assertPendingActivation(Cluster cluster, String keyspace)
    {
        // Activation did not complete, files should still exist on all replicas
        assertPendingDirs(cluster, keyspace, (File pendingUuidDir) -> {
            Assertions.assertThat(pendingUuidDir.listUnchecked(File::isFile)).isNotEmpty();
        });

        // No one has activated, so should not be present in any summary
        assertSummary(cluster, keyspace, summary -> {
            Assertions.assertThat(summary).satisfies(s -> {
                assertEquals(0, s.reconciledIds());
                assertEquals(0, s.unreconciledIds());
            });
        });
    }

    protected static void assertPendingDirs(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<File> forPendingUuidDir)
    {
        assertPendingDirs(validate, KEYSPACE, forPendingUuidDir);
    }

    protected static void assertPendingDirs(Iterable<IInvokableInstance> validate, String keysapce, IIsolatedExecutor.SerializableConsumer<File> forPendingUuidDir)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                Set<File> allPendingDirs = ColumnFamilyStore.getIfExists(keysapce, TABLE).getDirectories().getPendingLocations();
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

    protected static void assertSummary(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<MutationSummary> onSummary)
    {
        assertSummary(validate, KEYSPACE, onSummary);
    }
    
    protected static void assertSummary(Iterable<IInvokableInstance> validate, String keyspace, IIsolatedExecutor.SerializableConsumer<MutationSummary> onSummary)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                TableId tableId = ColumnFamilyStore.getIfExists(keyspace, TABLE).metadata().id;
                MutationSummary summary;
                try
                {
                    summary = MutationTrackingService.instance().createSummaryForKey(key, tableId, false);
                }
                catch (UnknownShardException e)
                {
                    summary = null;
                }
                onSummary.accept(summary);
            });
        }
    }

    protected static void bounce(Cluster cluster)
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
    
    protected static String withKeyspace(String replaceIn, String keyspace)
    {
        return String.format(replaceIn, keyspace);
    }
    
    protected static String tableSchema(String keyspace)
    {
        return String.format("CREATE TABLE %s.%s (k int PRIMARY KEY, v int)", keyspace, TABLE);
    }

    protected static String tableWithKeyspace(String keyspace)
    {
        return String.format("%s.%s", keyspace, TABLE);
    }

    protected static void createSchema(Cluster cluster)
    {
        createSchema(cluster, KEYSPACE, NODES);
    }

    protected static void createSchema(Cluster cluster, String keyspace)
    {
        createSchema(cluster, keyspace, NODES);
    }

    protected static void createSchema(Cluster cluster, int rf)
    {
        createSchema(cluster, KEYSPACE, rf);
    }

    protected static void createSchema(Cluster cluster, String keyspace, int rf)
    {
        cluster.schemaChange(String.format(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                                        "{'class': 'SimpleStrategy', 'replication_factor': " + rf + "} " +
                                                        "AND replication_type='tracked'", keyspace)));
        cluster.schemaChange(tableSchema(keyspace));
    }

    protected static void doImport(Cluster cluster) throws IOException
    {
        doImport(cluster, KEYSPACE);
    }

    protected static void doImport(Cluster cluster, String keyspace) throws IOException
    {
        doImport(cluster, cluster.get(1), keyspace);
    }

    protected static void doImport(Cluster cluster, IInvokableInstance target) throws IOException
    {
        doImport(cluster, target, KEYSPACE);
    }

    protected static void doImport(Cluster cluster, IInvokableInstance target, String keyspace) throws IOException
    {
        doImport(cluster, target, failedDirs -> Assertions.assertThat(failedDirs).isEmpty(), keyspace, null);
    }

    protected static void doImport(Cluster cluster, IInvokableInstance target, String keyspace, @Nullable String createIndexCql) throws IOException
    {
        doImport(cluster, target, failedDirs -> Assertions.assertThat(failedDirs).isEmpty(), keyspace, createIndexCql);
    }

    protected static void doImport(Cluster cluster, IInvokableInstance target, Consumer<List<String>> onFailedDirs, String keyspace, @Nullable String createIndexCql) throws IOException
    {
        String file = Files.createTempDirectory(MutationTrackingTest.class.getSimpleName()).toString();

        // Needs to run outside of instance executor because creates schema
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                           .forTable(tableSchema(keyspace))
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + tableWithKeyspace(keyspace) + " (k, v) " + "VALUES (?, ?)");

        if (createIndexCql != null)
        {
            builder.withIndexes(createIndexCql).withBuildIndexes(true);
        }

        try (CQLSSTableWriter writer = builder.build())
        {
            writer.addRow(IMPORT_PK, 1);
            writer.addRow(3, 1);
        }

        // empty
        assertLocalSelect(cluster, keyspace, AssertUtils::assertRows);

        List<String> failed = target.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
            Set<String> paths = Set.of(file);
            logger.info("Importing SSTables {}", paths);
            return cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
        });

        // Sleep for a while to make sure import completes
        Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
        onFailedDirs.accept(failed);
    }

    protected static void assertLocalSelect(Iterable<IInvokableInstance> validate, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        assertLocalSelect(validate, KEYSPACE, onRows);
    }

    protected static void assertLocalSelect(Iterable<IInvokableInstance> validate, String keyspace, IIsolatedExecutor.SerializableConsumer<Object[][]> onRows)
    {
        for (IInvokableInstance instance : validate)
        {
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = 1", keyspace));
                onRows.accept(rows);
            }
            {
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE, keyspace));
                onRows.accept(rows);
            }
        }
    }

    protected static void assertCompaction(Cluster cluster,
                                           Iterable<IInvokableInstance> validate,
                                           IIsolatedExecutor.SerializableConsumer<SSTableReader> before,
                                           IIsolatedExecutor.SerializableConsumer<SSTableReader> after)
    {
        assertCompaction(cluster, KEYSPACE, validate, before, after);
    }
    protected static void assertCompaction(Cluster cluster,
                                           String keyspace,
                                           Iterable<IInvokableInstance> validate,
                                           IIsolatedExecutor.SerializableConsumer<SSTableReader> before,
                                           IIsolatedExecutor.SerializableConsumer<SSTableReader> after)
    {
        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
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
                MutationTrackingService.instance().persistLogStateForTesting();
                MutationTrackingService.instance().broadcastOffsetsForTesting();
            });
        });

        // Broadcast is async, wait until completion
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        for (IInvokableInstance instance : validate)
        {
            instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
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

    public static class ByteBuddyInjections
    {
        // Only skips direct transfer activation, not activation as part of read reconciliation
        public static class SkipActivation
        {
            // null to not skip
            public static volatile ActivationRequest.Phase phase;

            public static volatile boolean throwOnActivation = false;

            @SuppressWarnings("resource")
            public static IInstanceInitializer install(int...nodes)
            {
                return (ClassLoader cl, ThreadGroup tg, int num, int generation) -> {
                    for (int node : nodes)
                        if (node == num)
                            new ByteBuddy().rebase(ActivationRequest.VerbHandler.class)
                                           .method(named("doVerb"))
                                           .intercept(MethodDelegation.to(ByteBuddyInjections.SkipActivation.class))
                                           .make()
                                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
                };
            }

            // Need to set phase in each instance's classloader, otherwise assignment won't be visible to injected method body
            public static void setup(Cluster cluster, ActivationRequest.Phase phase)
            {
                setup(cluster, phase, false);
            }

            public static void setup(Cluster cluster, ActivationRequest.Phase phase, boolean throwOnActivation)
            {
                logger.debug("Setting up phase {}, throwOnActivation {}", phase, throwOnActivation);
                cluster.forEach(instance -> instance.runOnInstance(() -> {
                    SkipActivation.phase = phase;
                    SkipActivation.throwOnActivation = throwOnActivation;
                }));
            }

            @SuppressWarnings("unused")
            public static void doVerb(Message<ActivationRequest> msg, @SuperCall Callable<?> zuper)
            {
                if (phase != null && msg.payload.phase == SkipActivation.phase)
                {
                    if (throwOnActivation)
                    {
                        // Avoid spamming logs with retries
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                        throw new RuntimeException("Throwing on activation for test");
                    }
                    else
                    {
                        logger.info("Skipping activation for test: {}", msg.payload);
                        return;
                    }
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
            @SuppressWarnings("resource")
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

            @SuppressWarnings("resource")
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
}
