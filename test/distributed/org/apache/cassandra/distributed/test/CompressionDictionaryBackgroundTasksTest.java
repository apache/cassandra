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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.db.compression.SSTableChunkSampler;
import org.apache.cassandra.db.compression.TrainingState;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A drain of a node interrupts the training of a dictionary and a read of a dictionary, and on startup, a table refreshes
 * its dictionary when the keyspace which stores dictionaries may not be open yet. None of that is an error. A training
 * which fails while sampling, for any other reason, has to report the failure and let a next training start.
 */
public class CompressionDictionaryBackgroundTasksTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 5000;

    @Test
    public void shutdownWhileStoring() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("write_request_timeout", "5m"))
                                           .start()))
        {
            prepareTable(cluster, 2);

            // storing waits for the response of node 1 at QUORUM, it never comes
            CountDownLatch storing = new CountDownLatch(1);
            cluster.filters().outbound().verbs(Verb.MUTATION_REQ.id).from(2).to(1)
                   .messagesMatching((from, to, message) -> {
                       storing.countDown();
                       return true;
                   }).drop();

            long mark = cluster.get(2).logs().mark();
            train(cluster.get(2));
            assertThat(storing.await(1, TimeUnit.MINUTES)).as("the dictionary is being stored").isTrue();
            assertThat(trainingStatus(cluster.get(2))).isEqualTo("TRAINING");

            cluster.filters().reset();
            restart(cluster.get(2));
            assertThat(errors(cluster.get(2), mark)).isEmpty();
            assertThat(cluster.get(2).logs().grep(mark, "Dictionary training for .* stopped by the shutdown").getResult()).hasSize(1);
        }
    }

    @Test
    public void shutdownWhileSampling() throws Throwable
    {
        try (Cluster cluster = singleNode(false))
        {
            prepareTable(cluster, 1);
            cluster.get(1).runOnInstance(() -> BB.read = BB.BLOCK);

            long mark = cluster.get(1).logs().mark();
            train(cluster.get(1));
            await("sampling", () -> cluster.get(1).callOnInstance(() -> BB.sampling));

            restart(cluster.get(1));
            assertThat(errors(cluster.get(1), mark)).isEmpty();
            // an uncaught error of a node which shuts down reaches neither its log nor the cluster
            assertThat(cluster.get(1).logs().grep(mark, "Sampling from SSTables for .* stopped by the shutdown").getResult()).hasSize(1);
        }
    }

    @Test
    public void ioExceptionWhileSampling() throws Throwable
    {
        failWhileSampling(BB.IO_EXCEPTION);
    }

    @Test
    public void fsErrorWhileSampling() throws Throwable
    {
        failWhileSampling(BB.FS_ERROR);
    }

    private static void failWhileSampling(int failure) throws Throwable
    {
        try (Cluster cluster = singleNode(false))
        {
            prepareTable(cluster, 1);
            cluster.get(1).runOnInstance(() -> BB.read = failure);

            train(cluster.get(1));
            await("the failure of the training", () -> trainingStatus(cluster.get(1)).startsWith("FAILED"));

            cluster.get(1).runOnInstance(() -> BB.read = BB.REAL);
            train(cluster.get(1));
            await("the next training", () -> trainingStatus(cluster.get(1)).equals("COMPLETED"));
        }
    }

    @Test
    public void refreshBeforeKeyspaceWithDictionariesOpens() throws Throwable
    {
        try (Cluster cluster = singleNode(true))
        {
            prepareTable(cluster, 1);
            train(cluster.get(1));
            await("the training", () -> trainingStatus(cluster.get(1)).equals("COMPLETED"));

            long mark = cluster.get(1).logs().mark();
            restart(cluster.get(1));

            assertThat(errors(cluster.get(1), mark)).isEmpty();
            // the refresh really ran while the keyspace was not there
            assertThat(cluster.get(1).logs().grep(mark, "Could not read the latest compression dictionary for .* is not loaded yet").getResult()).isNotEmpty();
            // as did the retrieval of a notified dictionary, see BB.initCf
            assertThat(cluster.get(1).logs().grep(mark, "Could not read compression dictionary .* is not loaded yet").getResult()).hasSize(1);
            // and a later refresh reads the dictionary
            await("the dictionary", () -> cluster.get(1).callOnInstance(() -> manager().getCurrent() != null));
        }
    }

    @Test
    public void shutdownWhileRefreshing() throws Throwable
    {
        try (Cluster cluster = singleNode(false))
        {
            prepareTable(cluster, 1);
            cluster.get(1).runOnInstance(() -> BB.retrieve = BB.BLOCK);
            await("the refresh", () -> cluster.get(1).callOnInstance(() -> BB.retrieving));

            long mark = cluster.get(1).logs().mark();
            restart(cluster.get(1));

            assertThat(errors(cluster.get(1), mark)).isEmpty();
            // an uncaught error of a node which shuts down reaches neither its log nor the cluster
            assertThat(cluster.get(1).logs().grep(mark, "Refresh of the compression dictionary for .* stopped by the shutdown").getResult()).hasSize(1);
        }
    }

    @Test
    public void shutdownWhileRetrievingNotifiedDictionary() throws Throwable
    {
        try (Cluster cluster = singleNode(false))
        {
            prepareTable(cluster, 1);
            cluster.get(1).runOnInstance(() -> {
                BB.retrieveNotified = BB.BLOCK;
                manager().onNewDictionaryAvailable(new CompressionDictionary.DictId(CompressionDictionary.Kind.ZSTD, 1));
            });
            await("the retrieval", () -> cluster.get(1).callOnInstance(() -> BB.retrieving));

            long mark = cluster.get(1).logs().mark();
            restart(cluster.get(1));

            assertThat(errors(cluster.get(1), mark)).isEmpty();
            assertThat(cluster.get(1).logs().grep(mark, "Retrieval of compression dictionary .* stopped by the shutdown").getResult()).hasSize(1);
        }
    }

    @Test
    public void errorWhileRefreshing() throws Throwable
    {
        try (Cluster cluster = singleNode(false))
        {
            prepareTable(cluster, 1);
            // by name, as the class of a throwable of a node comes from the class loader of the node
            cluster.setUncaughtExceptionsFilter(t -> FSReadError.class.getName().equals(t.getClass().getName()));
            long mark = cluster.get(1).logs().mark();
            cluster.get(1).runOnInstance(() -> {
                BB.refreshes.set(0);
                BB.retrieve = BB.FS_ERROR;
            });

            // the task stays scheduled
            await("two failed refreshes", () -> cluster.get(1).callOnInstance(() -> BB.refreshes.get() >= 2));
            cluster.get(1).runOnInstance(() -> BB.retrieve = BB.REAL);

            assertThat(cluster.get(1).logs().grep(mark, "Exception in thread .*ScheduledTasks").getResult()).isNotEmpty();
        }
    }

    private static Cluster singleNode(boolean slowOpeningOfKeyspaceWithDictionaries) throws IOException
    {
        Cluster cluster = init(Cluster.build(1)
                                      .withInstanceInitializer(slowOpeningOfKeyspaceWithDictionaries ? BB::installWithSlowOpening : BB::install)
                                      .withConfig(c -> c.set("disk_failure_policy", "ignore")
                                                        .set("compression_dictionary_refresh_initial_delay", "1s")
                                                        .set("compression_dictionary_refresh_interval", "1s"))
                                      .start());
        // trained dictionaries are stored and read at QUORUM, which a single node reaches only with one replica
        cluster.schemaChange("ALTER KEYSPACE system_distributed WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        return cluster;
    }

    private static void prepareTable(Cluster cluster, int node)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text) " +
                                          "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));
        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
        for (int pk = 0; pk < ROWS; pk++)
            cluster.get(node).executeInternal(insert, pk, "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms");
        cluster.get(node).flush(KEYSPACE);
    }

    private static void train(IInvokableInstance node)
    {
        node.runOnInstance(() -> manager().train(true, Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, "10KiB",
                                                              TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, "1MiB")));
    }

    private static String trainingStatus(IInvokableInstance node)
    {
        return node.callOnInstance(() -> TrainingState.fromCompositeData(manager().getTrainingState()).status.toString());
    }

    private static CompressionDictionaryManager manager()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).compressionDictionaryManager();
    }

    private static void restart(IInvokableInstance node) throws Exception
    {
        node.shutdown().get();
        node.startup();
    }

    private static List<String> errors(IInvokableInstance node, long mark)
    {
        return node.logs().grep(mark, line -> line.startsWith("ERROR") && !line.contains("Strong self-ref loop detected")).getResult();
    }

    private static void await(String what, Callable<Boolean> condition) throws Exception
    {
        long deadline = nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (!condition.call())
        {
            assertThat(nanoTime()).as("waiting for %s", what).isLessThan(deadline);
            Thread.sleep(50);
        }
    }

    public static class BB
    {
        static final int REAL = 0;
        static final int BLOCK = 1;
        static final int IO_EXCEPTION = 2;
        static final int FS_ERROR = 3;

        // longer than the initial delay of the refresh, so the refresh runs while the keyspace is opening
        private static final long OPENING_MILLIS = 5000;

        public static volatile int read = REAL;
        public static volatile boolean sampling = false;
        public static volatile int retrieve = REAL;
        public static volatile int retrieveNotified = REAL;
        public static volatile boolean retrieving = false;
        public static final AtomicInteger refreshes = new AtomicInteger();
        private static boolean opened = false;

        static void install(ClassLoader classLoader, int node)
        {
            new ByteBuddy().rebase(SSTableChunkSampler.class)
                           .method(named("readChunk"))
                           .intercept(MethodDelegation.withDefaultConfiguration().filter(named("readChunk")).to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(SystemDistributedKeyspace.class)
                           .method(named("retrieveLatestCompressionDictionary").or(named("retrieveCompressionDictionary")))
                           .intercept(MethodDelegation.withDefaultConfiguration()
                                                      .filter(named("retrieveLatestCompressionDictionary").or(named("retrieveCompressionDictionary")))
                                                      .to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        static void installWithSlowOpening(ClassLoader classLoader, int node)
        {
            install(classLoader, node);
            new ByteBuddy().rebase(Keyspace.class)
                           .method(named("initCf"))
                           .intercept(MethodDelegation.withDefaultConfiguration().filter(named("initCf")).to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ByteBuffer readChunk(@SuperCall Callable<ByteBuffer> real) throws Exception
        {
            switch (read)
            {
                case BLOCK:
                    sampling = true;
                    try
                    {
                        new CountDownLatch(1).await();
                    }
                    catch (InterruptedException e)
                    {
                        // the real read fails as it would fail when interrupted, with ClosedByInterruptException
                        Thread.currentThread().interrupt();
                    }
                    return real.call();
                case IO_EXCEPTION:
                    throw new IOException("failed read");
                case FS_ERROR:
                    throw new FSReadError(new IOException("failed read"), "sstable");
                default:
                    return real.call();
            }
        }

        public static synchronized void initCf(@Argument(0) TableMetadata metadata, @SuperCall Callable<Void> real) throws Exception
        {
            if (metadata.keyspace.equals(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME) && !opened)
            {
                opened = true;
                Thread.sleep(OPENING_MILLIS);
                // a notification can come at any time, this thread is as good as the one of the event handler
                SystemDistributedKeyspace.retrieveCompressionDictionary(KEYSPACE, TABLE, "1", 1);
            }
            real.call();
        }

        public static CompressionDictionary retrieveLatestCompressionDictionary(@Argument(0) String keyspace, @Argument(1) String table, @Argument(2) String id, @SuperCall Callable<CompressionDictionary> real) throws Exception
        {
            refreshes.incrementAndGet();
            return retrieve(retrieve, real);
        }

        public static CompressionDictionary retrieveCompressionDictionary(@Argument(0) String keyspace, @Argument(1) String table, @Argument(2) String id, @Argument(3) long dictionaryId, @SuperCall Callable<CompressionDictionary> real) throws Exception
        {
            return retrieve(retrieveNotified, real);
        }

        private static CompressionDictionary retrieve(int mode, Callable<CompressionDictionary> real) throws Exception
        {
            switch (mode)
            {
                case BLOCK:
                    retrieving = true;
                    try
                    {
                        new CountDownLatch(1).await();
                    }
                    catch (InterruptedException e)
                    {
                        // an inline local read fails like this when interrupted
                        throw new FSReadError(new ClosedByInterruptException(), "sstable");
                    }
                    return null;
                case FS_ERROR:
                    throw new FSReadError(new IOException("failed read"), "sstable");
                default:
                    return real.call();
            }
        }
    }
}
