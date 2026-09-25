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

package org.apache.cassandra.distributed.test.sai;

import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.FieldValue;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraEntireSSTableStreamWriter;
import org.apache.cassandra.db.streaming.ComponentContext;
import org.apache.cassandra.db.streaming.ComponentManifest;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the CASSANDRA-21520 contract: a concurrent SAI index rebuild fails fast while an
 * entire-sstable (zero-copy) stream is in flight, and the stream completes with correct data on the receiver.
 *
 * <p>Entire-sstable-streaming (ZCS) first records every streamable component and its size in a
 * {@link org.apache.cassandra.db.streaming.ComponentManifest}, serializes and flushes that manifest to the
 * peer, and only then copies each component file verbatim onto the wire. The sender now acquires the per-sstable
 * ZCS streaming status when the outgoing file is constructed, before the manifest is advertised, so a concurrent
 * SAI rebuild must be rejected rather than deleting and rewriting index components underneath the in-flight stream.</p>
 *
 * <p>This test forces exactly that interleaving:
 * <ol>
 *     <li>creates an SAI index and flushes a single sstable on the sender (node1),</li>
 *     <li>triggers ZCS from the receiver (node2) and pauses the sender right after the manifest has been sent,</li>
 *     <li>attempts a blocking SAI index rebuild on the sender while the stream is paused,</li>
 *     <li>asserts that the rebuild is rejected, then resumes streaming and verifies the receiver has correct data.</li>
 * </ol>
 */
public class IndexRebuildDuringEntireSSTableStreamingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String INDEX = "sai_idx";
    private static final int ROWS = 200;

    @Test
    public void testIndexRebuildWhileEntireSSTableStreaming() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("stream_entire_sstables", true)
                                                             .set("autocompaction_on_startup_enabled", false))
                                           // Only the sender (node1) needs the streaming pause installed.
                                           .withInstanceInitializer((cl, num) -> {
                                               if (num == 1)
                                                   BBHelper.install(cl);
                                           })
                                           .start()))
        {
            cluster.disableAutoCompaction(KEYSPACE);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX " + INDEX + " ON %s." + TABLE + "(v) USING 'sai'"));
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, INDEX);

            IInvokableInstance node1 = cluster.get(1); // sender
            IInvokableInstance node2 = cluster.get(2); // receiver

            // Populate the sender only (executeInternal bypasses replication) and flush to a single sstable that
            // carries the SAI components. node2 owns the same range (RF == cluster size) but has no data yet.
            for (int i = 0; i < ROWS; i++)
                node1.executeInternal(withKeyspace("INSERT INTO %s." + TABLE + "(pk, v) VALUES (?, ?)"), i, "v" + i);
            node1.flush(KEYSPACE);

            // Arm the sender-side pause so only the upcoming ZCS stream is intercepted.
            node1.runOnInstance(() -> BBHelper.armed.set(true));

            // Kick off the streaming asynchronously: node2 rebuilds its data from node1. The call blocks until
            // streaming completes, and streaming is about to block on the sender, so run it off-thread.
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try
            {
                Future<NodeToolResult> streaming =
                    executor.submit(() -> node2.nodetoolResult("rebuild", "--keyspace", KEYSPACE));

                // Wait until the sender has serialized and flushed the manifest and is paused before opening the
                // outgoing component channels.
                node1.runOnInstance(() -> BBHelper.manifestSent.awaitUninterruptibly(2, TimeUnit.MINUTES));

                // While the stream is paused mid-flight (status is already ZCS_STREAMING for this sstable), a
                // concurrent SAI rebuild must fail fast rather than mutate components underneath the in-flight
                // stream. CASSANDRA-21520.
                boolean rebuildRejected = node1.callOnInstance(IndexRebuildDuringEntireSSTableStreamingTest::rebuildAndReturnRejected);
                assertThat(rebuildRejected)
                    .describedAs("SAI rebuild must fail fast while entire-sstable streaming is in progress")
                    .isTrue();
                assertThat(node1.logs()
                                .grep("while entire-sstable \\(zero-copy\\) streaming is in progress")
                                .getResult())
                    .describedAs("Expected the rebuild to be rejected due to active streaming")
                    .isNotEmpty();

                // Let the paused stream continue; its components were never mutated, so it completes cleanly.
                node1.runOnInstance(() -> BBHelper.proceed.decrement());

                NodeToolResult result = streaming.get(3, TimeUnit.MINUTES);
                result.asserts().success();
            }
            finally
            {
                executor.shutdownNow();
            }

            for (int i = 0; i < ROWS; i++)
            {
                Object[][] byPrimaryKey = node2.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE pk = ?"), i);
                assertThat(byPrimaryKey.length)
                    .describedAs("Receiver must retain streamed data (pk=%d)", i)
                    .isEqualTo(1);
                Object[][] byIndex = node2.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE v = ?"), "v" + i);
                assertThat(byIndex.length)
                    .describedAs("Receiver SAI index must return streamed row (v%d)", i)
                    .isEqualTo(1);
                assertThat(byIndex[0][0]).isEqualTo(i);
            }

            // A rejected full rebuild leaves the index marked for rebuild - this is consistent with existing
            // full-rebuild failure semantics, since markIndexesBuilding makes the index non-queryable before the
            // rejection happens. The sender's data is intact, and once streaming has completed and released the
            // status a fresh rebuild succeeds - proving the rejection was clean and the SAI components on the sender
            // were never deleted or corrupted underneath the stream.
            for (int i = 0; i < ROWS; i++)
            {
                Object[][] onSender = node1.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE pk = ?"), i);
                assertThat(onSender.length).describedAs("Sender data must be intact (pk=%d)", i).isEqualTo(1);
            }

            node1.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(INDEX));
            });
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, INDEX);

            for (int i = 0; i < ROWS; i++)
            {
                Object[][] onSender = node1.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE v = ?"), "v" + i);
                assertThat(onSender.length).describedAs("Sender index must be queryable after re-rebuild (v%d)", i).isEqualTo(1);
                assertThat(onSender[0][0]).isEqualTo(i);
            }
        }
    }

    private static boolean rebuildAndReturnRejected()
    {
        try
        {
            // Drive the real user-facing rebuild path (nodetool rebuild_index). It routes through
            // SecondaryIndexManager.buildIndexesBlocking -> StorageAttachedIndexBuildingSupport.getIndexBuildTask,
            // which reserves the per-sstable rebuild status BEFORE deleting any SAI components. With a stream in
            // flight that reservation must fail fast, so no component is deleted underneath the stream.
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(INDEX));
            return false;
        }
        catch (Throwable t)
        {
            org.slf4j.LoggerFactory.getLogger(IndexRebuildDuringEntireSSTableStreamingTest.class).error(t.getMessage(), t);
            return true;
        }
    }

    public static class BBHelper
    {
        // Per-instance state, resolved in the sender's classloader.
        static final AtomicBoolean armed = new AtomicBoolean(false);
        static final AtomicBoolean paused = new AtomicBoolean(false);
        static final CountDownLatch manifestSent = CountDownLatch.newCountDownLatch(1);
        static final CountDownLatch proceed = CountDownLatch.newCountDownLatch(1);

        /**
         * Intercepts {@link CassandraEntireSSTableStreamWriter#write}. On entry the caller has already serialized and
         * flushed the component manifest, so this is the "manifest sent, channels not yet opened" point. Print the
         * manifest that was just sent (component -> advertised size), then pause once so the test can mutate the SAI
         * components before the component channels are created.
         */
        @SuppressWarnings("unused")
        public static void write(@FieldValue("manifest") ComponentManifest manifest, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (armed.get() && paused.compareAndSet(false, true))
            {
                StringJoiner sj = new StringJoiner(", ", "{", "}");
                for (Component component : manifest.components())
                    sj.add(component.name + '=' + manifest.sizeOf(component));
                System.out.println("[ZCS-VERIFY] manifest sent (component=advertisedSize): " + sj);

                manifestSent.decrement();
                proceed.awaitUninterruptibly(2, TimeUnit.MINUTES);
            }
            zuper.call();
        }

        /**
         * Intercepts {@link ComponentContext#channel}. This is invoked for every component right before it is copied
         * onto the wire. Print the component name, the size advertised in the manifest, and the actual on-disk size
         * at stream time so the divergence introduced by the concurrent rebuild is visible (the component whose size
         * changed will not match, which is what fails the stream).
         */
        @SuppressWarnings("unused")
        public static FileChannel channel(@AllArguments Object[] args, @SuperCall Callable<FileChannel> zuper) throws Exception
        {
            Component component = (Component) args[1];
            long advertised = (Long) args[2];
            try
            {
                FileChannel channel = zuper.call();
                System.out.println("[ZCS-VERIFY] streaming component " + component.name +
                                   ": advertisedSize=" + advertised + ", actualSize=" + channel.size());
                return channel;
            }
            catch (Throwable t)
            {
                System.out.println("[ZCS-VERIFY] streaming component " + component.name +
                                   ": advertisedSize=" + advertised + " -> MISMATCH: " + t.getMessage());
                throw t;
            }
        }

        public static void install(ClassLoader classLoader)
        {
            new ByteBuddy().rebase(CassandraEntireSSTableStreamWriter.class)
                           .method(named("write").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(ComponentContext.class)
                           .method(named("channel").and(takesArguments(3)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
