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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the CASSANDRA-21520 safety net for entire-sstable (zero-copy) streaming: if a streamed SAI component
 * changes size underneath an in-flight stream, the sender must fail the stream rather than ship bytes that
 * disagree with the already-sent {@link ComponentManifest}.
 *
 * <p>While the sender is paused after sending the manifest (component -> advertised size) but before opening the
 * component channels, the test directly truncates one of the streamed SAI component files. SAI components are not
 * part of {@code mutableComponents()}, so they are not hard-linked and are streamed from the live file. When the
 * writer opens the channel, the size check in {@link ComponentContext#channel} detects the mismatch and fails the
 * stream. This check is a real exception (not an {@code assert}), so it is effective in production where assertions
 * are disabled.</p>
 *
 * <p>The failed streaming transaction must be rolled back on the receiver, which must not expose any partial or
 * size-mismatched data.</p>
 */
public class IndexDeleteDuringEntireSSTableStreamingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String INDEX = "sai_idx";
    private static final int ROWS = 200;

    @Test
    public void testIndexDeleteWhileEntireSSTableStreaming() throws Exception
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
                // While the stream is paused mid-flight (after the manifest advertising each component's size has
                // been sent, before the component channels are opened), directly truncate one of the streamed SAI
                // component files on the sender. SAI components are not part of mutableComponents(), so they are not
                // hard-linked and are streamed from the live file. When the writer opens the channel the on-disk
                // size no longer matches the size advertised in the manifest, and the production size check in
                // ComponentContext.channel() must fail the stream rather than ship corrupt bytes. This check must be
                // effective even with assertions disabled (it is a real exception, not an assert). CASSANDRA-21520.
                node1.runOnInstance(IndexDeleteDuringEntireSSTableStreamingTest::truncateStreamedSaiComponent);

                // Let the paused stream continue now that a component has been mutated underneath it.
                node1.runOnInstance(() -> BBHelper.proceed.decrement());

                NodeToolResult result = streaming.get(3, TimeUnit.MINUTES);
                result.asserts().failure();
                assertThat(node1.logs()
                                .grep("Entire sstable streaming expects .* file size to be .* but got")
                                .getResult())
                    .describedAs("Expected the ComponentContext size check to fail the stream")
                    .isNotEmpty();
            }
            finally
            {
                executor.shutdownNow();
            }

            for (int i = 0; i < ROWS; i++)
            {
                assertThat(node2.executeInternal(withKeyspace("SELECT pk FROM %s." + TABLE + " WHERE pk = ?"), i))
                    .describedAs("Receiver must not retain data from the failed stream (pk=%d)", i)
                    .isEmpty();
            }
        }
    }

    private static void truncateStreamedSaiComponent()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        for (Component component : sstable.getStreamingComponents())
        {
            // Only mutate a component that is streamed from the live file (not a hard-linked mutable component and
            // not a primary component), i.e. a SAI index component, so the size divergence hits ComponentContext.
            if (sstable.descriptor.getFormat().primaryComponents().contains(component))
                continue;
            if (sstable.descriptor.getFormat().mutableComponents().contains(component))
                continue;
            File file = sstable.descriptor.fileFor(component);
            if (!file.exists() || file.length() == 0)
                continue;
            try (FileChannel channel = file.newReadWriteChannel())
            {
                channel.truncate(Math.max(0, channel.size() - 1));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return;
        }
        throw new IllegalStateException("No streamed SAI component available to mutate");
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
         * manifest that was just sent (component -> advertised size), then pause once so the test can truncate a
         * streamed SAI component before the component channels are created.
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
         * at stream time.
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
                                   ": advertisedSize=" + advertised + " -> FAILED TO OPEN: " + t);
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
