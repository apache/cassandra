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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraEntireSSTableStreamWriter;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * CASSANDRA-21520 (reverse direction): when an SAI index rebuild is in progress on the sender, a new
 * entire-sstable (zero-copy) stream of the same sstable must degrade to legacy section-based streaming, so the
 * rebuild and the stream never mutate/ship the SAI components concurrently. The receiver still ends up with a
 * correct, queryable index (it rebuilds it locally from the legacy stream).
 *
 * <p>The test deterministically models an in-progress rebuild by reserving the per-sstable rebuild status on the
 * sender (exactly what {@code StorageAttachedIndexBuildingSupport.getIndexBuildTask} does before dropping SAI
 * components), then streams from the receiver. It asserts that the sender never invokes
 * {@link CassandraEntireSSTableStreamWriter#write} (i.e. it fell back to legacy) and that the receiver ends up
 * with the streamed data and a queryable SAI index.</p>
 */
public class StreamingDuringIndexRebuildTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String INDEX = "sai_idx";
    private static final int ROWS = 200;

    @Test
    public void testStreamingFallsBackToLegacyDuringRebuild() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("stream_entire_sstables", true)
                                                             .set("autocompaction_on_startup_enabled", false))
                                           // Only the sender (node1) needs the entire-sstable writer counter installed.
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

            for (int i = 0; i < ROWS; i++)
                node1.executeInternal(withKeyspace("INSERT INTO %s." + TABLE + "(pk, v) VALUES (?, ?)"), i, "v" + i);
            node1.flush(KEYSPACE);

            // Model an in-progress SAI rebuild on the sender by reserving the per-sstable rebuild status for every
            // live sstable. While this is held, an entire-sstable stream of the same sstable must fall back to legacy.
            node1.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                cfs.getLiveSSTables().forEach(sstable -> {
                    if (!sstable.streamRebuildState().tryBeginRebuild())
                        throw new IllegalStateException("Could not reserve rebuild status for " + sstable.descriptor);
                });
            });

            node1.runOnInstance(() -> BBHelper.armed.set(true));

            try
            {
                // node2 rebuilds its data from node1. The sender must NOT use entire-sstable streaming while a
                // rebuild holds the sstable, so it degrades to legacy streaming (blocking call).
                NodeToolResult result = node2.nodetoolResult("rebuild", "--keyspace", KEYSPACE);
                result.asserts().success();

                int entireWrites = node1.callOnInstance(() -> BBHelper.entireSSTableWrites.get());
                assertThat(entireWrites)
                    .describedAs("Entire-sstable streaming must not run while a rebuild holds the sstable")
                    .isZero();
            }
            finally
            {
                // Release the reserved status so the sender returns to normal.
                node1.runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                    cfs.getLiveSSTables().forEach(sstable -> sstable.streamRebuildState().endRebuild());
                });
            }

            // The receiver built its SAI index locally from the legacy stream and must expose the streamed rows.
            SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, INDEX);
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
        }
    }

    public static class BBHelper
    {
        static final AtomicBoolean armed = new AtomicBoolean(false);
        static final AtomicInteger entireSSTableWrites = new AtomicInteger(0);

        /**
         * Counts invocations of {@link CassandraEntireSSTableStreamWriter#write}. Any invocation while armed means
         * entire-sstable (zero-copy) streaming was used rather than the expected legacy fallback.
         */
        @SuppressWarnings("unused")
        public static void write(@SuperCall Callable<Void> zuper) throws Exception
        {
            if (armed.get())
                entireSSTableWrites.incrementAndGet();
            zuper.call();
        }

        public static void install(ClassLoader classLoader)
        {
            new ByteBuddy().rebase(CassandraEntireSSTableStreamWriter.class)
                           .method(named("write").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
