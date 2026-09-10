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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.fail;

/**
 * Regression test for the TrieMemtable shard-lock / flush-barrier deadlock
 * (CASSANDRA-21019): a post-barrier write holding a shard writeLock parked for pool
 * room strands pre-barrier writes queued on the lock, where markBlocking() cannot
 * release them, and writeBarrier.await() never completes.
 */
public class TrieMemtableShardLockDeadlockTest
{
    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.prepareServer();
    }

    @Test(timeout = 60_000)
    public void writeBarrierMustCompleteDespiteShardLockQueue() throws Exception
    {
        TableMetadata tm = TableMetadata.builder("ks", "t")
                                        .addPartitionKeyColumn("pk", Int32Type.instance)
                                        .addRegularColumn("v", BytesType.instance)
                                        .build();

        Memtable.Factory factory = TrieMemtable.factory(new HashMap<>());
        Memtable mt = factory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(tm), OWNER);
        OpOrder order = new OpOrder(); // stands in for Keyspace.writeOrder
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;

        OpOrder.Group g1 = order.start();          // W1's op: PRE-barrier

        OpOrder.Barrier barrier = order.newBarrier();
        barrier.issue();
        barrier.markBlocking();                    // exactly what ColumnFamilyStore.Flush.run does

        // Exhaust the shared pool that every AbstractAllocatorMemtable allocates from; a memtable
        // cannot be bound to a private pool. maybeClean() may fire and flush other memtables, but
        // it cannot release this synthetic amount, so belowLimit() stays false and both writers
        // stay gated for the duration of the test.
        pool.onHeap.allocated(pool.onHeap.limit);
        pool.offHeap.allocated(pool.offHeap.limit);
        try
        {
            OpOrder.Group g2 = order.start();      // W2's op: POST-barrier
            Thread w2 = run("w2", () -> { mt.checkSpaceAndPut(update(tm), UpdateTransaction.NO_OP, g2); g2.close(); });
            waitUntilBlockedAtOrDone(w2, "SubAllocator"); // unpatched: lock holder in allocate(); patched: parked in awaitRoom() before the lock

            Thread w1 = run("w1", () -> { mt.checkSpaceAndPut(update(tm), UpdateTransaction.NO_OP, g1); g1.close(); });
            waitUntilBlockedAtOrDone(w1, "MemtableShard", "ReentrantLock");

            Thread flush = run("flush", barrier::await);
            flush.join(15_000);
            if (flush.isAlive())
                fail("DEADLOCK (CASSANDRA-21019 regression): writeBarrier.await() did not complete in 15s.\n" + dump(flush, w1, w2));
        }
        finally
        {
            pool.onHeap.released(pool.onHeap.limit);
            pool.offHeap.released(pool.offHeap.limit);
        }
    }

    private static PartitionUpdate update(TableMetadata tm)
    {
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(tm, 42);
        b.row().add("v", ByteBuffer.allocate(64));
        return b.build();
    }

    private static Thread run(String name, Runnable r)
    {
        Thread t = new Thread(r, name); t.setDaemon(true); t.start(); return t;
    }

    private static void waitUntilBlockedAtOrDone(Thread t, String... frameSubstrings) throws InterruptedException
    {
        for (long deadline = System.nanoTime() + 30_000_000_000L; System.nanoTime() < deadline; Thread.sleep(50))
        {
            if (!t.isAlive())
                return;
            String stack = java.util.Arrays.toString(t.getStackTrace());
            boolean all = true;
            for (String s : frameSubstrings)
                all &= stack.contains(s);
            if (all)
                return;
        }
        throw new AssertionError(t.getName() + " neither finished nor reached " + String.join("+", frameSubstrings));
    }

    private static String dump(Thread... threads)
    {
        StringBuilder sb = new StringBuilder();
        for (Thread t : threads)
        {
            sb.append('"').append(t.getName()).append("\" ").append(t.getState()).append('\n');
            StackTraceElement[] stack = t.getStackTrace();
            for (int i = 0; i < Math.min(stack.length, 12); i++)
                sb.append("    at ").append(stack[i]).append('\n');
        }
        return sb.toString();
    }

    private static final Memtable.Owner OWNER = new Memtable.Owner()
    {
        public org.apache.cassandra.utils.concurrent.Future<CommitLogPosition> signalFlushRequired(Memtable m, ColumnFamilyStore.FlushReason r) { return null; }
        public Memtable getCurrentMemtable() { return null; }
        public Iterable<Memtable> getIndexMemtables() { return Collections.emptyList(); }
        public ShardBoundaries localRangeSplits(int shardCount) { return ShardBoundaries.NONE; }
    };
}
