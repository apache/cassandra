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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * CASSANDRA-21019: putNested() must never wait for pool room there; put() must still apply back-pressure.
 */
public class MemtableNestedPutSkipsRoomWaitTest
{
    @BeforeClass
    public static void setup()
    {
        ServerTestUtils.prepareServer();
    }

    @Test(timeout = 60_000)
    public void nestedPutMustNotWaitForRoom() throws Exception
    {
        TableMetadata tmBase = metadata("base");
        TableMetadata tmIndex = metadata("index");
        Memtable.Factory factory = SkipListMemtable.FACTORY;
        Memtable base = factory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(tmBase), OWNER);
        Memtable index = factory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(tmIndex), OWNER);
        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        OpOrder.Group g = new OpOrder().start();
        AtomicBoolean claimed = new AtomicBoolean();

        UpdateTransaction tx = new UpdateTransaction()
        {
            public void start() {}
            public void onPartitionDeletion(DeletionTime deletionTime) {}
            public void onRangeTombstone(RangeTombstone rangeTombstone) {}
            public void onInserted(Row row)
            {
                pool.onHeap.allocated(pool.onHeap.limit);
                pool.offHeap.allocated(pool.offHeap.limit);
                claimed.set(true);
                index.putNested(update(tmIndex), UpdateTransaction.NO_OP, g);
            }
            public void onUpdated(Row existing, Row updated) {}
            public void commit() {}
        };

        Thread writer = new Thread(() -> { base.put(update(tmBase), tx, g); g.close(); }, "base-writer");
        writer.setDaemon(true);
        writer.start();
        try
        {
            writer.join(10_000);
            if (writer.isAlive())
            {
                StringBuilder sb = new StringBuilder("nested putNested() waited for room under the base mutation:\n");
                for (StackTraceElement f : writer.getStackTrace())
                    sb.append("    at ").append(f).append('\n');
                fail(sb.toString());
            }
        }
        finally
        {
            if (claimed.get())
            {
                pool.onHeap.released(pool.onHeap.limit);
                pool.offHeap.released(pool.offHeap.limit);
            }
        }
    }

    @Test(timeout = 60_000)
    public void topLevelPutGatesAtLimit() throws Exception
    {
        TableMetadata tm = metadata("gated");
        Memtable.Factory factory = SkipListMemtable.FACTORY;

        Memtable mt = factory.create(new AtomicReference<>(CommitLogPosition.NONE), TableMetadataRef.forOfflineTools(tm), OWNER);

        MemtablePool pool = AbstractAllocatorMemtable.MEMORY_POOL;
        OpOrder.Group g = new OpOrder().start();
        pool.onHeap.allocated(pool.onHeap.limit);
        pool.offHeap.allocated(pool.offHeap.limit);
        try
        {
            Thread writer = new Thread(() -> { mt.put(update(tm), UpdateTransaction.NO_OP, g); g.close(); }, "gated-writer");
            writer.setDaemon(true);
            writer.start();
            writer.join(2_000);
            assertTrue("top-level put() did not wait for room at the pool limit", writer.isAlive());
        }
        finally
        {
            pool.onHeap.released(pool.onHeap.limit);
            pool.offHeap.released(pool.offHeap.limit);
        }
    }

    private static TableMetadata metadata(String table)
    {
        return TableMetadata.builder("ks", table)
                            .addPartitionKeyColumn("pk", Int32Type.instance)
                            .addRegularColumn("v", BytesType.instance)
                            .build();
    }

    private static PartitionUpdate update(TableMetadata tm)
    {
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(tm, 42);
        b.row().add("v", ByteBuffer.allocate(64));
        return b.build();
    }

    private static final Memtable.Owner OWNER = new Memtable.Owner()
    {
        public Future<CommitLogPosition> signalFlushRequired(Memtable m, ColumnFamilyStore.FlushReason r) { return null; }
        public Memtable getCurrentMemtable() { return null; }
        public Iterable<Memtable> getIndexMemtables() { return Collections.emptyList(); }
        public ShardBoundaries localRangeSplits(int shardCount) { return ShardBoundaries.NONE; }
    };
}
