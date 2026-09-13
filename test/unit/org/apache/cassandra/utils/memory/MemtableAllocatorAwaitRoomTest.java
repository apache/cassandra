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

import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-21019: heap_buffers / unslabbed_heap_buffers create their pool with an
 * off-heap limit of 0; a zero-limit sub-pool is never allocated from and never
 * signalled, so awaitRoom() must skip it, and configured limits must still gate.
 * awaitRoom() is also the only place that waits: accounting an allocation must not.
 */
public class MemtableAllocatorAwaitRoomTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test(timeout = 30_000)
    public void zeroLimitPoolDoesNotBlock() throws Exception
    {
        SlabPool pool = new SlabPool(1 << 20, 0, 1.0f, () -> ImmediateFuture.success(true));
        try
        {
            MemtableAllocator allocator = pool.newAllocator("test");
            OpOrder.Group g = new OpOrder().start();
            Thread gate = run(() -> allocator.awaitRoomToStart(g));
            gate.join(5_000);
            assertFalse("awaitRoomToStart hung on the unused zero-limit off-heap pool", gate.isAlive());
            g.close();
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    @Test(timeout = 30_000)
    public void configuredLimitStillGates() throws Exception
    {
        SlabPool pool = new SlabPool(1 << 20, 1 << 20, 1.0f, () -> ImmediateFuture.success(true));
        try
        {
            MemtableAllocator allocator = pool.newAllocator("test");
            OpOrder.Group g = new OpOrder().start();
            pool.onHeap.allocated(pool.onHeap.limit);
            Thread gate = run(() -> allocator.awaitRoomToStart(g));
            gate.join(2_000);
            assertTrue("gate did not wait while the on-heap pool was at its limit", gate.isAlive());
            pool.onHeap.released(pool.onHeap.limit);
            gate.join(10_000);
            assertFalse("gate did not wake after room was released", gate.isAlive());
            g.close();
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    /**
     * The path Memtable.markExtraOnHeapUsed takes, and with it every allocation an already-started mutation
     * makes. SAI, SASI and the Accord route index all account index memory from an indexer callback, under the
     * base table's memtable-internal locks, so this path must record and return however far over the limit the
     * pool is; parking here is what CASSANDRA-21019 removed.
     */
    @Test(timeout = 30_000)
    public void accountingDoesNotWaitAboveTheLimit() throws Exception
    {
        SlabPool pool = new SlabPool(1 << 20, 1 << 20, 1.0f, () -> ImmediateFuture.success(true));
        try
        {
            MemtableAllocator allocator = pool.newAllocator("test");
            pool.onHeap.allocated(pool.onHeap.limit);
            Thread mark = run(() -> allocator.onHeap().allocate(1024));
            mark.join(5_000);
            assertFalse("accounting waited for room while the on-heap pool was over its limit", mark.isAlive());
            assertTrue("the accounting was not recorded", allocator.onHeap().owns() >= 1024);
        }
        finally
        {
            pool.shutdownAndWait(1, TimeUnit.MINUTES);
        }
    }

    private static Thread run(Runnable r)
    {
        Thread t = new Thread(r, "await-room");
        t.setDaemon(true);
        t.start();
        return t;
    }
}
