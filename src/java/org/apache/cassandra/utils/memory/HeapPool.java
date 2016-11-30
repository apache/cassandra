/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.concurrent.OpOrder;

public class HeapPool extends MemtablePool
{
    public HeapPool(long maxOnHeapMemory, float cleanupThreshold, Runnable cleaner)
    {
        super(maxOnHeapMemory, 0, cleanupThreshold, cleaner);
    }

    public MemtableAllocator newAllocator()
    {
        return new Allocator(this);
    }

    private static class Allocator extends MemtableBufferAllocator
    {
        private static final EnsureOnHeap ENSURE_NOOP = new EnsureOnHeap.NoOp();
        Allocator(HeapPool pool)
        {
            super(pool.onHeap.newAllocator(), pool.offHeap.newAllocator());
        }

        public ByteBuffer allocate(int size, OpOrder.Group opGroup)
        {
            super.onHeap().allocate(size, opGroup);
            return ByteBuffer.allocate(size);
        }

        public EnsureOnHeap ensureOnHeap()
        {
            return ENSURE_NOOP;
        }
    }
}
