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

package org.apache.cassandra.db.marshal;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;
import org.apache.cassandra.utils.memory.NativePool;

/**
 * A primitive NativeData allocator is used for test purposes only
 * It releases memory only when releaseMemory() or close() are called
 */
public class TestNativeDataAllocator implements NativeDataAllocator, Closeable
{
    private final NativePool nativePool = new NativePool(0, 10 * 1024 * 1024, 1.0f,
                                                         () -> ImmediateFuture.success(true));
    private NativeAllocator nativeAllocator = nativePool.newAllocator("test");
    private final OpOrder order = new OpOrder();

    @Override
    public NativeData allocateBasedOnBuffer(ByteBuffer data)
    {
        try(OpOrder.Group group = order.start())
        {
            long address = nativeAllocator.allocate(data.remaining(), group);
            MemoryUtil.setBytes(address, data);
            return new AddressBasedNativeData(address, data.remaining());
        }
    }

    public void releaseMemory() {
        nativeAllocator.setDiscarding();
        nativeAllocator.setDiscarded();
        nativeAllocator = nativePool.newAllocator("test");
    }

    public void close() {
        nativeAllocator.setDiscarding();
        nativeAllocator.setDiscarded();
        try
        {
            nativePool.shutdownAndWait(5, TimeUnit.SECONDS);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

}
