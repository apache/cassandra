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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.assertj.core.api.Assertions;

public class MemoryUtilTest
{

    @Test
    public void testClean()
    {
        // We assert direct pool state changes as no ByteBuffer state changes are observable as a result of cleaner
        // executing.
        BufferPoolMXBean directPool = getDirectBufferPool();

        int bufferSize = 1024 * 1024; // some non-insignificant size
        ByteBuffer original = ByteBuffer.allocateDirect(bufferSize);

        long memoryUsedBefore = directPool.getMemoryUsed();
        MemoryUtil.clean(original);
        long memoryUsedAfter = directPool.getMemoryUsed();

        Assert.assertEquals("Direct memory used should decrease by buffer capacity",
                            // Allow 5% tolerance for other activities
                            memoryUsedBefore - bufferSize, memoryUsedAfter, bufferSize * 0.05);
    }

    @Test
    public void testCleanViewDoesNotThrow()
    {
        // Use a large buffer to likely get mmap'd memory from the OS. This ensures that if cleaning a view incorrectly
        // unmaps the original buffer's memory, subsequent access to 'original' would more reliably fail.
        // For context: glibc's mmap threshold is 32MB on 64-bit systems
        ByteBuffer original = ByteBuffer.allocateDirect(64 * 1024 * 1024);

        ByteBuffer slice = original.slice();
        MemoryUtil.clean(slice);
        try
        {
            original.putInt(10);
        }
        catch (Exception exc)
        {
            Assertions.fail("Unable to write to original buffer after cleaning (slice). " + exc.getMessage(), exc);
        }

        ByteBuffer duplicate = original.duplicate();
        MemoryUtil.clean(duplicate);

        try
        {
            original.putInt(10);
        }
        catch (Exception exc)
        {
            Assertions.fail("Unable to write to original buffer after cleaning (duplicate). " + exc.getMessage(), exc);
        }
    }

    @Test
    public void testCleanNonDirectDoesNotThrow()
    {
        ByteBuffer original = ByteBuffer.allocate(16);
        MemoryUtil.clean(original);
    }

    private static BufferPoolMXBean getDirectBufferPool()
    {
        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools)
        {
            if (pool.getName().equals("direct"))
            {
                return pool;
            }
        }
        throw new IllegalArgumentException("Direct buffer pool not found");
    }
}