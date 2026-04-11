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

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

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
    public void testCleanSliceThrows()
    {
        ByteBuffer original = ByteBuffer.allocateDirect(1024);
        ByteBuffer slice = original.slice();

        Assertions.assertThatThrownBy(() -> MemoryUtil.clean(slice))
                  .isInstanceOf(IllegalArgumentException.class);

        // original should still be usable after the rejected clean
        original.putInt(10);
        MemoryUtil.clean(original);
    }

    @Test
    public void testCleanDuplicateThrows()
    {
        ByteBuffer original = ByteBuffer.allocateDirect(1024);
        ByteBuffer duplicate = original.duplicate();

        Assertions.assertThatThrownBy(() -> MemoryUtil.clean(duplicate))
                  .isInstanceOf(IllegalArgumentException.class);

        // original should still be usable after the rejected clean
        original.putInt(10);
        MemoryUtil.clean(original);
    }

    @Test
    public void testCleanNonDirectDoesNotThrow()
    {
        ByteBuffer original = ByteBuffer.allocate(16);
        MemoryUtil.clean(original);
    }

    @Test
    public void testCleanWithNonNullAttachmentAndCleanerSucceeds()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        MemoryUtil.setAttachment(buffer, (Runnable) () -> {});
        MemoryUtil.clean(buffer);
    }

    @Test
    public void testCleanNoCleanerWithAttachmentThrows()
    {
        ByteBuffer hollow = MemoryUtil.getHollowDirectByteBuffer();
        MemoryUtil.setAttachment(hollow, new Object());

        Assertions.assertThatThrownBy(() -> MemoryUtil.clean(hollow))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("does not own its memory");
    }

    @Test
    public void testCleanNoCleanerNoAttachmentIsNoOp()
    {
        ByteBuffer hollow = MemoryUtil.getHollowDirectByteBuffer();
        MemoryUtil.clean(hollow);
    }

    @Test
    public void testCleanWithCleanerButUnexpectedAttachmentThrows()
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
        MemoryUtil.setAttachment(buffer, new Object());

        Assertions.assertThatThrownBy(() -> MemoryUtil.clean(buffer))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("unexpected attachment type");

        MemoryUtil.setAttachment(buffer, null);
        MemoryUtil.clean(buffer);
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