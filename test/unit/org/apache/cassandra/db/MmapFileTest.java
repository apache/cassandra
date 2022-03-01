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
package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

public class MmapFileTest
{
    @BeforeClass
    public static void setup()
    {
        // PathUtils touches StorageService which touches StreamManager which requires configs be setup
        DatabaseDescriptor.daemonInitialization();
    }


    /**
     * Verifies that {@link sun.misc.Cleaner} works and that mmap'd files can be deleted.
     */
    @Test
    public void testMmapFile() throws Exception
    {
        ObjectName bpmName = new ObjectName("java.nio:type=BufferPool,name=mapped");

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        Long mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
        Long mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");

        Assert.assertEquals("# of mapped buffers should be 0", Long.valueOf(0L), mmapCount);
        Assert.assertEquals("amount of mapped memory should be 0", Long.valueOf(0L), mmapMemoryUsed);

        File f1 = FileUtils.createTempFile("MmapFileTest1", ".bin");
        File f2 = FileUtils.createTempFile("MmapFileTest2", ".bin");
        File f3 = FileUtils.createTempFile("MmapFileTest2", ".bin");

        try
        {
            int size = 1024 * 1024;

            Util.setFileLength(f1, size);
            Util.setFileLength(f2, size);
            Util.setFileLength(f3,size);

            try (FileChannel channel = FileChannel.open(f1.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ))
            {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);

                mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
                mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
                Assert.assertEquals("mapped buffers don't work?", Long.valueOf(1L), mmapCount);
                Assert.assertTrue("mapped buffers don't work?", mmapMemoryUsed >= size);

                Assert.assertTrue(buffer.isDirect());

                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                
                FileUtils.clean(buffer);
            }

            mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
            mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
            Assert.assertEquals("# of mapped buffers should be 0", Long.valueOf(0L), mmapCount);
            Assert.assertEquals("amount of mapped memory should be 0", Long.valueOf(0L), mmapMemoryUsed);

            try (FileChannel channel = FileChannel.open(f2.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ))
            {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);

                // # of mapped buffers is == 1 here - seems that previous direct buffer for 'f1' is deallocated now

                mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
                mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
                Assert.assertEquals("mapped buffers don't work?", Long.valueOf(1L), mmapCount);
                Assert.assertTrue("mapped buffers don't work?", mmapMemoryUsed >= size);

                Assert.assertTrue(buffer.isDirect());

                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);

                FileUtils.clean(buffer);
            }

            mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
            mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
            Assert.assertEquals("# of mapped buffers should be 0", Long.valueOf(0L), mmapCount);
            Assert.assertEquals("amount of mapped memory should be 0", Long.valueOf(0L), mmapMemoryUsed);

            try (FileChannel channel = FileChannel.open(f3.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ))
            {
                MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);

                mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
                mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
                Assert.assertEquals("mapped buffers don't work?", Long.valueOf(1L), mmapCount);
                Assert.assertTrue("mapped buffers don't work?", mmapMemoryUsed >= size);

                Assert.assertTrue(buffer.isDirect());

                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);
                buffer.putInt(42);

                FileUtils.clean(buffer);
            }

            mmapCount = (Long) mbs.getAttribute(bpmName, "Count");
            mmapMemoryUsed = (Long) mbs.getAttribute(bpmName, "MemoryUsed");
            Assert.assertEquals("# of mapped buffers should be 0", Long.valueOf(0L), mmapCount);
            Assert.assertEquals("amount of mapped memory should be 0", Long.valueOf(0L), mmapMemoryUsed);

            Assert.assertTrue(f1.tryDelete());
            Assert.assertTrue(f2.tryDelete());
            Assert.assertTrue(f3.tryDelete());
        }
        finally
        {
            Runtime.getRuntime().gc();
            f1.tryDelete();
            f2.tryDelete();
            f3.tryDelete();
        }
    }
}
