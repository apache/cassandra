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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertTrue;

public class HintsWriteExecutorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * The write buffer is direct memory, and the hints service is shut down and started again within one process by
     * the in-JVM test framework, so every shutdown that keeps the buffer costs another {@code WRITE_BUFFER_SIZE}.
     */
    @Test
    public void testShutdownReleasesTheWriteBuffer() throws IOException
    {
        File directory = new File(temporaryFolder.newFolder().toPath());
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());

        long before = directMemoryUsed();
        HintsWriteExecutor executor = new HintsWriteExecutor(catalog);
        long allocated = directMemoryUsed() - before;
        assertTrue("Expected at least " + HintsWriteExecutor.WRITE_BUFFER_SIZE + " bytes of direct memory, got " + allocated,
                   allocated >= HintsWriteExecutor.WRITE_BUFFER_SIZE);

        executor.shutdownBlocking();
        long retained = directMemoryUsed() - before;
        assertTrue("Expected the write buffer to be released, " + retained + " bytes of direct memory are still held",
                   retained < HintsWriteExecutor.WRITE_BUFFER_SIZE);
    }

    private static long directMemoryUsed()
    {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            if (pool.getName().equals("direct"))
                return pool.getMemoryUsed();

        throw new AssertionError("The JVM reports no direct buffer pool");
    }
}
