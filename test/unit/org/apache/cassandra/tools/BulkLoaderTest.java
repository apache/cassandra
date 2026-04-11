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

package org.apache.cassandra.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BulkLoaderTest extends OfflineToolUtils
{
    private static final String[] ALLOWED_BULK_LOADER_THREADS = new String[]{
            "ObjectCleanerThread",
            "globalEventExecutor-[1-9]-[1-9]",
            "globalEventExecutor-[1-9]-[1-9]",
            "Shutdown-checker",
            "cluster[0-9]-connection-reaper-[0-9]",
            "Attach Listener",
            "process reaper",
            "JNA Cleaner",
    };

    // the driver isn't expected to terminate threads on close synchronously (CASSANDRA-19000)
    private static final String SYNC_DRIVER_THREAD = "cluster[0-9]-nio-worker-[0-9]";

    @Test
    public void testBulkLoader_NoArgs()
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class);
        assertEquals(1, tool.getExitCode());
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsString("Missing sstable directory argument"));

        assertNoUnexpectedThreadsStarted(false, "ObjectCleanerThread",
                "Shutdown-checker",
                "cluster[0-9]-connection-reaper-[0-9]");

        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }
    
    @Test
    public void testBulkLoader_WithArgs() throws Exception
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));

        assertEquals(-1, tool.getExitCode());
        if (!(tool.getException().getCause() instanceof BulkLoadException))
            throw tool.getException();
        if (!(tool.getException().getCause().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();


        assertNoUnexpectedThreadsStarted(false, ALLOWED_BULK_LOADER_THREADS);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testBulkLoader_WithArgs1() throws Exception
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1",
                                                 "--port",
                                                 "9042",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));

        assertEquals(-1, tool.getExitCode());
        if (!(tool.getException().getCause() instanceof BulkLoadException))
            throw tool.getException();
        if (!(tool.getException().getCause().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();

        assertNoUnexpectedThreadsStarted(false, appendToAllowedThreads(SYNC_DRIVER_THREAD));
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testBulkLoader_WithArgs2() throws Exception
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9042",
                                                 "--port",
                                                 "9041",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));

        assertEquals(-1, tool.getExitCode());
        if (!(tool.getException().getCause() instanceof BulkLoadException))
            throw tool.getException();
        if (!(tool.getException().getCause().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();

        assertNoUnexpectedThreadsStarted(false, appendToAllowedThreads(SYNC_DRIVER_THREAD));
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test(expected = NoHostAvailableException.class)
    public void testBulkLoader_WithArgs3() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1",
                                                 "--port",
                                                 "9041",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        throw tool.getException().getCause().getCause().getCause();
    }

    @Test(expected = NoHostAvailableException.class)
    public void testBulkLoader_WithArgs4() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9041",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        throw tool.getException().getCause().getCause().getCause();
    }

    @Test(expected = NoHostAvailableException.class)
    public void testBulkLoader_WithArgs5() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9041",
                                                 "--throttle",
                                                 "10",
                                                 "--inter-dc-throttle",
                                                 "15",
                                                 "--entire-sstable-throttle-mib",
                                                 "20",
                                                 "--entire-sstable-inter-dc-throttle-mib",
                                                 "25",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        assertEquals(10 * 125_000, DatabaseDescriptor.getStreamThroughputOutboundBytesPerSec(), 0.0);
        assertEquals(15 * 125_000, DatabaseDescriptor.getInterDCStreamThroughputOutboundBytesPerSec(), 0.0);
        assertEquals(20, DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMebibytesPerSec(), 0.0);
        assertEquals(25, DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(), 0.0);
        throw tool.getException().getCause().getCause().getCause();
    }

    @Test(expected = NoHostAvailableException.class)
    public void testBulkLoader_WithArgs6() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9041",
                                                 "--throttle-mib",
                                                 "3",
                                                 "--inter-dc-throttle-mib",
                                                 "4",
                                                 "--entire-sstable-throttle-mib",
                                                 "5",
                                                 "--entire-sstable-inter-dc-throttle-mib",
                                                 "6",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        assertEquals(3 * 1024 * 1024, DatabaseDescriptor.getStreamThroughputOutboundBytesPerSec(), 0.0);
        assertEquals(4 * 1024 * 1024, DatabaseDescriptor.getInterDCStreamThroughputOutboundBytesPerSec(), 0.0);
        assertEquals(5, DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMebibytesPerSec(), 0.0);
        assertEquals(6, DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMebibytesPerSec(), 0.0);
        throw tool.getException().getCause().getCause().getCause();
    }

    private String[] appendToAllowedThreads(String... optionalThreads)
    {
        var result = new ArrayList<>(Arrays.asList(ALLOWED_BULK_LOADER_THREADS));
        Collections.addAll(result, optionalThreads);
        return result.toArray(String[]::new);
    }
}
