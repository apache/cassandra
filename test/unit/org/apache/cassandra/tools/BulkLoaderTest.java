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

import org.junit.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BulkLoaderTest extends OfflineToolUtils
{
    @Test
    public void testBulkLoader_NoArgs()
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class);
        assertEquals(1, tool.getExitCode());
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsString("Missing sstable directory argument"));
        
        assertNoUnexpectedThreadsStarted(null, null);
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
        if (!(tool.getException().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();

        assertNoUnexpectedThreadsStarted(null, new String[]{"globalEventExecutor-1-1", "globalEventExecutor-1-2"});
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
        if (!(tool.getException().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();

        assertNoUnexpectedThreadsStarted(null, new String[] { "globalEventExecutor-1-1", "globalEventExecutor-1-2" });
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
        if (!(tool.getException().getCause().getCause() instanceof NoHostAvailableException))
            throw tool.getException();

        assertNoUnexpectedThreadsStarted(null, new String[] { "globalEventExecutor-1-1", "globalEventExecutor-1-2" });
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
        throw tool.getException().getCause().getCause();
    }

    @Test(expected = NoHostAvailableException.class)
    public void testBulkLoader_WithArgs4() throws Throwable
    {
        ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                 "-d",
                                                 "127.9.9.1:9041",
                                                 OfflineToolUtils.sstableDirName("legacy_sstables", "legacy_ma_simple"));
        assertEquals(-1, tool.getExitCode());
        throw tool.getException().getCause().getCause();
    }
}
