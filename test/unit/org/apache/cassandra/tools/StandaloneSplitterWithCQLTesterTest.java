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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;

import org.apache.cassandra.io.util.File;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandaloneSplitterWithCQLTesterTest extends CQLTester
{
    private static String sstableFileName;
    private static File sstablesDir;
    private static List<File> origSstables;

    @Before
    public void before() throws Throwable
    {
        setupTestSstables();
        tearDownClass();
        SSTableReader.resetTidying();
    }

    @Test
    public void testMinFileSizeCheck() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, sstableFileName);
        Assertions.assertThat(tool.getStdout()).contains("is less than the split size");
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testSplittingSSTable() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", sstableFileName);
        List<File> splitFiles = Arrays.asList(sstablesDir.tryList());
        splitFiles.stream().forEach(f -> {
            if (f.name().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.name() + " is way bigger than 1MiB: [" + f.length() + "] bytes",
                           f.length() <= 1024 * 1024 * 1.2); //give a 20% margin on size check
        });
        assertTrue(origSstables.size() < splitFiles.size());
        Assertions.assertThat(tool.getStdout()).contains("sstables snapshotted into");
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testSplittingMultipleSSTables() throws Throwable
    {
        ArrayList<String> args = new ArrayList<>(Arrays.asList("-s", "1"));

        args.addAll(Arrays.asList(sstablesDir.tryList())
                          .stream()
                          .map(f -> f.absolutePath())
                          .collect(Collectors.toList()));

        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, args.toArray(new String[args.size()]));
        List<File> splitFiles = Arrays.asList(sstablesDir.tryList());
        splitFiles.stream().forEach(f -> {
            if (f.name().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.name() + " is way bigger than 1MiB: [" + f.length() + "] bytes",
                           f.length() <= 1024 * 1024 * 1.2); //give a 20% margin on size check
        });
        assertTrue(origSstables.size() < splitFiles.size());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testNoSnapshotOption() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", "--no-snapshot", sstableFileName);
        assertTrue(origSstables.size() < Arrays.asList(sstablesDir.tryList()).size());
        assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    private void setupTestSstables() throws Throwable
    {
        SSTableReader.resetTidying();
        createTable("CREATE TABLE %s (id text primary key, val text)");
        for (int i = 0; i < 100000; i++)
            executeFormattedQuery(formatQuery("INSERT INTO %s (id, val) VALUES (?, ?)"), "mockData" + i, "mockData" + i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        org.apache.cassandra.Util.flush(cfs);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        sstableFileName = sstables.iterator().next().getFilename();
        assertTrue("Generated sstable must be at least 1MiB", (new File(sstableFileName)).length() > 1024*1024);
        sstablesDir = new File(sstableFileName).parent();
        origSstables = Arrays.asList(sstablesDir.tryList());
        TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.setBoolean(true);
    }
}
