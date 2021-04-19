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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.io.Files;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneSplitterWithCQLTesterTest extends CQLTester
{
    private static String sstableFileName;
    private static File sstablesDir;
    private static File sstablesBackupDir;
    private static List<File> origSstables;

    // CQLTester post test method cleanup needs to be avoided by overriding as it'd clean all sstables, env, etc.
    @Override
    public void afterTest() throws Throwable
    {
    }

    @Test
    public void setupEnv() throws Throwable
    {
        // Stop the server after setup as we're going to be changing things under it's feet
        setupTestSstables();
        tearDownClass();
    }

    @Test
    public void testMinFileSizeCheck() throws Throwable
    {
        restoreOrigSstables();
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, sstableFileName);
        Assertions.assertThat(tool.getStdout()).contains("is less than the split size");
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testSplittingSSTable() throws Throwable
    {
        restoreOrigSstables();

        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", sstableFileName);
        List<File> splitFiles = Arrays.asList(sstablesDir.listFiles());
        splitFiles.stream().forEach(f -> {
            if (f.getName().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.getName() + " is way bigger than 1MB: [" + f.length() + "] bytes",
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
        restoreOrigSstables();
        ArrayList<String> args = new ArrayList<>(Arrays.asList("-s", "1"));

        args.addAll(Arrays.asList(sstablesDir.listFiles())
                          .stream()
                          .map(f -> f.getAbsolutePath())
                          .collect(Collectors.toList()));

        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, args.toArray(new String[args.size()]));
        List<File> splitFiles = Arrays.asList(sstablesDir.listFiles());
        splitFiles.stream().forEach(f -> {
            if (f.getName().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.getName() + " is way bigger than 1MB: [" + f.length() + "] bytes",
                           f.length() <= 1024 * 1024 * 1.2); //give a 20% margin on size check
        });
        assertTrue(origSstables.size() < splitFiles.size());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testNoSnapshotOption() throws Throwable
    {
        restoreOrigSstables();
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", "--no-snapshot", sstableFileName);
        assertThat(origSstables.size()).isLessThan(sstablesDir.listFiles().length);
        assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void cleanEnv() throws Throwable
    {
        super.afterTest();
        System.clearProperty(Util.ALLOW_TOOL_REINIT_FOR_TEST);
    }

    private void setupTestSstables() throws Throwable
    {
        createTable("CREATE TABLE %s (id text primary key, val text)");
        for (int i = 0; i < 100000; i++)
            executeFormattedQuery(formatQuery("INSERT INTO %s (id, val) VALUES (?, ?)"), "mockData" + i, "mockData" + i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        sstableFileName = sstables.iterator().next().getFilename();
        assertTrue("Generated sstable must be at least 1MB", (new File(sstableFileName)).length() > 1024*1024);
        sstablesDir = new File(sstableFileName).getParentFile();
        sstablesBackupDir = new File(sstablesDir.getAbsolutePath() + "/testbackup");
        sstablesBackupDir.mkdir();
        origSstables = Arrays.asList(sstablesDir.listFiles());

        // Back up orig sstables
        origSstables.stream().forEach(f -> {
            if (f.isFile())
                try
                {
                    Files.copy(f, new File(sstablesBackupDir.getAbsolutePath() + "/" + f.getName()));
                }
                catch(IOException e)
                {
                    throw new RuntimeException(e);
                }
        });

        System.setProperty(Util.ALLOW_TOOL_REINIT_FOR_TEST, "true"); // Necessary for testing
    }

    private void restoreOrigSstables()
    {
        Arrays.stream(Objects.requireNonNull(sstablesDir.listFiles())).forEach(f -> {
            if (f.isFile())
                f.delete();
        });
        Arrays.stream(Objects.requireNonNull(sstablesBackupDir.listFiles())).forEach(f -> {
            if (f.isFile())
                try
                {
                    Files.copy(f, new File(sstablesDir.getAbsolutePath() + '/' + f.getName()));
                }
                catch(IOException e)
                {
                    throw new RuntimeException(e);
                }
        });

        SSTableReader.resetTidying();
    }
}
