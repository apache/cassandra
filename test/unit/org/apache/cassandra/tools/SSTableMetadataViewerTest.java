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
import java.util.Arrays;

import com.google.common.base.CharMatcher;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SSTableMetadataViewerTest extends OfflineToolUtils
{
    private static String sstable;

    @BeforeClass
    public static void setupTest() throws IOException
    {
        sstable = findOneSSTable("legacy_sstables", "legacy_ma_simple");
    }

    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class);
        {
            assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
            assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Options:"));
            assertEquals(1, tool.getExitCode());
        }
        assertNoUnexpectedThreadsStarted(null, null);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testMaybeChangeDocs()
    {
        // If you added, modified options or help, please update docs if necessary
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "-h");
        assertEquals("You must supply at least one sstable\n" + 
                     "usage: sstablemetadata <options> <sstable...> [-c] [-g <arg>] [-s] [-t <arg>] [-u]\n" + 
                     "\n" + 
                     "Dump information about SSTable[s] for Apache Cassandra 3.x\n" + 
                     "Options:\n" + 
                     "  -c,--colors                 Use ANSI color sequences\n" + 
                     "  -g,--gc_grace_seconds <arg> Time to use when calculating droppable tombstones\n" + 
                     "  -s,--scan                   Full sstable scan for additional details. Only available in 3.0+ sstables. Defaults: false\n" + 
                     "  -t,--timestamp_unit <arg>   Time unit that cell timestamps are written with\n" + 
                     "  -u,--unicode                Use unicode to draw histograms and progress bars\n\n" 
                     , tool.getCleanedStderr());
    }

    @Test
    public void testWrongArgFailsAndPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "--debugwrong", "sstableFile");
        assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
        assertThat(tool.getCleanedStderr(), CoreMatchers.containsStringIgnoringCase("Options:"));
        assertEquals(1, tool.getExitCode());
    }

    @Test
    public void testNAFileCall()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, "mockFile");
        assertThat(tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
        Assertions.assertThat(tool.getCleanedStderr()).isEmpty();
        assertEquals(0, tool.getExitCode());
        assertGoodEnvPostTest();
    }

    @Test
    public void testOnlySstableArg()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, sstable);
        Assertions.assertThat(tool.getStdout()).doesNotContain(Util.BLUE);
        assertTrue(tool.getStdout(), CharMatcher.ascii().matchesAllOf(tool.getStdout()));
        Assertions.assertThat(tool.getStdout()).doesNotContain("Widest Partitions");
        Assertions.assertThat(tool.getStdout()).contains(sstable.replaceAll("-Data\\.db$", ""));
        assertTrue(tool.getStderr(), tool.getStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertGoodEnvPostTest();
    }

    @Test
    public void testColorArg()
    {
        Arrays.asList("-c",
                      "--colors")
              .stream()
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, arg, sstable);
                  Assertions.assertThat(tool.getStdout()).contains(Util.BLUE);
                  Assertions.assertThat(tool.getStdout()).contains(sstable.replaceAll("-Data\\.db$", ""));
                  assertTrue("Arg: [" + arg + "]\n" + tool.getStderr(), tool.getStderr().isEmpty());
                  assertEquals(0, tool.getExitCode());
                  assertGoodEnvPostTest();
              });
    }

    @Test
    public void testUnicodeArg()
    {
        Arrays.asList("-u",
                      "--unicode")
              .stream()
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, arg, sstable);
                  assertTrue(tool.getStdout(), !CharMatcher.ascii().matchesAllOf(tool.getStdout()));
                  Assertions.assertThat(tool.getStdout()).contains(sstable.replaceAll("-Data\\.db$", ""));
                  assertTrue("Arg: [" + arg + "]\n" + tool.getStderr(), tool.getStderr().isEmpty());
                  assertEquals(0, tool.getExitCode());
                  assertGoodEnvPostTest();
              });
    }

    @Test
    public void testGCArg()
    {
        Arrays.asList(Pair.of("-g", ""),
                      Pair.of("-g", "w"),
                      Pair.of("--gc_grace_seconds", ""),
                      Pair.of("--gc_grace_seconds", "w"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                           arg.getLeft(),
                                                           arg.getRight(),
                                                           "mockFile");
                  assertEquals(-1, tool.getExitCode());
                  Assertions.assertThat(tool.getStderr()).contains(NumberFormatException.class.getSimpleName());
              });

        Arrays.asList(Pair.of("-g", "5"), Pair.of("--gc_grace_seconds", "5")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                            arg.getLeft(),
                                                            arg.getRight(),
                                                            "mockFile");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertGoodEnvPostTest();
        });
    }

    @Test
    public void testTSUnitArg()
    {
        Arrays.asList(Pair.of("-t", ""),
                      Pair.of("-t", "w"),
                      Pair.of("--timestamp_unit", ""),
                      Pair.of("--timestamp_unit", "w"))
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                           arg.getLeft(),
                                                           arg.getRight(),
                                                           "mockFile");
                  assertEquals(-1, tool.getExitCode());
                  Assertions.assertThat(tool.getStderr()).contains(IllegalArgumentException.class.getSimpleName());
              });

        Arrays.asList(Pair.of("-t", "SECONDS"), Pair.of("--timestamp_unit", "SECONDS")).forEach(arg -> {
            ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class,
                                                       arg.getLeft(),
                                                       arg.getRight(),
                                                       "mockFile");
            assertThat("Arg: [" + arg + "]", tool.getStdout(), CoreMatchers.containsStringIgnoringCase("No such file"));
            Assertions.assertThat(tool.getCleanedStderr()).as("Arg: [%s]", arg).isEmpty();
            tool.assertOnExitCode();
            assertGoodEnvPostTest();
        });
    }

    @Test
    public void testScanArg()
    {
        Arrays.asList("-s",
                      "--scan")
              .stream()
              .forEach(arg -> {
                  ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, arg, sstable);
                  Assertions.assertThat(tool.getStdout()).contains("Widest Partitions");
                  Assertions.assertThat(tool.getStdout()).contains(sstable.replaceAll("-Data\\.db$", ""));
                  assertTrue("Arg: [" + arg + "]\n" + tool.getStderr(), tool.getStderr().isEmpty());
                  assertEquals(0, tool.getExitCode());
              });
    }

    private void assertGoodEnvPostTest()
    {
        assertNoUnexpectedThreadsStarted(null, OPTIONAL_THREADS_WITH_SCHEMA);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testLoadingOfSStableWithRangeTombstoneAsMaxClustering() throws IOException
    {
        String sstableWithTombstones = copySSTables("sstable_metadata", "CASSANDRA_20855");
        ToolResult tool = ToolRunner.invokeClass(SSTableMetadataViewer.class, sstableWithTombstones);
        assertTrue(tool.getStdout(), CharMatcher.ascii().matchesAllOf(tool.getStdout()));
        Assertions.assertThat(tool.getStdout()).contains(sstableWithTombstones.replaceAll("-Data\\.db$", ""));
        Assertions.assertThat(tool.getStdout()).contains("minClusteringValues: [clust_key_1_1, clust_key_2_1]");
        // Range tombstone end as a max clustering value, only the first clustering key is stored in metadata
        Assertions.assertThat(tool.getStdout()).contains("maxClusteringValues: [clust_key_1_2]");
        assertTrue(tool.getStderr(), tool.getStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertGoodEnvPostTest();
    }

    @Test
    @Ignore
    // the logic is used only to generate test data for testLoadingOfSStableWithRangeTombstoneAsMaxClustering
    // the result of the generation is committed to SCM
    public void generateSSTableWithRangeTombstone() throws IOException
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        QueryProcessor.executeInternal("CREATE KEYSPACE sstable_metadata WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        QueryProcessor.executeInternal("CREATE TABLE sstable_metadata.CASSANDRA_20855 (pk text, ck_1 text, ck_2 text, val text, PRIMARY KEY (pk, ck_1, ck_2))");

        QueryProcessor.executeInternal(String.format("INSERT INTO sstable_metadata.CASSANDRA_20855 (pk, ck_1, ck_2, val) VALUES ('%s', '%s', '%s', '%s')",
                                                     "part_key_1", "clust_key_1_1", "clust_key_2_1", "value"));

        QueryProcessor.executeInternal(String.format("DELETE FROM sstable_metadata.CASSANDRA_20855 WHERE pk = '%s' AND ck_1 = '%s'",
                                                     "part_key_1", "clust_key_1_2"));

        StorageService.instance.forceKeyspaceFlush("sstable_metadata");
    }


    private static String copySSTables(String keyspace, String table) throws IOException
    {
        File targetKeyspaceDir = new File("build/test/cassandra/data/", keyspace);
        targetKeyspaceDir.mkdirs();
        File targetTableDir = new File(targetKeyspaceDir, table);
        File srcKeyspaceDir = new File("test/data/" + keyspace);
        FileUtils.copyDirectory(new File(srcKeyspaceDir, table), targetTableDir);
        File[] sstableFiles = targetTableDir.listFiles((file) -> file.isFile() && file.getName().endsWith("-Data.db"));
        return sstableFiles[0].getAbsolutePath();
    }
}
