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

import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Directories;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.LegacySSTableTest;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/*
 * SStableUpdater should be run with the server shutdown, but we need to set up a certain env to be able to
 * load/swap/drop sstable files under the test's feet. Hence why we need a separate file vs StandaloneUpgraderTest.
 * 
 * Caution: heavy hacking ahead.
 */
public class StandaloneUpgraderOnSStablesTest
{
    String legacyId = LegacySSTableTest.legacyVersions[LegacySSTableTest.legacyVersions.length - 1];

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        LegacySSTableTest.defineSchema();
        System.setProperty(Util.ALLOW_TOOL_REINIT_FOR_TEST, "true"); // Necessary for testing
    }

    @AfterClass
    public static void clearClassEnv()
    {
        System.clearProperty(Util.ALLOW_TOOL_REINIT_FOR_TEST);
    }

    @Test
    public void testUpgradeKeepFiles() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(legacyId);
        LegacySSTableTest.loadLegacyTables(legacyId);

        List<String> origFiles = getSStableFiles("legacy_tables", "legacy_" + legacyId + "_simple");
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "-k",
                                                 "legacy_tables",
                                                 "legacy_" + legacyId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need upgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + legacyId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + legacyId + "_simple");
        origFiles.removeAll(newFiles);
        assertEquals(0, origFiles.size()); // check previous version files are kept

        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + legacyId + "_simple").loadNewSSTables();
    }

    @Test
    public void testUpgradeSnapshot() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(legacyId);
        LegacySSTableTest.loadLegacyTables(legacyId);
        StorageService.instance.takeSnapshot("testsnapshot",
                                             Collections.emptyMap(),
                                             "legacy_tables.legacy_" + legacyId + "_simple");

        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "-k",
                                                 "legacy_tables",
                                                 "legacy_" + legacyId + "_simple",
                                                 "wrongsnapshot");
        Assertions.assertThat(tool.getStdout()).contains("Found 0 sstables that need upgrading.");

        tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                      "legacy_tables",
                                      "legacy_" + legacyId + "_simple",
                                      "testsnapshot");
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need upgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + legacyId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();
    }

    @Test
    public void testUpgrade() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(legacyId);
        LegacySSTableTest.loadLegacyTables(legacyId);

        List<String> origFiles = getSStableFiles("legacy_tables", "legacy_" + legacyId + "_simple");
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "legacy_tables",
                                                 "legacy_" + legacyId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need upgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + legacyId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + legacyId + "_simple");
        int origSize = origFiles.size();
        origFiles.removeAll(newFiles);
        assertEquals(origSize, origFiles.size()); // check previous version files are gone
        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + legacyId + "_simple").loadNewSSTables();
    }

    @Test
    public void testUpgradeSequence() throws Throwable
    {
        int startGeneration = LegacySSTableTest.generateMultipleTables(legacyId);

        String tableName = "legacy_" + legacyId + "_multiple";

        List<String> origFiles = getSStableFiles("legacy_tables", tableName);
        // verify that the files are not returned in the correct order.
        Set<String> treeSet = new TreeSet<>();
        treeSet.addAll(origFiles);
        assertNotEquals("Initial data was not in the incorret order", treeSet.toArray(), origFiles.toArray());

        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "legacy_tables",
                                                 tableName);
        Assertions.assertThat(tool.getStdout()).contains("Found 3 sstables that need upgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/" + tableName);
        int[] loc = new int[3];
        for (int i = 0; i < 3; i++)
        {
            String fileName = String.format("%s-%s-big-Data.db", legacyId, startGeneration + i);
            Assertions.assertThat(tool.getStdout()).contains(fileName);
            loc[i] = tool.getStdout().lastIndexOf(fileName);
        }
        Assertions.assertThat(loc).isSorted();
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", tableName);
        int origSize = origFiles.size();
        origFiles.removeAll(newFiles);
        assertEquals(origSize, origFiles.size()); // check previous version files are gone
        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore(tableName).loadNewSSTables();
    }

     /**
     * Gets sstable names  for the keyspace in the order returned by Directories.SSTableLister
     * @param ks the keyspace to read.
     * @param table the name to read.
     * @return a list of sstable names.
     * @throws StartupException
     */
    private List<String> getSStableFiles(String ks, String table) throws StartupException
    {
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
        org.apache.cassandra.Util.flush(cfs);
        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());

        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW);
        lister.includeBackups(false);
        return lister.list().keySet().stream().map( s -> s.toString()).collect(Collectors.toList());
    }
}
