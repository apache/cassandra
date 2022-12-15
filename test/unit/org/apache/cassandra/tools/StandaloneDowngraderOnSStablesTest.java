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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.LegacySSTableTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;

import static org.junit.Assert.assertEquals;

/*
 * SStableUpdater should be run with the server shutdown, but we need to set up a certain env to be able to
 * load/swap/drop sstable files under the test's feet. Hence why we need a separate file vs StandaloneDowngraderTest.
 * 
 * Caution: heavy hacking ahead.
 */
public class StandaloneDowngraderOnSStablesTest
{
    String currentId = LegacySSTableTest.legacyVersions[0];
    String legacyId = LegacySSTableTest.legacyVersions[ LegacySSTableTest.legacyVersions.length -1 ];

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
    public void testDowngradeKeepFiles() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(currentId);
        LegacySSTableTest.loadLegacyTables(currentId);

        List<String> origFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        ToolResult tool = ToolRunner.invokeClass(StandaloneDowngrader.class,
                                                 "-k",
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple",
                                                 legacyId);
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need downgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + currentId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        origFiles.removeAll(newFiles);
        assertEquals(0, origFiles.size()); // check previous version files are kept

        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + currentId + "_simple").loadNewSSTables();
    }

    @Test
    public void testDowngradeSnapshot() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(currentId);
        LegacySSTableTest.loadLegacyTables(currentId);
        StorageService.instance.takeSnapshot("testsnapshot",
                                             Collections.emptyMap(),
                                             "legacy_tables.legacy_" + currentId + "_simple");

        ToolResult tool = ToolRunner.invokeClass(StandaloneDowngrader.class,
                                                 "-k",
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple",
                                                 legacyId,
                                                 "wrongsnapshot");
        Assertions.assertThat(tool.getStdout()).contains("Found 0 sstables that need downgrading.");

        tool = ToolRunner.invokeClass(StandaloneDowngrader.class,
                                      "legacy_tables",
                                      "legacy_" + currentId + "_simple",
                                      legacyId,
                                      "testsnapshot");
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need downgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + currentId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();
    }

    @Test
    public void testDowngrade() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(currentId);
        LegacySSTableTest.loadLegacyTables(currentId);

        List<String> origFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        ToolResult tool = ToolRunner.invokeClass(StandaloneDowngrader.class,
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple",
                                                legacyId);
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need downgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + currentId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        int origSize = origFiles.size();
        origFiles.removeAll(newFiles);
        assertEquals(origSize, origFiles.size()); // check previous version files are gone

        // verify no more files to downgrade detected
         tool = ToolRunner.invokeClass(StandaloneDowngrader.class,
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple",
                                                 legacyId);
        Assertions.assertThat(tool.getStdout()).contains("Found 0 sstables that need downgrading.");
        tool.assertOnCleanExit();

        // now upgrade them
        origFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple");
        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + currentId + "_simple").loadNewSSTables();
    }

    private List<String> getSStableFiles(String ks, String table) throws StartupException
    {
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
        org.apache.cassandra.Util.flush(cfs);
        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        if (sstables.isEmpty())
            return Lists.emptyList();

        String sstableFileName = sstables.iterator().next().getFilename();
        File sstablesDir = new File(sstableFileName).parent();
        return Arrays.asList(sstablesDir.tryList())
                     .stream()
                     .filter(f -> f.isFile())
                     .map(file -> file.toString())
                     .collect(Collectors.toList());
    }

    public void upgradeTables() throws Throwable
    {
        LegacySSTableTest.truncateLegacyTables(currentId);
        LegacySSTableTest.loadLegacyTables(currentId);

        List<String> origFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        ToolResult tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                                 "legacy_tables",
                                                 "legacy_" + currentId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("Found 1 sstables that need upgrading.");
        Assertions.assertThat(tool.getStdout()).contains("legacy_tables/legacy_" + currentId + "_simple");
        Assertions.assertThat(tool.getStdout()).contains("-Data.db");
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + currentId + "_simple");
        int origSize = origFiles.size();
        origFiles.removeAll(newFiles);
        assertEquals(origSize, origFiles.size()); // check previous version files are gone
        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + currentId + "_simple").loadNewSSTables();
    }
}
