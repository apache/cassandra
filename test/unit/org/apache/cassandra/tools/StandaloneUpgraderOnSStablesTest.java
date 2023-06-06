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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.LegacySSTableTest;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;

/*
 * SStableUpdater should be run with the server shutdown, but we need to set up a certain env to be able to
 * load/swap/drop sstable files under the test's feet. Hence why we need a separate file vs StandaloneUpgraderTest.
 *
 * Caution: heavy hacking ahead.
 */
public class StandaloneUpgraderOnSStablesTest
{
    static WithProperties properties;

    String legacyId = LegacySSTableTest.legacyVersions[LegacySSTableTest.legacyVersions.length - 1];

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        LegacySSTableTest.defineSchema();
        properties = new WithProperties().set(TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST, true);
    }

    @AfterClass
    public static void clearClassEnv()
    {
        properties.close();
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
        checkUpgradeToolOutput(tool, origFiles);
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

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists("legacy_tables", "legacy_" + legacyId + "_simple");
        List<String> names = cfs.getDirectories()
                                .sstableLister(Directories.OnTxnErr.IGNORE)
                                .snapshots("testsnapshot").list().keySet().stream()
                                .map(descriptor -> descriptor.baseFile().toString())
                                .collect(Collectors.toList());

        tool = ToolRunner.invokeClass(StandaloneUpgrader.class,
                                      "legacy_tables",
                                      "legacy_" + legacyId + "_simple",
                                      "testsnapshot");
        checkUpgradeToolOutput(tool, names);

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

        checkUpgradeToolOutput(tool, origFiles);
        tool.assertOnCleanExit();

        List<String> newFiles = getSStableFiles("legacy_tables", "legacy_" + legacyId + "_simple");
        int origSize = origFiles.size();
        origFiles.removeAll(newFiles);
        assertEquals(origSize, origFiles.size()); // check previous version files are gone
        // need to make sure the new sstables are live, so that they get truncated later
        Keyspace.open("legacy_tables").getColumnFamilyStore("legacy_" + legacyId + "_simple").loadNewSSTables();
    }

    private static void checkUpgradeToolOutput(ToolResult tool, List<String> names)
    {
        Assertions.assertThat(tool.getStdout()).contains("Found " + names.size() + " sstables that need upgrading.");
        for (String name : names)
        {
            Assertions.assertThat(tool.getStdout()).matches("(?s).*Upgrading.*" + Pattern.quote(name) + ".*");
            Assertions.assertThat(tool.getStdout()).matches("(?s).*Upgrade of.*" + Pattern.quote(name) + ".*complete.*");
        }
    }

    private List<String> getSStableFiles(String ks, String table) throws StartupException
    {
        ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
        org.apache.cassandra.Util.flush(cfs);
        ColumnFamilyStore.scrubDataDirectories(cfs.metadata());

        return cfs.getDirectories()
                  .sstableLister(Directories.OnTxnErr.IGNORE).list().keySet().stream()
                  .map(descriptor -> descriptor.baseFile().toString())
                  .collect(Collectors.toList());
    }
}
