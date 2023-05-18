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
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.VerifyTest;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigTableVerifier;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;

/**
 * Class that tests tables for {@link StandaloneVerifier} by updating using {@link SchemaLoader}
 * Similar in vein to other {@link SchemaLoader} type tests, as well as {@link StandaloneUpgraderOnSStablesTest}.
 * Since the tool mainly exercises the {@link BigTableVerifier}, we elect to
 * not run every conceivable option as many tests are already covered by {@link VerifyTest}.
 * 
 * Note: the complete coverage is composed of:
 * - {@link StandaloneVerifierOnSSTablesTest}
 * - {@link StandaloneVerifierTest}
 * - {@link VerifyTest}
 */
public class StandaloneVerifierOnSSTablesTest extends OfflineToolUtils
{
    static WithProperties properties;

    @BeforeClass
    public static void setup()
    {
        // since legacy tables test data uses ByteOrderedPartitioner that's what we need
        // for the check version to work
        CassandraRelevantProperties.PARTITIONER.setString("org.apache.cassandra.dht.ByteOrderedPartitioner");
        properties = new WithProperties().set(TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST, true);
        SchemaLoader.loadSchema();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        SchemaLoader.cleanupAndLeaveDirs();
        properties.close();
    }

    @Test
    public void testCheckVersionValidVersion() throws Exception
    {
        String keyspaceName = "StandaloneVerifierTestCheckVersionWorking";
        String workingTable = "workingCheckTable";

        createAndPopulateTable(keyspaceName, workingTable, x -> {});

        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, keyspaceName, workingTable, "--force", "-c");
        assertEquals(0, tool.getExitCode());
        tool.assertOnCleanExit();
    }

    @Test
    public void testCheckVersionWithWrongVersion() throws Exception
    {
        String keyspace = "StandaloneVerifierTestWrongVersions";
        String tableName = "legacy_ma_simple";

        createAndPopulateTable(keyspace, tableName, cfs -> {
            // let's just copy old version files from test data into the source dir
            File testDataDir = new File("test/data/legacy-sstables/ma/legacy_tables/legacy_ma_simple");
            for (org.apache.cassandra.io.util.File cfsDir : cfs.getDirectories().getCFDirectories())
            {
                FileUtils.copyDirectory(testDataDir, cfsDir.toJavaIOFile());
            }
        });

        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, keyspace, tableName, "-f", "-c");

        assertEquals(1, tool.getExitCode());
        Assertions.assertThat(tool.getStdout()).contains("is not the latest version, run upgradesstables");
    }

    @Test
    public void testWorkingDataFile() throws Exception
    {
        String keyspaceName = "StandaloneVerifierTestWorkingDataKs";
        String workingTable = "workingTable";

        createAndPopulateTable(keyspaceName, workingTable, x -> {});

        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, keyspaceName, workingTable, "--force");
        assertEquals(0, tool.getExitCode());
        tool.assertOnCleanExit();
    }

    @Test
    public void testCorruptStatsFile() throws Exception
    {
        String keyspaceName = "StandaloneVerifierTestCorruptStatsKs";
        String corruptStatsTable = "corruptStatsTable";
        createAndPopulateTable(keyspaceName, corruptStatsTable, cfs -> {
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
            try (RandomAccessFile file = new RandomAccessFile(sstable.descriptor.fileFor(Components.STATS).toJavaIOFile(), "rw"))
            {
                file.seek(0);
                file.writeBytes(StringUtils.repeat('z', 2));
            }
        });

        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, keyspaceName, corruptStatsTable, "-f");

        assertEquals(1, tool.getExitCode());
        Assertions.assertThat(tool.getStderr()).contains("Error Loading", corruptStatsTable);
    }

    @Test
    public void testCorruptDataFile() throws Exception
    {
        String keyspaceName = "StandaloneVerifierTestCorruptDataKs";
        String corruptDataTable = "corruptDataTable";

        createAndPopulateTable(keyspaceName, corruptDataTable, cfs -> {
            SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
            long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("0"), cfs.getPartitioner()), SSTableReader.Operator.EQ);
            long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("1"), cfs.getPartitioner()), SSTableReader.Operator.EQ);
            long startPosition = Math.min(row0Start, row1Start);

            try (RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw"))
            {
                file.seek(startPosition);
                file.writeBytes(StringUtils.repeat('z', 2));
            }
        });

        ToolResult tool = ToolRunner.invokeClass(StandaloneVerifier.class, keyspaceName, corruptDataTable, "--force");
        assertEquals(1, tool.getExitCode());
        Assertions.assertThat(tool.getStdout()).contains("Invalid SSTable", corruptDataTable);
    }

    /**
     * Since we are testing a verifier, we'd like to corrupt files to verify code paths
     * This function definition is used by {@link this#createAndPopulateTable}.
     *
     * CFS is the open ColumnFamilyStore for a given keyspace, table
     */
    @FunctionalInterface
    private interface CorruptFunction
    {
        public void apply(ColumnFamilyStore cfs) throws Exception;
    }

    /**
     * This function sets up the keyspace, and table schema for a standardCFMD table.
     * <p>
     * This will also populate the tableName with a few rows.  After completion the
     * server will be shutdown.
     *
     * @param keyspace the name of the keyspace in which the table should be created
     * @param tableName new table name of the standard CFMD table
     * @param corruptionFn function called to corrupt or change the contents on disk, is passed the Cfs of the table name.
     * @throws Exception on error.
     */
    private static void createAndPopulateTable(String keyspace, String tableName, CorruptFunction corruptionFn) throws Exception
    {
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    standardCFMD(keyspace, tableName));

        CompactionManager.instance.disableAutoCompaction();

        Keyspace k = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = k.getColumnFamilyStore(tableName);

        populateTable(cfs, 2);

        corruptionFn.apply(cfs);
    }

    private static void populateTable(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                         .newRow("c1").add("val", "1")
                         .newRow("c2").add("val", "2")
                         .apply();
        }

        org.apache.cassandra.Util.flush(cfs);
    }
}
