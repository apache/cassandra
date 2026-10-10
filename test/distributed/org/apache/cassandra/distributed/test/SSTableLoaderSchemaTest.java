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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;

import static com.google.common.collect.Lists.transform;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SSTableLoaderSchemaTest extends TestBaseImpl
{
    private static final String KEYSPACE = "sstableloader_schema";
    private static final String TABLE = "vectors";
    private static final int ROWS = 42;

    static Cluster CLUSTER;
    static String NODES;
    static int NATIVE_PORT;
    static int STORAGE_PORT;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = Cluster.build().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP); // need gossip to get hostid for java driver
        }).start();
        NODES = CLUSTER.get(1).config().broadcastAddress().getHostString();
        NATIVE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
        STORAGE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getStoragePort);

        CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, val vector<float, 3>, PRIMARY KEY (pk))");
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void bulkLoaderStreamsTableWithVectorColumn() throws Throwable
    {
        File sstablesToUpload = prepareSstablesForUpload();
        ToolRunner.ToolResult tool = runBulkLoader(sstablesToUpload);
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
        assertRows(CLUSTER.get(1).executeInternal("SELECT count(*) FROM " + KEYSPACE + '.' + TABLE), row((long) ROWS));
    }

    @Test
    public void bulkLoaderFailsWhenDriverCannotParseSchema() throws Throwable
    {
        File sstablesToUpload = prepareSstablesForUpload();
        CLUSTER.schemaChange("CREATE KEYSPACE vector_udt WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        try
        {
            CLUSTER.schemaChange("CREATE TYPE vector_udt.embedding (v vector<float, 3>)");
            CLUSTER.schemaChange("CREATE TABLE vector_udt.comments (pk int PRIMARY KEY, e frozen<embedding>)");
            ToolRunner.ToolResult tool = runBulkLoader(sstablesToUpload);
            assertNotEquals(0, tool.getExitCode());
            assertTrue(tool.getStderr().contains("the driver failed to parse the cluster schema"));
            assertRows(CLUSTER.get(1).executeInternal("SELECT count(*) FROM " + KEYSPACE + '.' + TABLE), row(0L));
        }
        finally
        {
            CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS vector_udt");
        }
    }

    @Test
    public void bulkLoaderFailsWhenKeyspaceDoesNotExist()
    {
        File sstablesToUpload = new File("build/test/cassandra/sstableloader_schema_missing/no_such_keyspace", TABLE);
        sstablesToUpload.tryCreateDirectories();
        ToolRunner.ToolResult tool = runBulkLoader(sstablesToUpload);
        assertNotEquals(0, tool.getExitCode());
        assertTrue(tool.getStderr().contains("does not exist"));
    }

    @Test
    public void bulkLoaderSucceedsWithEmptySstableDirectory()
    {
        File sstablesToUpload = new File("build/test/cassandra/sstableloader_schema_empty/" + KEYSPACE, TABLE);
        sstablesToUpload.tryCreateDirectories();
        ToolRunner.ToolResult tool = runBulkLoader(sstablesToUpload);
        tool.assertOnCleanExit();
    }

    private static ToolRunner.ToolResult runBulkLoader(File sstablesToUpload)
    {
        return ToolRunner.invokeClass(BulkLoader.class,
                                      "--nodes", NODES,
                                      "--port", Integer.toString(NATIVE_PORT),
                                      "--storage-port", Integer.toString(STORAGE_PORT),
                                      sstablesToUpload.absolutePath());
    }

    private static File prepareSstablesForUpload() throws IOException
    {
        generateSstables();
        File sstableDir = copySstablesFromDataDir();
        CLUSTER.get(1).executeInternal("TRUNCATE " + KEYSPACE + '.' + TABLE);
        return sstableDir;
    }

    private static void generateSstables()
    {
        for (int i = 0; i < ROWS; i++)
            CLUSTER.get(1).executeInternal(String.format("INSERT INTO %s.%s (pk, val) VALUES (%d, [%d.0, %d.5, 1.0])", KEYSPACE, TABLE, i, i, i));
        CLUSTER.get(1).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush(KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS)));
    }

    private static File copySstablesFromDataDir() throws IOException
    {
        File cfDir = new File("build/test/cassandra/" + KEYSPACE, TABLE);
        if (cfDir.exists())
            cfDir.deleteRecursive();
        cfDir.tryCreateDirectories();
        // Get paths as Strings, because org.apache.cassandra.io.util.File in the dtest
        // node is loaded by org.apache.cassandra.distributed.shared.InstanceClassLoader.
        List<String> keyspaceDirPaths = CLUSTER.get(1).callOnInstance(() -> {
            List<File> cfDirs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getDirectories().getCFDirectories();
            return transform(cfDirs, (d) -> d.absolutePath());
        });
        for (File srcDir : transform(keyspaceDirPaths, (p) -> new File(p)))
        {
            for (File file : srcDir.tryList((file) -> file.isFile()))
                FileUtils.copyFileToDirectory(file.toJavaIOFile(), cfDir.toJavaIOFile());
        }
        return cfDir;
    }
}
