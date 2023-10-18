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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;

import static com.google.common.collect.Lists.transform;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.junit.Assert.assertTrue;

/**
 * Tests the guardrail for bulk load, {@link Guardrails#bulkLoadEnabled}.
 */
public class GuardrailBulkLoadEnabledTest extends GuardrailTester
{
    private static Path tempDir;
    private static Cluster cluster;
    private static String nodes;
    private static int nativePort;
    private static int storagePort;

    public static Path tempDir() throws Exception
    {
        return Files.createTempDirectory("GuardrailBulkLoadEnabledTest");
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @BeforeClass
    public static void setupCluster() throws Exception
    {
        tempDir = tempDir();
        cluster = init(Cluster.build(1).withConfig(c -> c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP)).start());
        nodes = cluster.get(1).config().broadcastAddress().getHostString();
        nativePort = cluster.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
        storagePort = cluster.get(1).callOnInstance(DatabaseDescriptor::getStoragePort);
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();

        for (File f : new File(tempDir).tryList())
        {
            f.tryDelete();
        }
    }

    @Test
    public void bulkLoaderEnabled() throws Throwable
    {
        File sstablesToUpload = prepareSstablesForUpload();
        // bulk load SSTables work as expected
        ToolRunner.ToolResult tool = loadData(sstablesToUpload);
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
        assertRows(cluster.get(1).executeInternal("SELECT count(*) FROM bulk_load_tables.test"), row(22L));

        // truncate table
        truncateGeneratedTables();
        assertRows(cluster.get(1).executeInternal("SELECT * FROM bulk_load_tables.test"), EMPTY_ROWS);

        // Disable bulk load, stream should fail and no data should be loaded
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setBulkLoadEnabled(false));
        long mark = cluster.get(1).logs().mark();
        tool = loadData(sstablesToUpload);

        cluster.get(1).logs().watchFor(mark, "Bulk load of SSTables is not allowed");
        tool.asserts().failure().errorContains("Stream failed");
        assertRows(cluster.get(1).executeInternal("SELECT * FROM bulk_load_tables.test"), EMPTY_ROWS);

        // Enable bulk load again, data should be loaded successfully
        cluster.get(1).runOnInstance(() -> Guardrails.instance.setBulkLoadEnabled(true));
        tool = loadData(sstablesToUpload);
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
        assertRows(cluster.get(1).executeInternal("SELECT count(*) FROM bulk_load_tables.test"), row(22L));
    }

    private static ToolRunner.ToolResult loadData(File sstablesToUpload)
    {
        return ToolRunner.invokeClass(BulkLoader.class,
                                      "--nodes", nodes,
                                      "--port", Integer.toString(nativePort),
                                      "--storage-port", Integer.toString(storagePort),
                                      sstablesToUpload.absolutePath());
    }

    private static File prepareSstablesForUpload() throws IOException
    {
        generateSSTables();
        File sstableDir = copySStablesFromDataDir("test");
        truncateGeneratedTables();
        return sstableDir;
    }

    private static File copySStablesFromDataDir(String table) throws IOException
    {
        File cfDir = new File(tempDir +  File.pathSeparator() + "bulk_load_tables" + File.pathSeparator() + table);
        cfDir.tryCreateDirectories();
        List<String> keyspaceDirPaths = cluster.get(1).callOnInstance(
        () -> Keyspace.open("bulk_load_tables")
                      .getColumnFamilyStore(table)
                      .getDirectories()
                      .getCFDirectories()
                      .stream()
                      .map(File::absolutePath)
                      .collect(toList())
        );
        for (File srcDir : transform(keyspaceDirPaths, File::new))
        {
            for (File file : srcDir.tryList(File::isFile))
                FileUtils.copyFileToDirectory(file.toJavaIOFile(), cfDir.toJavaIOFile());
        }
        return cfDir;
    }

    private static void generateSSTables()
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS bulk_load_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS bulk_load_tables.test (pk int, val text, PRIMARY KEY (pk))");
        for (int i = 0; i < 22; i++)
        {
            cluster.get(1).executeInternal(String.format("INSERT INTO bulk_load_tables.test (pk, val) VALUES (%s, '%s')", i, i));
        }
        cluster.get(1).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush("bulk_load_tables", UNIT_TESTS)));
    }

    private static void truncateGeneratedTables()
    {
        cluster.get(1).executeInternal("TRUNCATE bulk_load_tables.test");
    }
}
