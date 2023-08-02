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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
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
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.NativeSSTableLoaderClient;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import static com.google.common.collect.Lists.transform;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class SSTableLoaderEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    static Cluster CLUSTER;
    static String NODES;
    static int NATIVE_PORT;
    static int STORAGE_PORT;
    static int SSL_STORAGE_PORT;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = Cluster.build().withNodes(1).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP); // need gossip to get hostid for java driver
            c.set("server_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("internode_encryption", "all")
                              .put("optional", false)
                              .build());
            c.set("client_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("optional", false)
                              .put("accepted_protocols", Collections.singletonList("TLSv1.2"))
                              .build());
        }).start();
        NODES = CLUSTER.get(1).config().broadcastAddress().getHostString();
        NATIVE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort);
        STORAGE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getStoragePort);
        SSL_STORAGE_PORT = CLUSTER.get(1).callOnInstance(DatabaseDescriptor::getSSLStoragePort);
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void bulkLoaderSuccessfullyStreamsOverSsl() throws Throwable
    {
        File sstables_to_upload = prepareSstablesForUpload();
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                            "--nodes", NODES,
                                                            "--port", Integer.toString(NATIVE_PORT),
                                                            "--storage-port", Integer.toString(STORAGE_PORT),
                                                            "--keystore", validKeyStorePath,
                                                            "--keystore-password", validKeyStorePassword,
                                                            "--truststore", validTrustStorePath,
                                                            "--truststore-password", validTrustStorePassword,
                                                            "--conf-path", "test/conf/sstableloader_with_encryption.yaml",
                                                            "--ssl-ciphers", "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA",
                                                            sstables_to_upload.absolutePath());
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
        assertRows(CLUSTER.get(1).executeInternal("SELECT count(*) FROM ssl_upload_tables.test"), row(42L));
    }

    @Test
    public void bulkLoaderSuccessfullyStreamsOverSslWithDeprecatedSslStoragePort() throws Throwable
    {
        File sstables_to_upload = prepareSstablesForUpload();
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                            "--nodes", NODES,
                                                            "--port", Integer.toString(NATIVE_PORT),
                                                            "--storage-port", Integer.toString(STORAGE_PORT),
                                                            "--ssl-storage-port", Integer.toString(SSL_STORAGE_PORT),
                                                            "--keystore", validKeyStorePath,
                                                            "--keystore-password", validKeyStorePassword,
                                                            "--truststore", validTrustStorePath,
                                                            "--truststore-password", validTrustStorePassword,
                                                            "--conf-path", "test/conf/sstableloader_with_encryption.yaml",
                                                            sstables_to_upload.absolutePath());
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
        assertTrue(tool.getStdout().contains("ssl storage port is deprecated and not used"));
        assertRows(CLUSTER.get(1).executeInternal("SELECT count(*) FROM ssl_upload_tables.test"), row(42L));
    }

    @Test
    public void bulkLoaderCannotAgreeOnClientTLSProtocol()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                            "--ssl-protocol", "TLSv1",
                                                            "--nodes", NODES,
                                                            "--port", Integer.toString(NATIVE_PORT),
                                                            "--storage-port", Integer.toString(STORAGE_PORT),
                                                            "--keystore", validKeyStorePath,
                                                            "--keystore-password", validKeyStorePassword,
                                                            "--truststore", validTrustStorePath,
                                                            "--truststore-password", validTrustStorePassword,
                                                            "test/data/legacy-sstables/na/legacy_tables/legacy_na_clust");
        assertNotEquals(0, tool.getExitCode());
        assertTrue(tool.getStderr().contains("Unable to initialise " + NativeSSTableLoaderClient.class.getName()));
    }

    private static File prepareSstablesForUpload() throws IOException
    {
        generateSstables();
        File sstable_dir = copySstablesFromDataDir("test");
        truncateGeneratedTables();
        return sstable_dir;
    }

    private static void generateSstables() throws IOException
    {
        CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS ssl_upload_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        CLUSTER.schemaChange("CREATE TABLE IF NOT EXISTS ssl_upload_tables.test (pk int, val text, PRIMARY KEY (pk))");
        for (int i = 0; i < 42; i++)
        {
            CLUSTER.get(1).executeInternal(String.format("INSERT INTO ssl_upload_tables.test (pk, val) VALUES (%s, '%s')", i, i));
        }
        CLUSTER.get(1).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush("ssl_upload_tables",
                                                                                              ColumnFamilyStore.FlushReason.UNIT_TESTS)));
    }

    private static void truncateGeneratedTables() throws IOException
    {
        CLUSTER.get(1).executeInternal("TRUNCATE ssl_upload_tables.test");
    }

    private static File copySstablesFromDataDir(String table) throws IOException
    {
        File cfDir = new File("build/test/cassandra/ssl_upload_tables", table);
        cfDir.tryCreateDirectories();
        // Get paths as Strings, because org.apache.cassandra.io.util.File in the dtest
        // node is loaded by org.apache.cassandra.distributed.shared.InstanceClassLoader.
        List<String> keyspace_dir_paths = CLUSTER.get(1).callOnInstance(() -> {
            List<File> cfDirs = Keyspace.open("ssl_upload_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories();
            return transform(cfDirs, (d) -> d.absolutePath());
        });
        for (File srcDir : transform(keyspace_dir_paths, (p) -> new File(p)))
        {
            for (File file : srcDir.tryList((file) -> file.isFile()))
            {
                FileUtils.copyFileToDirectory(file.toJavaIOFile(), cfDir.toJavaIOFile());
            }
        }
        return cfDir;
    }
}
