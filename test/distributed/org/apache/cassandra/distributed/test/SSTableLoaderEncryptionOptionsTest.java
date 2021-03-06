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

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class SSTableLoaderEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    static Cluster CLUSTER;
    static String NODES;
    static int NATIVE_PORT;
    static int STORAGE_PORT;

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
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
    @Test
    public void bulkLoaderSuccessfullyConnectsOverSsl() throws Throwable
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                            "--nodes", NODES,
                                                            "--port", Integer.toString(NATIVE_PORT),
                                                            "--storage-port", Integer.toString(STORAGE_PORT),
                                                            "--keystore", validKeyStorePath,
                                                            "--keystore-password", validKeyStorePassword,
                                                            "--truststore", validTrustStorePath,
                                                            "--truststore-password", validTrustStorePassword,
                                                            "test/data/legacy-sstables/na/legacy_tables/legacy_na_clust");
        tool.assertOnCleanExit();
        assertTrue(tool.getStdout().contains("Summary statistics"));
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
        assertTrue(tool.getStdout().contains("SSLHandshakeException"));
    }
}
