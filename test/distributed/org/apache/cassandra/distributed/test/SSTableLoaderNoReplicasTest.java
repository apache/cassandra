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

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SSTableLoaderNoReplicasTest extends TestBaseImpl
{
    private static final String KEYSPACE = "sstableloader_no_replicas";
    private static final String TABLE = "tbl";

    @Test
    public void bulkLoaderFailsWhenNoReplicasAreFound() throws IOException
    {
        try (Cluster cluster = Cluster.build(2).withDCs(2).withConfig(c -> {
            c.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK, Feature.GOSSIP); // need gossip to get hostid for java driver
        }).start())
        {
            String dc2 = cluster.get(2).config().localDatacenter();
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'NetworkTopologyStrategy', '" + dc2 + "': '1'}");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, val text, PRIMARY KEY (pk))");

            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();

            File sstablesToUpload = new File("build/test/cassandra/sstableloader_no_replicas_empty/" + KEYSPACE, TABLE);
            sstablesToUpload.tryCreateDirectories();
            ToolRunner.ToolResult tool = ToolRunner.invokeClass(BulkLoader.class,
                                                                "--nodes", cluster.get(1).config().broadcastAddress().getHostString(),
                                                                "--port", Integer.toString(cluster.get(1).callOnInstance(DatabaseDescriptor::getNativeTransportPort)),
                                                                "--storage-port", Integer.toString(cluster.get(1).callOnInstance(DatabaseDescriptor::getStoragePort)),
                                                                sstablesToUpload.absolutePath());
            assertNotEquals(0, tool.getExitCode());
            assertTrue(tool.getStderr().contains("check its replication settings"));
        }
    }
}
