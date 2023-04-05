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

import java.net.InetAddress;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_ENDPOINTS;
import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_VERSIONS;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MigrationCoordinatorTest extends TestBaseImpl
{

    @Before
    public void setUp()
    {
        System.clearProperty("cassandra.replace_address");
        System.clearProperty("cassandra.consistent.rangemovement");

        System.clearProperty(IGNORED_SCHEMA_CHECK_VERSIONS.getKey());
        System.clearProperty(IGNORED_SCHEMA_CHECK_VERSIONS.getKey());
    }
    /**
     * We shouldn't wait on versions only available from a node being replaced
     * see CASSANDRA-
     */
    @Test
    public void replaceNode() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            InetAddress replacementAddress = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(2).shutdown(false);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            System.setProperty("cassandra.replace_address", replacementAddress.getHostAddress());
            cluster.bootstrap(config).startup();
        }
    }

    @Test
    public void explicitEndpointIgnore() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            InetAddress ignoredEndpoint = cluster.get(2).broadcastAddress().getAddress();
            cluster.get(2).shutdown(false);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IGNORED_SCHEMA_CHECK_ENDPOINTS.setString(ignoredEndpoint.getHostAddress());
            System.setProperty("cassandra.consistent.rangemovement", "false");
            cluster.bootstrap(config).startup();
        }
    }

    @Test
    public void explicitVersionIgnore() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            UUID initialVersion = cluster.get(2).callsOnInstance(() -> Schema.instance.getVersion()).call();
            cluster.schemaChange("CREATE KEYSPACE ks with replication={'class':'SimpleStrategy', 'replication_factor':2}");
            UUID oldVersion;
            do
            {
                oldVersion = cluster.get(2).callsOnInstance(() -> Schema.instance.getVersion()).call();
            } while (oldVersion.equals(initialVersion));
            cluster.get(2).shutdown(false);
            cluster.schemaChangeIgnoringStoppedInstances("CREATE TABLE ks.tbl (k int primary key, v int)");

            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            IGNORED_SCHEMA_CHECK_VERSIONS.setString(initialVersion.toString() + ',' + oldVersion);
            System.setProperty("cassandra.consistent.rangemovement", "false");
            cluster.bootstrap(config).startup();
        }
    }
}
