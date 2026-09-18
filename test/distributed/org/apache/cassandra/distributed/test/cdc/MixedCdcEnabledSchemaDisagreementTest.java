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

package org.apache.cassandra.distributed.test.cdc;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests covering clusters with mixed CDC configurations
 */
public class MixedCdcEnabledSchemaDisagreementTest extends TestBaseImpl
{
    /**
     * ALTER TABLE ... WITH cdc=true, issued from a node that does have
     * cdc_enabled=true, must be visible as cdc=true in system_schema.tables on every node -
     * including ones with cdc_enabled=false.
     */
    @Test
    public void testAlterTableCdcMixedClusterCausesSchemaDisagreement() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> {
                                               if (config.num() == 3)
                                                   config.set("cdc_enabled", true);
                                               config.with(NETWORK, GOSSIP);
                                           })
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k INT PRIMARY KEY, v INT)"));

            // node 3 is the only node with cdc_enabled=true; it's the one flipping cdc on.
            cluster.get(3).schemaChangeInternal(withKeyspace("ALTER TABLE %s.tbl WITH cdc = true"));

            for (int i = 1; i <= 3; i++)
            {
                Object[][] rows = cluster.get(i).executeInternal(
                    "SELECT cdc FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                    KEYSPACE, "tbl");
                assertRows(rows, row(true));
            }
        }
    }

    /**
     * bootstrapping a brand-new node with cdc_enabled=false into a cluster that
     * already has cdc=true tables must not leave the new node's local system_schema.tables
     * disagreeing with the rest of the cluster.
     */
    @Test
    public void testBootstrapNodeWithCdcDisabledCausesSchemaDisagreement() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                             .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                             .withConfig(config -> config.set("cdc_enabled", true)
                                                                         .with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k INT PRIMARY KEY, v INT) WITH cdc=true"));

            IInstanceConfig newNodeConfig = cluster.newInstanceConfig();
            newNodeConfig.set("auto_bootstrap", true);
            newNodeConfig.set("cdc_enabled", false);

            IInvokableInstance newNode = cluster.bootstrap(newNodeConfig);
            newNode.startup();

            Object[][] rows = newNode.executeInternal(
                "SELECT cdc FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
                KEYSPACE, "tbl");
            assertRows(rows, row(true));
        }
    }
}
