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
package org.apache.cassandra.distributed.test.tracking;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;

public class MutationTrackingWriteForwardingTest extends TestBaseImpl
{
    // Need a mix of local and remote replicas, multiple remote replicas to ensure inter-DC forwarding hits both
    // paths for chosen replica in a remote DC, and all other replicas in a remote DC.
    // Include an extra node to ensure non-replicas behave correctly.
    private static final int NODES = 5;
    private static final int RF_PER_DC = 2;
    private static final int NUM_DCS = 2;
    private static final int TOTAL_RF = RF_PER_DC * NUM_DCS;

    private static int inst(int i)
    {
        return (i % NODES) + 1;
    }

    @Test
    public void testBasicWriteForwarding() throws Throwable
    {
        // 2 DCs, 1 replica in each, to test forwarding to instances in remote DCs and local DCs
        Map<Integer, NetworkTopology.DcAndRack> topology = networkTopology(NODES, (nodeid) -> nodeid % 2 == 1 ? dcAndRack("dc1", "rack1") : dcAndRack("dc2", "rack2"));

        // TODO: disable background reconciliation so we can test that writes are reconciling immediately
        try (Cluster cluster = Cluster.build(NODES)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .withNodeIdTopology(topology)
                                      .start())
        {
            String keyspaceName = "basic_write_forwarding_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': " + RF_PER_DC + "} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            int ROWS = 100;
            for (int inserted = 0; inserted < ROWS; inserted++)
            {
                // Writes should be completed for the client, regardless of whether they are forwarded or not
                cluster.coordinator(inst(inserted)).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?)", keyspaceName, tableName), ConsistencyLevel.ALL, inserted, inserted, inserted);
            }

            Thread.sleep(1000); // allow time for all offsets to be broadcasted

            int allReconciled = 0;
            int allUnreconciled = 0;

            // Writes should be ack'd in the journal too, but these could lag behind client acks, so could be
            // permissive here. Each write should be reconciled everywhere.
            for (IInvokableInstance instance : cluster)
            {
                int reconciled = instance.callOnInstance(() -> {
                    Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
                    Range<Token> fullRange = new Range<>(token, token);
                    TableId tableId = Schema.instance.getTableMetadata(keyspaceName, tableName).id;
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForRange(fullRange, tableId, true);
                    return summary.reconciledIds();
                });

                int unreconciled = instance.callOnInstance(() -> {
                    Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
                    Range<Token> fullRange = new Range<>(token, token);
                    TableId tableId = Schema.instance.getTableMetadata(keyspaceName, tableName).id;
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForRange(fullRange, tableId, true);
                    return summary.unreconciledIds();
                });

                allReconciled += reconciled;
                allUnreconciled += unreconciled;
            }

            Assert.assertEquals(0, allUnreconciled);
            Assert.assertEquals(ROWS * TOTAL_RF, allReconciled);
        }
    }
}
