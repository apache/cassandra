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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;

public class MutationTrackingWriteForwardingTest extends TestBaseImpl
{
    private static final int NODES = 3;
    private static final int RF = 1;

    private static int inst(int i)
    {
        return (i % NODES) + 1;
    }

    @Test
    public void testBasicWriteForwarding() throws Throwable
    {
        // 2 DCs, 1 replica in each, to test forwarding to instances in remote DCs and local DCs
        Map<Integer, NetworkTopology.DcAndRack> topology = networkTopology(3, (nodeid) -> nodeid % 2 == 1 ? dcAndRack("dc1", "rack1") : dcAndRack("dc2", "rack2"));

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
                                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': " + RF + "} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            Map<IInstance, Integer> instanceUnreconciled = new HashMap<>();
            int ROWS = 100;
            for (int inserted = 0; inserted < ROWS; inserted++)
            {
                // Writes should be completed for the client, regardless of whether they are forwarded or not
                cluster.coordinator(inst(inserted)).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?)", keyspaceName, tableName), ConsistencyLevel.ALL, inserted, inserted, inserted);

                // Writes should be ack'd in the journal too, but these could lag behind client acks, so could be
                // permissive here. Each write should be reconciled on the leader, unreconciled on the replica (until
                // background reconciliation broadcast is implemented), and ignored on others.
                IInstance replica = null;
                for (IInvokableInstance instance : cluster)
                {
                    int unreconciled = instance.callOnInstance(() -> {
                        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
                        Range<Token> fullRange = new Range<>(token, token);
                        TableId tableId = Schema.instance.getTableMetadata(keyspaceName, tableName).id;
                        MutationSummary summary = MutationTrackingService.instance.createSummaryForRange(fullRange, tableId, true);
                        return summary.unreconciledIds();
                    });
                    int lastUnreconciled = instanceUnreconciled.getOrDefault(instance, 0);
                    int newUnreconciled = unreconciled - lastUnreconciled;
                    if (newUnreconciled == 1)
                    {
                        Assertions.assertThat(replica).isNull();
                        replica = instance;
                    }
                    instanceUnreconciled.put(instance, unreconciled);
                }
                Assertions.assertThat(replica).isNotNull();
            }
            Assertions.assertThat(instanceUnreconciled).matches(map -> {
                int sum = 0;
                for (Integer value : map.values())
                    sum += value;
                return sum == ROWS;
            });
        }
    }
}
