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
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.metrics.StorageMetrics;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.assertj.core.api.Assertions.assertThat;

public class HintsDisabledTest extends TestBaseImpl
{
    @Test
    public void testHintedHandoffDisabled() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("write_request_timeout", "10ms")
                                                                       .set("hinted_handoff_enabled", false))
                                           .start(), 2))
        {
            String createTableStatement = String.format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 text) " +
                                                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'} ", KEYSPACE);
            cluster.schemaChange(createTableStatement);

            // Drop all messages from node1 to node2 so hints should be created
            IMessageFilters.Filter drop1to2 = cluster.filters().verbs(MUTATION_REQ.id).from(1).to(2).drop();

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?) USING TIMESTAMP 1;"),
                                           ConsistencyLevel.ONE,
                                           String.valueOf(1),
                                           String.valueOf(1));

            // Wait 15ms for write to timeout (write_request_timeout=10ms)
            Uninterruptibles.sleepUninterruptibly(15, TimeUnit.MILLISECONDS);

            // Check that no hints were created on node1
            assertThat(cluster.get(1).callOnInstance(() -> Long.valueOf(StorageMetrics.totalHints.getCount()))).isEqualTo(0L);
        }
    }

    private static int getNumberOfSSTables(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> ColumnFamilyStore.getIfExists(KEYSPACE, "cf").getLiveSSTables().size());
    }
}
