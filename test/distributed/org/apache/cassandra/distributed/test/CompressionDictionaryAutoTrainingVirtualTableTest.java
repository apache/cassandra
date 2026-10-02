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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingHistory;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The auto-training history is node-local: each node records only the decisions it made. The
 * {@code system_views_remote} counterpart reads that local table on every node, so one coordinator answers for the
 * whole cluster, and a {@code node_id} restriction narrows the read to a single node.
 */
public class CompressionDictionaryAutoTrainingVirtualTableTest extends TestBaseImpl
{
    private static final String LOCAL = "SELECT keyspace_name, table_name, kind, improvement, promoted " +
                                       "FROM system_views.compression_dictionary_auto_training";
    private static final String REMOTE = "SELECT node_id, keyspace_name, table_name, kind, improvement, promoted " +
                                         "FROM system_views_remote.compression_dictionary_auto_training";

    @Test
    public void decisionsOfEveryNodeAreReadableFromOneCoordinator() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(GOSSIP, NETWORK)).start()))
        {
            // a distinct decision per node, so each row identifies the node that recorded it
            for (int i = 1; i <= 2; i++)
            {
                String keyspaceName = "ks" + i;
                String tableName = "tbl" + i;
                double improvement = 0.2 * i;
                boolean promoted = i == 1;
                cluster.get(i).runOnInstance(() ->
                    CompressionDictionaryAutoTrainingHistory.instance.record(1000L, keyspaceName, tableName,
                                                                            CompressionDictionary.Kind.ZSTD,
                                                                            0.5, 0.4, improvement, 0.15, promoted));
            }

            // the local table on each node sees only that node's own decision
            assertThat(rowsOf(cluster.coordinator(1).execute(LOCAL, ConsistencyLevel.ONE)))
            .describedAs("node1 local view")
            .containsExactly("ks1|tbl1|ZSTD|0.2|true");
            assertThat(rowsOf(cluster.coordinator(2).execute(LOCAL, ConsistencyLevel.ONE)))
            .describedAs("node2 local view")
            .containsExactly("ks2|tbl2|ZSTD|0.4|false");

            // one coordinator, both nodes' decisions
            Object[][] all = cluster.coordinator(1).execute(REMOTE, ConsistencyLevel.ONE);
            assertThat(all.length).describedAs("one row per node").isEqualTo(2);
            assertThat(rowsOf(all, 1))
            .describedAs("cluster-wide view from node1")
            .containsExactlyInAnyOrder("ks1|tbl1|ZSTD|0.2|true", "ks2|tbl2|ZSTD|0.4|false");

            // restricting node_id reads that node alone, and node_id identifies the recording node
            for (Object[] row : all)
            {
                int nodeId = (Integer) row[0];
                Object[][] single = cluster.coordinator(1).execute(REMOTE + " WHERE node_id = ?",
                                                                  ConsistencyLevel.ONE, nodeId);
                assertThat(rowsOf(single, 1))
                .describedAs("node_id " + nodeId + " in isolation")
                .containsExactly(rowOf(row, 1));
            }
        }
    }

    private static List<String> rowsOf(Object[][] rows)
    {
        return rowsOf(rows, 0);
    }

    private static List<String> rowsOf(Object[][] rows, int from)
    {
        List<String> result = new ArrayList<>();
        for (Object[] row : rows)
            result.add(rowOf(row, from));
        return result;
    }

    private static String rowOf(Object[] row, int from)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < row.length; i++)
        {
            if (i > from)
                sb.append('|');
            sb.append(row[i]);
        }
        return sb.toString();
    }
}
