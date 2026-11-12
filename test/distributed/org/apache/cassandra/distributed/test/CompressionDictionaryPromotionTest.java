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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A dictionary trained with local scope ({@code nodetool compressiondictionary train --local}) is persisted with
 * the id of its node, so it is used by that node only. {@code nodetool compressiondictionary promote} clears the node id
 * and announces the dictionary, so every node uses it right away.
 */
public class CompressionDictionaryPromotionTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 2000;

    @Test
    public void promotedDictionaryUsedByAllNodes() throws Throwable
    {
        // the periodic refresh does not run during the test, nodes learn about the promoted dictionary from the promotion
        // and its announcement only
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table")
                                                             .set("compression_dictionary_refresh_initial_delay", "1h")
                                                             .set("compression_dictionary_refresh_interval", "1h"))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text) " +
                                              "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, TABLE).asserts().success());

            assertThat(insertAndFlush(cluster, 0, ROWS)).containsOnly(Set.of(0L));
            cluster.get(1).nodetoolResult("compressiondictionary", "promote", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("Could not find a dictionary to promote");

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long dictId = currentDictId(cluster, 1);
            assertThat(dictId).isPositive();
            int nodeId = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertRows(persistedDictionaries(cluster), row(dictId, nodeId));

            assertThat(insertAndFlush(cluster, ROWS, 2 * ROWS)).containsExactly(Set.of(dictId), Set.of(0L), Set.of(0L));

            // promoted on node 2, which did not train it; the announcement skips node 2, which refreshes on its own
            assertThat(cluster.get(2).nodetool("compressiondictionary", "promote", KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(dictId, null));
            cluster.get(2).nodetoolResult("compressiondictionary", "promote", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("is already used by all nodes");
            cluster.get(3).nodetoolResult("compressiondictionary", "discard", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("is used by all nodes, only a local one can be discarded");

            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 2) == dictId && currentDictId(cluster, 3) == dictId);
            assertThat(insertAndFlush(cluster, 2 * ROWS, 3 * ROWS)).containsOnly(Set.of(dictId));

            for (int node = 1; node <= 3; node++)
                assertRows(cluster.get(node).executeInternal(withKeyspace("SELECT count(*) FROM %s." + TABLE)), row(3L * ROWS));
        }
    }

    /**
     * inserts and flushes rows from..to, returns per node the ids of the dictionaries the new sstables were written
     * with, 0 when none
     */
    private static List<Set<Long>> insertAndFlush(Cluster cluster, int from, int to)
    {
        List<Map<String, Long>> before = new ArrayList<>();
        for (int node = 1; node <= 3; node++)
            before.add(sstableDictionaries(cluster, node));

        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
        for (int pk = from; pk < to; pk++)
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, pk, "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms");
        cluster.forEach(i -> i.flush(KEYSPACE));

        List<Set<Long>> created = new ArrayList<>();
        for (int node = 1; node <= 3; node++)
        {
            Map<String, Long> after = sstableDictionaries(cluster, node);
            assertThat(after).containsAllEntriesOf(before.get(node - 1));
            after.keySet().removeAll(before.get(node - 1).keySet());
            created.add(new HashSet<>(after.values()));
        }
        return created;
    }

    /** dict_id and node_id of the persisted dictionaries, node_id is null for a global one */
    private static Object[][] persistedDictionaries(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT dict_id, node_id FROM system_distributed.compression_dictionaries", ConsistencyLevel.QUORUM);
    }

    private static long currentDictId(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            CompressionDictionary current = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE)
                                                    .compressionDictionaryManager()
                                                    .getCurrent();

            return current == null ? 0L : current.dictId().id;
        });
    }

    /**
     * sstable id to the id of the dictionary it was written with, 0 when none
     */
    private static Map<String, Long> sstableDictionaries(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            Map<String, Long> result = new HashMap<>();
            for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables())
            {
                CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
                result.put(sstable.descriptor.id.toString(), dictionary == null ? 0L : dictionary.dictId().id);
            }
            return result;
        });
    }
}
