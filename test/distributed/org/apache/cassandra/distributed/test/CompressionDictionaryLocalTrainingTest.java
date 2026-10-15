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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.CompressionDictionaryDataObject;
import org.apache.cassandra.db.compression.CompressionDictionaryScheduler;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.CompressionDictionaryHelper;
import org.apache.cassandra.utils.JsonUtils;

import static org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.fromCompositeData;
import static org.apache.cassandra.db.compression.CompressionDictionaryDetailsTabularData.fromCompressionDictionary;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A dictionary trained or imported with local scope ({@code nodetool compressiondictionary train|import --local})
 * is used by that node only: it is persisted to system_distributed with the id of that node, which peers skip on refresh,
 * and it is not announced to them. A node which reads sstables written with it does not compress with it.
 * An export carries the node id, an import ignores it.
 */
public class CompressionDictionaryLocalTrainingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final int ROWS = 2000;

    @Test
    public void localDictionaryUsedByTrainedNodeOnly() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long dictId = currentDictId(cluster, 1);
            assertThat(dictId).isPositive();

            assertUsedByFirstNodeOnly(cluster, dictId);

            // on startup, sstables written with it cache it for reading only, the refresh makes it current again
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 1) == dictId);
        }
    }

    @Test
    public void localDictionaryUsedByImportingNodeOnly() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);

            String path = FileUtils.createDeletableTempFile("dictionary", ".json").absolutePath();
            long dictId = cluster.get(1).callOnInstance(() -> exportDictionary(path));

            assertThat(cluster.get(1).nodetool("compressiondictionary", "import", "--local", path)).isZero();
            assertThat(currentDictId(cluster, 1)).isEqualTo(dictId);

            assertUsedByFirstNodeOnly(cluster, dictId);
        }
    }

    @Test
    public void importIgnoresNodeId() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long local = currentDictId(cluster, 1);
            assertThat(local).isPositive();

            String path = FileUtils.createDeletableTempFile("dictionary", ".json").absolutePath();
            assertThat(cluster.get(1).nodetool("compressiondictionary", "export", KEYSPACE, TABLE, path)).isZero();
            CompressionDictionaryDataObject exported = JsonUtils.JSON_OBJECT_MAPPER.readValue(String.join("\n", FileUtils.readLines(new File(path))),
                                                                                              CompressionDictionaryDataObject.class);
            assertThat(exported.nodeId).isEqualTo(nodeId(cluster, 1));

            // nothing can be imported while the latest dictionary is local
            cluster.get(2).nodetoolResult("compressiondictionary", "import", path).asserts().failure().stderrContains("promote or discard it first");
            assertThat(cluster.get(1).nodetool("compressiondictionary", "promote", KEYSPACE, TABLE)).isZero();

            // the same dictionary can not be imported again, that could change whether it is global or local
            cluster.get(2).nodetoolResult("compressiondictionary", "import", path).asserts().failure().stderrContains("is already stored");
            cluster.get(2).nodetoolResult("compressiondictionary", "import", "--local", path).asserts().failure().stderrContains("is already stored");
            assertRows(persistedDictionaries(cluster), row(local, null));

            // one second newer, the upper bits of a dictionary id are seconds, with the id of a node which is not in the cluster
            long newer = local + (1L << 32);
            int checksum = CompressionDictionary.calculateChecksum((byte) CompressionDictionary.Kind.valueOf(exported.kind).ordinal(), newer, exported.dict);
            CompressionDictionaryDataObject toImport = new CompressionDictionaryDataObject(exported.keyspace, exported.table, exported.tableId, newer, exported.dict,
                                                                                           exported.kind, checksum, exported.dictLength, exported.createdAt, 42);
            FileUtils.write(new File(path), List.of(JsonUtils.writeAsPrettyJsonString(toImport)));

            assertThat(cluster.get(2).nodetool("compressiondictionary", "import", path)).isZero();
            assertRows(persistedDictionaries(cluster), row(newer, null), row(local, null));
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 1) == newer && currentDictId(cluster, 2) == newer && currentDictId(cluster, 3) == newer);
            for (int node = 1; node <= 3; node++)
                assertThat(currentNodeId(cluster, node)).isNull();
        }
    }

    @Test
    public void localDictionaryDoesNotSpreadWithSSTables() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long local = currentDictId(cluster, 1);
            assertThat(local).isPositive();
            insert(cluster, ROWS, 2 * ROWS);
            cluster.get(1).flush(KEYSPACE);

            // sstables of node 1 written with its local dictionary, the same way as when streamed as whole files
            String dir = Files.createTempDirectory("sstables").toString();
            cluster.get(1).runOnInstance(() -> copySSTables(local, dir));
            cluster.get(2).nodetoolResult("import", KEYSPACE, TABLE, dir).asserts().success();
            assertThat(sstableDictionaries(cluster, 2).values()).contains(local);

            // node 2 reads them but neither makes their dictionary current nor compacts with it
            assertThat(currentDictId(cluster, 2)).isZero();
            cluster.get(2).nodetoolResult("compact", KEYSPACE, TABLE).asserts().success();
            assertThat(sstableDictionaries(cluster, 2).values()).isNotEmpty().containsOnly(0L);
            assertRows(cluster.get(2).executeInternal(withKeyspace("SELECT count(*) FROM %s." + TABLE)), row(2L * ROWS));

            // promoted on node 2, which has it cached for reading only, then every node uses it
            assertThat(cluster.get(2).nodetool("compressiondictionary", "promote", KEYSPACE, TABLE)).isZero();
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 2) == local && currentDictId(cluster, 3) == local);
        }
    }

    @Test
    public void discardedLocalDictionaryNotUsed() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table"))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", KEYSPACE, TABLE)).isZero();
            long global = currentDictId(cluster, 1);
            assertThat(global).isPositive();

            // dictionary ids are second-granular, the local one has to be newer
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long local = currentDictId(cluster, 1);
            assertThat(local).isGreaterThan(global);
            insert(cluster, ROWS, 2 * ROWS);
            cluster.get(1).flush(KEYSPACE);

            // no node trains another dictionary until the local one is promoted or discarded
            cluster.get(1).nodetoolResult("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("promote or discard it first");
            cluster.get(2).nodetoolResult("compressiondictionary", "train", "--force", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("promote or discard it first");

            // discarded on another node, node 1 is notified and goes back to the global dictionary
            assertThat(cluster.get(2).nodetool("compressiondictionary", "discard", KEYSPACE, TABLE)).isZero();
            assertRows(persistedDictionaries(cluster), row(global, null));
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 1) == global);
            cluster.get(3).nodetoolResult("compressiondictionary", "discard", KEYSPACE, TABLE)
                   .asserts().failure().stderrContains("is used by all nodes, only a local one can be discarded");

            // new sstables are written with the global dictionary, those written with a discarded one remain readable
            Map<String, Long> before = sstableDictionaries(cluster, 1);
            assertThat(before.values()).contains(local);
            insert(cluster, 2 * ROWS, 3 * ROWS);
            cluster.get(1).flush(KEYSPACE);
            Map<String, Long> after = sstableDictionaries(cluster, 1);
            after.keySet().removeAll(before.keySet());
            assertThat(after.values()).isNotEmpty().containsOnly(global);
            assertRows(cluster.get(1).executeInternal(withKeyspace("SELECT count(*) FROM %s." + TABLE)), row(3L * ROWS));
        }
    }

    @Test
    public void refreshLoadsGlobalOrOwnDictionary() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table")
                                                             .set("compression_dictionary_refresh_interval", "10s"))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", KEYSPACE, TABLE)).isZero();
            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 1) > 0 && currentDictId(cluster, 3) == currentDictId(cluster, 1));
            long global = currentDictId(cluster, 1);
            assertThat(currentNodeId(cluster, 1)).isNull();

            // dictionary ids are second-granular, the local one has to be newer
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            assertThat(cluster.get(1).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            long local = currentDictId(cluster, 1);
            assertThat(local).isGreaterThan(global);
            assertThat(currentNodeId(cluster, 1)).isEqualTo(nodeId(cluster, 1));

            // more than a refresh page of dictionaries newer than the global one, local to a node which is not in the cluster
            String tableId = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata.id.toLongString());
            for (long dictId = global + 1; dictId <= global + CompressionDictionaryScheduler.REFRESH_PAGE_SIZE + 2; dictId++)
            {
                long id = dictId;
                cluster.forEach(i -> i.executeInternal("INSERT INTO system_distributed.compression_dictionaries " +
                                                       "(keyspace_name, table_name, table_id, kind, dict_id, dict_length, dict_checksum, created_at, node_id) " +
                                                       "VALUES (?, ?, ?, 'ZSTD', ?, 1, 0, toTimestamp(now()), 42)",
                                                       KEYSPACE, TABLE, tableId, id));
            }

            // no sstable carries either dictionary, a restarted node gets one from refresh only
            for (int node : new int[]{ 1, 3 })
            {
                cluster.get(node).shutdown().get();
                cluster.get(node).startup();
            }

            await().atMost(1, TimeUnit.MINUTES).until(() -> currentDictId(cluster, 1) == local && currentDictId(cluster, 3) == global);
            assertThat(currentDictId(cluster, 2)).isEqualTo(global);
        }
    }

    @Test
    public void cleanupRemovesDictionaryOfDecommissionedNode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.set("flush_compression", "table").with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            populate(cluster);

            assertThat(cluster.get(3).nodetool("compressiondictionary", "train", "--force", "--local", KEYSPACE, TABLE)).isZero();
            assertThat(currentDictId(cluster, 3)).isPositive();
            int nodeId = nodeId(cluster, 3);
            assertRows(persistedDictionaries(cluster), row(currentDictId(cluster, 3), nodeId));

            cluster.get(3).nodetoolResult("decommission", "--force").asserts().success();
            await().atMost(1, TimeUnit.MINUTES)
                   .until(() -> cluster.get(1).callOnInstance(() -> ClusterMetadata.current().directory.peerState(new NodeId(nodeId)) == NodeState.LEFT));

            cluster.get(1).nodetoolResult("compressiondictionary", "cleanup").asserts().success();
            assertThat(persistedDictionaries(cluster)).isEmpty();
        }
    }

    /** creates the table with autocompaction disabled and flushes rows written without any dictionary on every node */
    private static void populate(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s." + TABLE + " (pk int PRIMARY KEY, v text) " +
                                          "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}"));

        cluster.forEach(i -> i.nodetoolResult("disableautocompaction", KEYSPACE, TABLE).asserts().success());

        insert(cluster, 0, ROWS);

        cluster.forEach(i -> i.flush(KEYSPACE));
        for (int node = 1; node <= 3; node++)
            assertThat(sstableDictionaries(cluster, node).values()).isNotEmpty().containsOnly(0L);
    }

    /** node 1 writes new sstables with the dictionary, the other nodes do not compress with it */
    private static void assertUsedByFirstNodeOnly(Cluster cluster, long dictId)
    {
        assertThat(currentDictId(cluster, 2)).isZero();
        assertThat(currentDictId(cluster, 3)).isZero();
        assertRows(persistedDictionaries(cluster), row(dictId, nodeId(cluster, 1)));

        Map<String, Long> before = sstableDictionaries(cluster, 1);
        insert(cluster, ROWS, 2 * ROWS);
        cluster.forEach(i -> i.flush(KEYSPACE));

        Map<String, Long> after = sstableDictionaries(cluster, 1);
        assertThat(after).containsAllEntriesOf(before);
        after.keySet().removeAll(before.keySet());
        assertThat(after.values()).isNotEmpty().containsOnly(dictId);
        for (int node = 2; node <= 3; node++)
            assertThat(sstableDictionaries(cluster, node).values()).hasSizeGreaterThan(before.size()).containsOnly(0L);

        String select = withKeyspace("SELECT v FROM %s." + TABLE + " WHERE pk = ?");
        assertRows(cluster.get(1).executeInternal(select, ROWS), row(value(ROWS)));
        assertRows(cluster.coordinator(2).execute(select, ConsistencyLevel.ALL, 2 * ROWS - 1), row(value(2 * ROWS - 1)));
    }

    /** copies all components of the sstables written with the dictionary to dir */
    private static void copySSTables(long dictId, String dir)
    {
        for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getLiveSSTables())
        {
            CompressionDictionary dictionary = sstable.getCompressionMetadata().compressionDictionary();
            if (dictionary == null || dictionary.dictId().id != dictId)
                continue;

            for (String path : sstable.getAllFilePaths())
            {
                File component = new File(path);
                try
                {
                    Files.copy(component.toPath(), new File(dir, component.name()).toPath());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    /** writes a dictionary which is not stored in the cluster to path, the way nodetool compressiondictionary export does */
    private static long exportDictionary(String path)
    {
        CompressionDictionary dictionary = CompressionDictionaryHelper.INSTANCE.trainDictionary(KEYSPACE, TABLE);
        String tableId = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata.id.toLongString();
        String json = JsonUtils.writeAsPrettyJsonString(fromCompositeData(fromCompressionDictionary(KEYSPACE, TABLE, tableId, dictionary)));
        FileUtils.write(new File(path), List.of(json));
        return dictionary.dictId().id;
    }

    private static void insert(Cluster cluster, int from, int to)
    {
        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (pk, v) VALUES (?, ?)");
        for (int pk = from; pk < to; pk++)
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, pk, value(pk));
    }

    private static String value(int pk)
    {
        return "2025-10-20 11:00:01 INFO com.example.UserService request " + pk + " processed in 12ms";
    }

    /** dict_id and node_id of the persisted dictionaries, node_id is null for a global one */
    private static Object[][] persistedDictionaries(Cluster cluster)
    {
        return cluster.coordinator(1).execute("SELECT dict_id, node_id FROM system_distributed.compression_dictionaries", ConsistencyLevel.QUORUM);
    }

    private static int nodeId(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
    }

    /** {@link CompressionDictionary#nodeId()} of the dictionary a node compresses with */
    private static Integer currentNodeId(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> {
            NodeId nodeId = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).compressionDictionaryManager().getCurrent().nodeId();
            return nodeId == null ? null : nodeId.id();
        });
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
