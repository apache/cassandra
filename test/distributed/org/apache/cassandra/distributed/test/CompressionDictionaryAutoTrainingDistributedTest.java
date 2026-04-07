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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingHistory;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;

/**
 * Distributed (in-JVM, 3-node) end-to-end test of compression-dictionary auto-training.
 * <p>
 * Mirrors the single-node recency integration test, but exercises the full cluster path: every node boots with
 * auto-training enabled, one node's scheduled auto-trainer (the CMS leader) adopts a better dictionary, and the
 * adopted dictionary is broadcast to the other nodes so that <em>new writes on every node</em> are compressed with it.
 * <p>
 * Flow:
 * <ol>
 *   <li>3-node cluster, RF=3, {@code compression_dictionary_auto_training_enabled=true} on each node;</li>
 *   <li>a TWCS, dictionary-compressed, auto-training-enabled table;</li>
 *   <li>load window 1 (vocabulary A), flush every node, and hand-train the first ("latest") dictionary;</li>
 *   <li>load window 2 with a <em>disjoint</em> vocabulary B (the drift), flush every node;</li>
 *   <li>wait for the scheduled auto-trainer to adopt a new dictionary (persisted to {@code system_distributed});</li>
 *   <li>wait for that dictionary to propagate to every node's current-dictionary cache;</li>
 *   <li>write more data and flush on every node, then assert every node's fresh SSTables are compressed with the
 *       adopted dictionary id - i.e. new writes cluster-wide use the new dictionary;</li>
 *   <li>read the decision back from {@code system_views.compression_dictionary_auto_training} on the node that
 *       trained, and from {@code system_views_remote.compression_dictionary_auto_training} on every node, which
 *       answers for the whole cluster.</li>
 * </ol>
 * Cadence note: the auto-training interval/initial-delay are minute-granular (minimum 60s), so this test genuinely
 * waits for a scheduled cycle rather than triggering one.
 */
public class CompressionDictionaryAutoTrainingDistributedTest extends TestBaseImpl
{
    private static final String TABLE = "recency_tbl";

    /** The node-local view: only the node that ran the training has rows. */
    private static final String LOCAL_DECISIONS =
        "SELECT node, kind, baseline_ratio, candidate_ratio, improvement, threshold, promoted " +
        "FROM system_views.compression_dictionary_auto_training WHERE keyspace_name = ? AND table_name = ?";

    /** The cluster-wide view: any coordinator answers for every node, each row tagged with its node_id. */
    private static final String REMOTE_DECISIONS =
        "SELECT node_id, node, keyspace_name, table_name, kind, improvement, threshold, promoted " +
        "FROM system_views_remote.compression_dictionary_auto_training";

    private static final String REMOTE_DECISIONS_OF_NODE = REMOTE_DECISIONS + " WHERE node_id = ?";

    /** Matches {@code auto_training_improvement_threshold} in the table definition below. */
    private static final double THRESHOLD = 0.05;
    private static final long MICROS_PER_DAY = 86_400L * 1_000_000L;
    private static final int ROWS_PER_WINDOW = 250;
    private static final int VOCAB_SIZE = 256;
    private static final int PHRASE_LEN = 32;
    private static final int PHRASES_PER_ROW = 32; // ~1KiB per value

    @Test
    public void newWritesUseAutoTrainedDictionaryAcrossCluster() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.set("compression_dictionary_auto_training_enabled", true)
                                                               .set("compression_dictionary_auto_training_initial_delay", "1m")
                                                               .set("compression_dictionary_auto_training_interval", "1m")
                                                               // flush with the table's own (dictionary) compressor at its 4KiB chunk length, so
                                                               // flushed SSTables have enough small chunks to sample for training
                                                               .set("flush_compression", "table"))
                                             .start(),
                                    3))
        {
            String ks = KEYSPACE;

            // 1. auto-training must be enabled on every node
            for (int n = 1; n <= 3; n++)
                Assert.assertTrue("auto-training must be enabled on node " + n,
                                  cluster.get(n).callOnInstance(DatabaseDescriptor::getCompressionDictionaryAutoTrainingEnabled));

            // 2. TWCS, dictionary-compressed, auto-training-enabled table (small sample sizes so training is quick)
            cluster.schemaChange(createTable(ks));

            // 3. window 1 (vocabulary A) -> flush everywhere -> hand-train the first ("latest") dictionary.
            //    This trains on node 1 directly (equivalent to `nodetool compressiondictionary train`, but without
            //    needing a JMX feature) and persists+broadcasts the dictionary cluster-wide.
            writeWindow(cluster, 1, vocabulary(1));
            cluster.forEach(i -> i.flush(ks));
            cluster.get(1).runOnInstance(() ->
                Keyspace.open(ks).getColumnFamilyStore(TABLE).compressionDictionaryManager().train(true, Collections.emptyMap()));
            await("hand-trained dictionary becomes available on node 1")
            .atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
            .until(() -> currentDictId(cluster.get(1), ks) > 0);

            long firstDictId = currentDictId(cluster.get(1), ks);
            Assert.assertTrue("a hand-trained dictionary must exist, was " + firstDictId, firstDictId > 0);

            // 4. window 2 (disjoint vocabulary B = the drift) -> flush everywhere
            writeWindow(cluster, 2, vocabulary(2));
            cluster.forEach(i -> i.flush(ks));

            // 5. wait for the scheduled auto-trainer (running on the CMS leader) to adopt and persist a new dictionary
            await("auto-training adopts a new dictionary")
            .atMost(4, TimeUnit.MINUTES).pollInterval(5, TimeUnit.SECONDS)
            .until(() -> {
                long latest = latestPersistedDictId(cluster.get(1), ks);
                return latest > 0 && latest != firstDictId;
            });
            long adoptedDictId = latestPersistedDictId(cluster.get(1), ks);
            Assert.assertNotEquals("a new dictionary must have been adopted", firstDictId, adoptedDictId);

            // 6. the adopted dictionary must propagate to every node's current-dictionary cache
            for (int n = 1; n <= 3; n++)
            {
                IInvokableInstance instance = cluster.get(n);
                await("node " + n + " picks up the adopted dictionary")
                .atMost(1, TimeUnit.MINUTES).pollInterval(2, TimeUnit.SECONDS)
                .until(() -> currentDictId(instance, ks) == adoptedDictId);
            }

            // 7. new writes on every node must be compressed with the adopted dictionary
            writeWindow(cluster, 3, vocabulary(2));
            cluster.forEach(i -> i.flush(ks));
            for (int n = 1; n <= 3; n++)
            {
                Set<Long> ids = sstableDictIds(cluster.get(n), ks);
                Assert.assertTrue("node " + n + " must have SSTable(s) written with the adopted dictionary " +
                                  adoptedDictId + ", but saw dictionary ids " + ids,
                                  ids.contains(adoptedDictId));
            }

            // 8. the decision behind that adoption must be readable from the virtual tables
            assertDecisionVisibleLocally(cluster, ks);
            assertDecisionVisibleClusterWide(cluster, ks);
        }
    }

    /**
     * Auto-training runs on exactly one node: the first CMS member, which is the lowest {@code NodeId} among
     * {@link ClusterMetadata#fullCMSMemberIds()}. That set is CMS <em>membership</em>, not liveness, so the role moves
     * only when membership changes - a reconfiguration that drops the current holder, the holder leaving the cluster,
     * or a node with a lower NodeId joining the CMS. A holder that is merely down does not hand over; auto-training
     * just stops until it returns or the CMS is reconfigured.
     * <p>
     * Here the CMS is moved to the other datacenter, which drops node1 from it, and the recording of decisions must
     * follow to the new holder.
     */
    @Test
    public void autoTrainingFollowsTheFirstCmsMember() throws Throwable
    {
        // 2 datacenters, 2 nodes each: datacenter1 = nodes 1,2; datacenter2 = nodes 3,4
        try (Cluster cluster = builder().withRacks(2, 1, 2)
                                        .withConfig(c -> c.with(GOSSIP).with(NETWORK)
                                                          .set("compression_dictionary_auto_training_enabled", true)
                                                          .set("compression_dictionary_auto_training_initial_delay", "1m")
                                                          .set("compression_dictionary_auto_training_interval", "1m")
                                                          .set("flush_compression", "table"))
                                        .start())
        {
            String ks = KEYSPACE;
            cluster.schemaChange("CREATE KEYSPACE " + ks + " WITH replication = " +
                                 "{'class':'NetworkTopologyStrategy','datacenter1':2,'datacenter2':2}");
            cluster.schemaChange(createTable(ks));

            // a baseline dictionary, so later cycles have something to compare a candidate against
            writeWindow(cluster, 1, vocabulary(1));
            cluster.forEach(i -> i.flush(ks));
            cluster.get(1).runOnInstance(() ->
                                         Keyspace.open(ks).getColumnFamilyStore(TABLE).compressionDictionaryManager().train(true, Collections.emptyMap()));
            await("hand-trained dictionary becomes available on node 1")
            .atMost(1, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
            .until(() -> currentDictId(cluster.get(1), ks) > 0);

            // drift, so every cycle from now on evaluates a candidate and therefore records a decision
            writeWindow(cluster, 2, vocabulary(2));
            cluster.forEach(i -> i.flush(ks));

            int firstHolder = firstCMSMember(cluster);
            Assert.assertEquals("node 1 starts out as the first CMS member", 1, firstHolder);
            awaitOnlyRecorder(cluster, ks, firstHolder);

            // move the CMS into the other datacenter, dropping node 1 from it
            cluster.get(1).nodetoolResult("cms", "reconfigure", "datacenter2:1").asserts().success();

            int secondHolder = firstCMSMember(cluster);
            Assert.assertNotEquals("moving the CMS must hand the role to another node", firstHolder, secondHolder);

            // forget the decisions recorded under the old holder, so the next cycle speaks for itself
            cluster.forEach(i -> i.runOnInstance(() -> CompressionDictionaryAutoTrainingHistory.instance.clear()));
            awaitOnlyRecorder(cluster, ks, secondHolder);
        }
    }

    /**
     * Auto-training runs on one node only (the first CMS member), so exactly one node records decisions, and its
     * node-local virtual table must describe the adoption: the configured threshold, an improvement that met it, a
     * candidate that compressed better than the baseline, and its own address.
     */
    private static void assertDecisionVisibleLocally(Cluster cluster, String ks)
    {
        int trainingNode = trainingNode(cluster, ks);
        Object[][] decisions = cluster.coordinator(trainingNode).execute(LOCAL_DECISIONS, ConsistencyLevel.ONE, ks, TABLE);
        Object[] promoted = promotedRow(decisions, 6);
        Assert.assertNotNull("node " + trainingNode + " must have recorded a promoted candidate, rows: " +
                             decisions.length, promoted);

        // columns: node, kind, baseline_ratio, candidate_ratio, improvement, threshold, promoted
        double baselineRatio = (Double) promoted[2];
        double candidateRatio = (Double) promoted[3];
        double improvement = (Double) promoted[4];
        double threshold = (Double) promoted[5];

        Assert.assertEquals("the trained dictionary kind must be recorded",
                            CompressionDictionary.Kind.ZSTD.name(), promoted[1]);

        Assert.assertEquals("threshold must be the table's configured one", THRESHOLD, threshold, 1e-9);
        Assert.assertTrue("a promoted candidate must have met the threshold, improvement=" + improvement +
                          " threshold=" + threshold, improvement >= threshold);
        Assert.assertTrue("the promoted candidate must compress better than the baseline, baseline=" + baselineRatio +
                          " candidate=" + candidateRatio, candidateRatio < baselineRatio);
        Assert.assertEquals("the row must name the node that trained",
                            cluster.get(trainingNode).config().broadcastAddress().getAddress(), promoted[0]);
    }

    /**
     * The same decision must be readable through {@code system_views_remote} from <em>any</em> coordinator, including
     * the nodes that did not train, and a {@code node_id} restriction must narrow the read to the node that did.
     */
    private static void assertDecisionVisibleClusterWide(Cluster cluster, String ks)
    {
        int trainingNode = trainingNode(cluster, ks);
        Object expectedAddress = cluster.get(trainingNode).config().broadcastAddress().getAddress();

        for (int n = 1; n <= 3; n++)
        {
            // columns: node_id, node, keyspace_name, table_name, kind, improvement, threshold, promoted
            Object[][] all = cluster.coordinator(n).execute(REMOTE_DECISIONS, ConsistencyLevel.ONE);
            Object[] promoted = promotedRow(ofTable(all, ks), 7);
            Assert.assertNotNull("the adoption must be visible cluster-wide from node " + n, promoted);
            Assert.assertEquals("the cluster-wide row must name the node that trained", expectedAddress, promoted[1]);
            Assert.assertEquals("the cluster-wide row must carry the dictionary kind",
                                CompressionDictionary.Kind.ZSTD.name(), promoted[4]);

            int nodeId = (Integer) promoted[0];
            Object[][] ofNode = cluster.coordinator(n).execute(REMOTE_DECISIONS_OF_NODE, ConsistencyLevel.ONE, nodeId);
            Assert.assertNotNull("reading node_id " + nodeId + " alone from node " + n + " must carry the adoption",
                                 promotedRow(ofTable(ofNode, ks), 7));
            for (Object[] row : ofNode)
                Assert.assertEquals("restricting node_id must only return that node's rows", nodeId, row[0]);
        }
    }

    /** The single node whose local history holds decisions for the table, waiting for it to appear. */
    private static int trainingNode(Cluster cluster, String ks)
    {
        Set<Integer> recording = new HashSet<>();
        await("exactly one node records auto-training decisions")
        .atMost(1, TimeUnit.MINUTES).pollInterval(2, TimeUnit.SECONDS)
        .until(() -> {
            recording.clear();
            for (int n = 1; n <= 3; n++)
            {
                if (cluster.coordinator(n).execute(LOCAL_DECISIONS, ConsistencyLevel.ONE, ks, TABLE).length > 0)
                    recording.add(n);
            }
            return recording.size() == 1;
        });
        return recording.iterator().next();
    }

    /** Rows whose {@code keyspace_name} is {@code ks} and whose table is {@link #TABLE}. */
    private static Object[][] ofTable(Object[][] rows, String ks)
    {
        List<Object[]> result = new ArrayList<>();
        for (Object[] row : rows)
        {
            if (ks.equals(row[2]) && TABLE.equals(row[3]))
                result.add(row);
        }
        return result.toArray(new Object[0][]);
    }

    /** The first row whose {@code promoted} column (at {@code promotedAt}) is true, or null. */
    private static Object[] promotedRow(Object[][] rows, int promotedAt)
    {
        for (Object[] row : rows)
        {
            if (Boolean.TRUE.equals(row[promotedAt]))
                return row;
        }
        return null;
    }


    /** TWCS, dictionary-compressed, auto-training-enabled, with small sample sizes so training is quick. */
    private static String createTable(String ks)
    {
        return "CREATE TABLE " + ks + '.' + TABLE + " (id int PRIMARY KEY, v text) WITH compression = {" +
               "'class':'ZstdDictionaryCompressor'," +
               "'chunk_length_in_kb':4," +
               "'auto_training_enabled':'true'," +
               "'training_min_frequency':'0m'," +
               "'auto_training_improvement_threshold':'" + THRESHOLD + "'," +
               "'training_max_dictionary_size':'8KiB'," +
               "'training_max_total_sample_size':'64KiB'} " +
               "AND compaction = {'class':'TimeWindowCompactionStrategy'," +
               "'compaction_window_unit':'DAYS','compaction_window_size':1}";
    }

    /**
     * The node the first-CMS-member rule picks, derived from cluster metadata rather than from the manager, so the
     * test states the rule independently of the code under test.
     */
    private static int firstCMSMember(Cluster cluster)
    {
        for (int n = 1; n <= cluster.size(); n++)
        {
            boolean isFirst = cluster.get(n).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                List<NodeId> members = new ArrayList<>(metadata.fullCMSMemberIds());
                members.sort(Comparator.comparingInt(NodeId::id));
                return !members.isEmpty() && members.get(0).equals(metadata.myNodeId());
            });

            if (isFirst)
                return n;
        }
        throw new AssertionError("no node considers itself the first CMS member");
    }

    /** Waits for {@code expected} to record a decision, then requires that no other node recorded one. */
    private static void awaitOnlyRecorder(Cluster cluster, String ks, int expected)
    {
        await("node " + expected + " records an auto-training decision")
        .atMost(4, TimeUnit.MINUTES).pollInterval(5, TimeUnit.SECONDS)
        .until(() -> decisionCount(cluster, ks, expected) > 0);

        for (int n = 1; n <= cluster.size(); n++)
        {
            if (n != expected)
                Assert.assertEquals("only the first CMS member may record decisions, but node " + n + " did",
                                    0, decisionCount(cluster, ks, n));
        }
    }

    private static int decisionCount(Cluster cluster, String ks, int node)
    {
        return cluster.coordinator(node).execute(LOCAL_DECISIONS, ConsistencyLevel.ONE, ks, TABLE).length;
    }

    /** The id of the dictionary new writes on this node currently use (the manager's cached current dictionary). */
    private static long currentDictId(IInvokableInstance instance, String ks)
    {
        return instance.callOnInstance(() -> {
            CompressionDictionary current = Keyspace.open(ks).getColumnFamilyStore(TABLE).compressionDictionaryManager().getCurrent();
            return current == null ? -1L : current.dictId().id;
        });
    }

    /** The id of the latest dictionary persisted in {@code system_distributed.compression_dictionaries}. */
    private static long latestPersistedDictId(IInvokableInstance instance, String ks)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(TABLE);
            CompressionDictionary latest = SystemDistributedKeyspace.retrieveLatestCompressionDictionary(ks, TABLE, cfs.metadata().id.toLongString());
            return latest == null ? -1L : latest.dictId().id;
        });
    }

    /** The distinct dictionary ids actually stored in this node's live SSTables (-1 = no dictionary). */
    private static Set<Long> sstableDictIds(IInvokableInstance instance, String ks)
    {
        return instance.callOnInstance(() -> {
            Set<Long> ids = new HashSet<>();
            ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(TABLE);
            try
            {
                // CompressionMetadata stores the dictionary the SSTable was compressed with, but exposes no public
                // getter, so read the private field reflectively (its DictId is public). This is what proves which
                // dictionary a given SSTable's data was actually compressed with.
                Field field = CompressionMetadata.class.getDeclaredField("compressionDictionary");
                field.setAccessible(true);
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    CompressionMetadata metadata = sstable.getCompressionMetadata();
                    CompressionDictionary dictionary = metadata == null ? null : (CompressionDictionary) field.get(metadata);
                    ids.add(dictionary == null ? -1L : dictionary.dictId().id);
                }
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
            return ids;
        });
    }

    /** Writes {@code ROWS_PER_WINDOW} rows (at ALL) whose timestamps fall in {@code day}'s TWCS window. */
    private static void writeWindow(Cluster cluster, int day, String[] vocabulary)
    {
        long base = day * MICROS_PER_DAY;
        Random random = new Random(day * 7919L); // deterministic per window
        String insert = withKeyspace("INSERT INTO %s." + TABLE + " (id, v) VALUES (?, ?) USING TIMESTAMP ?");
        for (int i = 0; i < ROWS_PER_WINDOW; i++)
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, day * 100_000 + i, row(vocabulary, random), base + i);
    }

    /** A row value: {@code PHRASES_PER_ROW} space-joined phrases drawn (with repetition) from the window's vocabulary. */
    private static String row(String[] vocabulary, Random random)
    {
        StringBuilder sb = new StringBuilder(PHRASES_PER_ROW * (PHRASE_LEN + 1));
        for (int p = 0; p < PHRASES_PER_ROW; p++)
        {
            if (p > 0)
                sb.append(' ');
            sb.append(vocabulary[random.nextInt(vocabulary.length)]);
        }
        return sb.toString();
    }

    /**
     * A deterministic vocabulary of {@code VOCAB_SIZE} distinct {@code PHRASE_LEN}-char phrases. Different seeds yield
     * effectively disjoint vocabularies, so a dictionary trained on one window compresses the other window poorly -
     * which is what makes the recency-trained candidate beat the stale one and get adopted.
     */
    private static String[] vocabulary(long seed)
    {
        Random random = new Random(seed);
        String[] vocabulary = new String[VOCAB_SIZE];
        for (int i = 0; i < VOCAB_SIZE; i++)
        {
            StringBuilder sb = new StringBuilder(PHRASE_LEN);
            for (int c = 0; c < PHRASE_LEN; c++)
                sb.append((char) ('a' + random.nextInt(26)));
            vocabulary[i] = sb.toString();
        }
        return vocabulary;
    }
}
