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
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

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
 *       adopted dictionary id - i.e. new writes cluster-wide use the new dictionary.</li>
 * </ol>
 * Cadence note: the auto-training interval/initial-delay are minute-granular (minimum 60s), so this test genuinely
 * waits for a scheduled cycle rather than triggering one.
 */
public class CompressionDictionaryAutoTrainingDistributedTest extends TestBaseImpl
{
    private static final String TABLE = "recency_tbl";
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
            cluster.schemaChange(withKeyspace(
                "CREATE TABLE %s." + TABLE + " (id int PRIMARY KEY, v text) WITH compression = {" +
                "'class':'ZstdDictionaryCompressor'," +
                "'chunk_length_in_kb':4," +
                "'auto_training_enabled':'true'," +
                "'training_min_frequency':'0m'," +
                "'auto_training_improvement_threshold':'0.05'," +
                "'training_max_dictionary_size':'8KiB'," +
                "'training_max_total_sample_size':'64KiB'} " +
                "AND compaction = {'class':'TimeWindowCompactionStrategy'," +
                "'compaction_window_unit':'DAYS','compaction_window_size':1}"));

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
        }
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
