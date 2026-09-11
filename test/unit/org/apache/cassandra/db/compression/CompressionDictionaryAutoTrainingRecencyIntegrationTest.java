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

package org.apache.cassandra.db.compression;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingManager.CompressionRatioEvaluator;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test of the recency-biased auto-training loop: it exercises window selection, real data loading,
 * a real training + adoption evaluation, and persistence.
 * <p>
 * The data drifts between two TWCS windows - window 1 and window 2 use two <em>disjoint</em> vocabularies - so a
 * dictionary trained on the recent window (window 2) compresses recent data markedly better than one trained on the
 * old window (window 1). The test:
 * <ol>
 *   <li>creates a TWCS, dictionary-compressed, auto-training-enabled table;</li>
 *   <li>loads window 1 and trains the first ("latest") dictionary by hand;</li>
 *   <li>loads window 2 with a different distribution;</li>
 *   <li>starts the real auto-trainer and waits for a run;</li>
 *   <li>asserts the freshly trained dictionary compresses the recent window better than the latest one, and that it
 *       was persisted (adopted) on top of the old one.</li>
 * </ol>
 */
public class CompressionDictionaryAutoTrainingRecencyIntegrationTest extends CQLTester
{
    private static final long MICROS_PER_DAY = 86_400L * 1_000_000L;
    private static final int ROWS_PER_WINDOW = 1200;
    private static final int VOCAB_SIZE = 256;
    private static final int PHRASE_LEN = 32;
    private static final int PHRASES_PER_ROW = 12;

    private Config.FlushCompression originalFlushCompression;

    @Before
    public void useTableFlushCompression()
    {
        // flush with the table's own (dictionary) compressor at its small chunk length, so the sampled chunks are
        // small enough that the dictionary - not in-chunk LZ matching - is what drives the compression ratio
        originalFlushCompression = DatabaseDescriptor.getFlushCompression();
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
    }

    @After
    public void restoreFlushCompression()
    {
        DatabaseDescriptor.setFlushCompression(originalFlushCompression);
    }

    @Test
    public void autoTrainerAdoptsADictionaryThatCompressesTheRecentWindowBetter() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) WITH compression = {" +
                                       "'class':'ZstdDictionaryCompressor'," +
                                       "'auto_training_enabled':'true'," +
                                       "'chunk_length_in_kb':4," +
                                       "'training_max_dictionary_size':'16KiB'," +
                                       "'training_max_total_sample_size':'256KiB'," +
                                       "'auto_training_improvement_threshold':'0.05'} " +
                                       "AND compaction = {'class':'TimeWindowCompactionStrategy'," +
                                       "'compaction_window_unit':'DAYS','compaction_window_size':1}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(tableName);
        cfs.disableAutoCompaction(); // keep each window's flush as its own SSTable
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // window 1: vocabulary A; hand-train the first ("latest") dictionary on it
        writeWindow(1, vocabulary(1));
        manager.train(true, Map.of());
        await("hand-trained dictionary is persisted")
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> retrieveLatest(cfs) != null);

        CompressionDictionary firstDictionary = retrieveLatest(cfs);
        assertThat(firstDictionary).isNotNull();
        long firstDictId = firstDictionary.dictId().id;

        // window 2: a disjoint vocabulary B (the drift), newer than window 1
        writeWindow(2, vocabulary(2));

        //start the real auto-trainer, constrained to this table, and wait for it to adopt a new dictionary
        CompressionDictionaryAutoTrainingManager autoTrainer = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            Iterable<ColumnFamilyStore> getTables()
            {
                return Collections.singletonList(cfs);
            }

            @Override
            boolean isFirstCMSMember()
            {
                return true;
            }
        };

        try
        {
            autoTrainer.start(0, 1, TimeUnit.SECONDS);
            await("auto-trainer trains on the recent window and adopts a new dictionary")
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(retrieveLatest(cfs).dictId().id)
                                 .as("a new dictionary must be persisted on top of the hand-trained one")
                                 .isNotEqualTo(firstDictId));
        }
        finally
        {
            autoTrainer.close();
        }

        CompressionDictionary adoptedDictionary = retrieveLatest(cfs);
        assertThat(adoptedDictionary.dictId().id)
        .as("the persisted latest dictionary is the newly adopted one, not the hand-trained one")
        .isNotEqualTo(firstDictId);

        // the adopted (recent-window) dictionary compresses the recent window
        // better than the hand-trained (old-window) one.
        CompressionDictionaryTrainingConfig config = manager.createTrainingConfig(Map.of());
        ColumnFamilyStore.RefViewFragment recentWindow = autoTrainer.resolveViewFragment(cfs);
        assertThat(recentWindow).as("the recent window must resolve to a non-null fragment").isNotNull();

        ICompressionDictionaryTrainer trainer = autoTrainer.createTrainer(cfs);
        trainer.start(config);
        // CompressionRatioEvaluator.close() closes both the trainer and the fragment
        try (CompressionRatioEvaluator evaluator = autoTrainer.createEvaluator(cfs, recentWindow, trainer, config))
        {
            double staleRatioOnRecent = evaluator.evaluate(firstDictionary);
            double adoptedRatioOnRecent = evaluator.evaluate(adoptedDictionary);

            assertThat(adoptedRatioOnRecent)
            .as("dictionary trained on the recent window compresses the recent window better (lower ratio=%s) " +
                "than the stale dictionary (ratio=%s)", adoptedRatioOnRecent, staleRatioOnRecent)
            .isLessThan(staleRatioOnRecent);
        }
    }

    private CompressionDictionary retrieveLatest(ColumnFamilyStore cfs)
    {
        return SystemDistributedKeyspace.retrieveLatestCompressionDictionary(cfs.getKeyspaceName(),
                                                                             cfs.getTableName(),
                                                                             cfs.metadata().id.toLongString());
    }

    /**
     * Writes {@code ROWS_PER_WINDOW} rows whose timestamps all fall in {@code day}'s TWCS window, then flushes.
     */
    private void writeWindow(int day, String[] vocabulary)
    {
        long base = day * MICROS_PER_DAY;
        Random random = new Random(day * 7919L); // deterministic per window
        for (int i = 0; i < ROWS_PER_WINDOW; i++)
            execute("INSERT INTO %s (id, v) VALUES (?, ?) USING TIMESTAMP ?", day * 100_000 + i, row(vocabulary, random), base + i);
        flush();
    }

    /**
     * A row value: {@code PHRASES_PER_ROW} space-joined phrases drawn (with repetition) from the window's vocabulary.
     */
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
     * effectively disjoint vocabularies (random 32-char strings collide with negligible probability), which is what
     * makes a dictionary trained on one window compress the other window poorly.
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
