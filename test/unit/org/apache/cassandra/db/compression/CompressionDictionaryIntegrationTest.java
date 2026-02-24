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
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryIntegrationTest extends CQLTester
{
    private static final String REPEATED_DATA = "The quick brown fox jumps over the lazy dog. This text repeats for better compression. ";

    private final static String TRAINING_MAX_TOTAL_SAMPLE_SIZE = "128KiB";
    private final static String TRAINING_MAX_DICTIONARY_SIZE = "10KiB";

    @Before
    public void configureDatabaseDescriptor()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.flush_compression = Config.FlushCompression.table;
        DatabaseDescriptor.setConfig(config);
    }

    @Test
    public void testEnableDisableDictionaryCompression()
    {
        String table = createTable(getTableCql());
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Insert data and flush to create SSTables
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (pk, data) VALUES (?, ?)", Integer.toString(i), REPEATED_DATA + " " + i);
        }
        flush();

        assertThatNoException()
        .as("Should allow manual training")
        .isThrownBy(() -> manager.train(false,
                                        Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE,
                                               TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE)));

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        manager.maybeReloadFromSchema(nonDictParams);

        assertThatThrownBy(() -> manager.train(false,
                                               Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE,
                                                      TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE)))
        .as("Should disallow manual training when using lz4")
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support dictionary compression");

        // Re-enable dictionary compression
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Map.of("compression_level", "3",
                                                                     TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE,
                                                                     TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE));
        manager.maybeReloadFromSchema(dictParams);

        // Insert more data for the re-enabled compression
        for (int i = 100; i < 200; i++)
        {
            execute("INSERT INTO %s (pk, data) VALUES (?, ?)", Integer.toString(i), REPEATED_DATA + " " + i);
        }
        flush();

        assertThatNoException()
        .as("Should allow manual training after switching back to dictionary compression")
        .isThrownBy(() -> manager.train(false,
                                        Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE,
                                               TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE)));
    }

    @Test
    public void testCompressionParameterChanges()
    {
        String table = createTable(getTableCql());
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();
        ICompressionDictionaryTrainer trainer = manager.trainer();
        assertThat(trainer).isNotNull();
        assertThat(trainer.kind()).isEqualTo(Kind.ZSTD);

        // Change compression level - should create new trainer
        CompressionParams newParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                             Collections.singletonMap("compression_level", "5"));
        manager.maybeReloadFromSchema(newParams);
        ICompressionDictionaryTrainer newTrainer = manager.trainer();
        assertThat(newTrainer.kind()).isEqualTo(Kind.ZSTD);
        assertThat(newTrainer)
        .as("Should create a different trainer instance when compression level is changed")
        .isNotSameAs(trainer);
    }

    @Test
    public void testResourceCleanupOnClose() throws Exception
    {
        createTable(getTableCql());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Add test dictionary
        ZstdCompressionDictionary testDict = createTestDictionary();
        manager.add(testDict);

        assertThat(testDict.selfRef().globalCount())
        .as("Dictionary's reference count should be 1 after adding to cache")
        .isOne();

        assertThat(manager.getCurrent())
        .as("Should have current dictionary before close")
        .isNotNull();

        manager.close();

        assertThat(manager.trainer()).isNull();
        // Dictionary's reference count should be 0 after closing manager
        // Dictionary is closed in a separate thread. Wait a bit for the reference count to be updated.
        spinUntilTrue(() -> testDict.selfRef().globalCount() == 0, 1, TimeUnit.SECONDS);
        assertThat(testDict.rawDictionary())
        .as("The raw dictionary bytes should still be accessible")
        .isNotNull();
    }

    private static ZstdCompressionDictionary createTestDictionary()
    {
        byte[] dictBytes = (REPEATED_DATA + " dictionary training data").getBytes();
        DictId dictId = new DictId(Kind.ZSTD, Clock.Global.currentTimeMillis());
        return new ZstdCompressionDictionary(dictId, dictBytes);
    }

    @Test
    public void testSSTableBasedTraining()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        String table = createTable(getTableCqlWithChunkLength());
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Insert compressible data and flush to create SSTables
        for (int i = 0; i < 1000; i++)
        {
            execute("INSERT INTO %s (pk, data) VALUES (?, ?)",
                    "key" + i,
                    REPEATED_DATA + " row " + i);
            if (i % 200 == 0)
                flush();
        }
        flush();

        // Verify we have SSTables
        assertThat(cfs.getLiveSSTables())
        .as("Should have created SSTables")
        .hasSizeGreaterThan(0);

        // Train from existing SSTables
        manager.train(true, Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE,
                                   TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE));

        // Training should complete quickly since we're reading from existing SSTables
        spinUntilTrue(() -> TrainingState.fromCompositeData(manager.getTrainingState()).status == TrainingStatus.COMPLETED, 10);

        // Verify dictionary was trained and is available
        spinUntilTrue(() -> manager.getCurrent() != null, 2);

        CompressionDictionary currentDict = manager.getCurrent();
        assertThat(currentDict).isNotNull();
        assertThat(currentDict.kind())
        .as("Dictionary should be ZSTD type")
        .isEqualTo(Kind.ZSTD);

        assertThat(currentDict.rawDictionary().length)
        .as("Dictionary should have content")
        .isGreaterThan(0);

        // Verify we can still read the data
        assertRows(execute("SELECT pk, data FROM %s WHERE pk = ?", "key0"),
                   row("key0", REPEATED_DATA + " row 0"));
    }

    @Test
    public void testSSTableBasedTrainingWithoutSSTables()
    {
        String table = createTable(getTableCql());
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Try to train without any SSTables
        assertThatThrownBy(() -> manager.train(false, Map.of(TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME, TRAINING_MAX_DICTIONARY_SIZE,
                                                             TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME, TRAINING_MAX_TOTAL_SAMPLE_SIZE)))
        .as("Should fail when no SSTables are available")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No SSTables available for training");
    }

    private String getTableCqlWithChunkLength()
    {
        return "CREATE TABLE %s (pk text PRIMARY KEY, data text) " +
               "WITH compression = {" +
               "'class': 'ZstdDictionaryCompressor'," +
               "'chunk_length_in_kb' : 4, " +
               '\'' + TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME + "': '" + DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE + "'," +
               '\'' + TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME + "': '" + DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE + '\'' +
               '}';
    }

    private String getTableCql()
    {
        return "CREATE TABLE %s (pk text PRIMARY KEY, data text) " +
               "WITH compression = {" +
               "'class': 'ZstdDictionaryCompressor'," +
               '\'' + TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_NAME + "': '" + DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE + "'," +
               '\'' + TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_NAME + "': '" + DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE + '\'' +
               '}';
    }
}
