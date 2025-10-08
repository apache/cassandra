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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.CompressionDictionary.DictId;
import org.apache.cassandra.db.compression.CompressionDictionary.Kind;
import org.apache.cassandra.db.compression.ICompressionDictionaryTrainer.TrainingStatus;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CompressionDictionaryIntegrationTest extends CQLTester
{
    private static final String REPEATED_DATA = "The quick brown fox jumps over the lazy dog. This text repeats for better compression. ";

    @Before
    public void configureDatabaseDescriptor()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_dictionary_training_sampling_rate = 1.0f;
        config.compression_dictionary_training_max_total_sample_size = new DataStorageSpec.IntKibibytesBound("128KiB");
        config.compression_dictionary_training_max_dictionary_size = new DataStorageSpec.IntKibibytesBound("10KiB");
        // Ensures that data are still sampled when using the LZ4 (which is picked up when using 'fast')
        // on the SSTable flushing code path
        config.flush_compression = Config.FlushCompression.fast;
        DatabaseDescriptor.setConfig(config);
    }

    @Test
    public void testEndToEndDictionaryTraining()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Verify initial state
        assertThat(manager.getTrainingStatus())
        .as("Initial training status should be NOT_STARTED or SAMPLING")
        .isEqualTo(TrainingStatus.NOT_STARTED.toString());

        // Trigger manual training
        manager.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "2"));

        // Add sample data that benefits from dictionary compression
        int i = 0;
        while (!manager.isReady())
        {
            ByteBuffer sample = ByteBuffer.wrap((REPEATED_DATA + " variation " + i++).getBytes());
            manager.addSample(sample);
        }

        assertThat(manager.isReady())
        .as("Trainer should be ready to train")
        .isTrue();

        // Training should complete
        spinUntilTrue(() -> manager.getTrainingStatus().equals(TrainingStatus.COMPLETED.toString()), 2);

        // Verify dictionary is available
        // There could be a slight delay, as the dictionary has to be peristed to system table first.
        spinUntilTrue(() -> manager.getCurrent() != null, 2);

        CompressionDictionary currentDict = manager.getCurrent();

        assertThat(currentDict.kind())
        .as("Dictionary should be ZSTD type")
        .isEqualTo(Kind.ZSTD);

        assertThat(currentDict.rawDictionary().length)
        .as("Dictionary should have content")
        .isGreaterThan(0);
    }

    @Test
    public void testEnableDisableDictionaryCompression()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        assertThatNoException()
        .as("Should allow manual training")
        .isThrownBy(() -> manager.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600")));

        // Disable dictionary compression
        CompressionParams nonDictParams = CompressionParams.lz4();
        manager.maybeReloadFromSchema(nonDictParams);

        assertThatThrownBy(() -> manager.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600")))
        .as("Should disallow manual training when using lz4")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not support dictionary compression");

        // Re-enable dictionary compression
        CompressionParams dictParams = CompressionParams.zstd(CompressionParams.DEFAULT_CHUNK_LENGTH, true,
                                                              Collections.singletonMap("compression_level", "3"));
        manager.maybeReloadFromSchema(dictParams);

        assertThatNoException()
        .as("Should allow manual training after switching back to dictionary compression")
        .isThrownBy(() -> manager.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "600")));
    }

    @Test
    public void testCompressionParameterChanges()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
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
    public void testSSTableCompressionWithDictionary()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        String table = createTable("CREATE TABLE %s (pk text PRIMARY KEY, data text) " +
                                   // use 4 KiB, so it collects enough samples. Trainer requires at least 10 samples
                                   "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb' : 4}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        manager.train(Map.of(ManualTrainingOptions.MAX_SAMPLING_DURATION_SECONDS_KEY, "5"));

        // Insert compressible data to train dictionary
        int i = 0;
        while (!manager.isReady())
        {
            int index = i++;
            execute("INSERT INTO %s (pk, data) VALUES (?, ?)",
                    "key" + index,
                    REPEATED_DATA + " row " + index);
            if (i % 200 == 0)
                flush();
        }
        flush();

        // training should finish in 3 seconds and have the dictionary available
        spinUntilTrue(() -> manager.getCurrent() != null, 3);

        // Insert compressible data to be compressed by dictionary
        for (int j = i; j < i + 500; j++)
        {
            execute("INSERT INTO %s (pk, data) VALUES (?, ?)",
                    "key" + j,
                    REPEATED_DATA + " row " + j);
        }

        // Verify SSTable was created with compression
        assertThat(cfs.getLiveSSTables())
        .as("Should have created SSTables")
        .hasSizeGreaterThan(1);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertThat(sstable.compression)
        .as("SSTable should have compression parameters")
        .isNotNull();

        // Verify data can be read back correctly
        // - Can read data from the sstable w/o dictionary
        assertRows(execute("SELECT pk, data FROM %s WHERE pk = ?", "key0"),
                   row("key0", REPEATED_DATA + " row 0"));
        // - Can read data from the sstable w/ dictionary
        int rowInDictSSTable = i + 100;
        assertRows(execute("SELECT pk, data FROM %s WHERE pk = ?", "key" + rowInDictSSTable),
                   row("key" + rowInDictSSTable, REPEATED_DATA + " row " + rowInDictSSTable));
    }

    @Test
    public void testResourceCleanupOnClose() throws Exception
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        // Add test dictionary
        ZstdCompressionDictionary testDict = createTestDictionary();
        manager.add(testDict);
        manager.setCurrentIfNewer(testDict);

        assertThat(testDict.selfRef().globalCount())
        .as("Dictionary's reference count should be 1 after adding to cache")
        .isOne();

        assertThat(manager.getCurrent())
        .as("Should have current dictionary before close")
        .isNotNull();

        manager.close();

        assertThat(manager.trainer()).isNull();
        assertThat(testDict.selfRef().globalCount())
        .as("Dictionary's reference count should be 0 after closing manager")
        .isZero();
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
}
