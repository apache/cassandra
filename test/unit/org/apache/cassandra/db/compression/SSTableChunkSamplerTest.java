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

import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compression.SSTableChunkSampler.SSTableChunkInfo;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SSTableChunkSamplerTest extends CQLTester
{
    @Test
    public void testSSTableChunkInfoForCompressedSSTable()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '64'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Insert data and flush to create an SSTable
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "test data " + i);
        }
        flush();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).isNotEmpty();

        SSTableReader sstable = sstables.iterator().next();
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .chunkSize(64 * 1024)
                                                                                        .build();

        SSTableChunkInfo info = new SSTableChunkInfo(sstable, config);

        assertThat(info.isCompressed).isTrue();
        assertThat(info.chunkCount).isGreaterThan(0);
        assertThat(info.dataLength).isGreaterThan(0);
        assertThat(info.chunkSize).isEqualTo(64 * 1024);
        assertThat(info.metadata).isNotNull();
    }

    @Test
    public void testSSTableChunkInfoForUncompressedSSTable()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Insert data and flush to create an uncompressed SSTable
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "test data " + i);
        }
        flush();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).isNotEmpty();

        SSTableReader sstable = sstables.iterator().next();
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .chunkSize(64 * 1024)
                                                                                        .build();

        SSTableChunkInfo info = new SSTableChunkInfo(sstable, config);

        assertThat(info.isCompressed).isFalse();
        assertThat(info.chunkCount).isGreaterThan(0);
        assertThat(info.dataLength).isGreaterThan(0);
        assertThat(info.chunkSize).isEqualTo(64 * 1024);
        assertThat(info.metadata).isNull();
    }

    @Test
    public void testCalculateTargetChunkCount()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Create multiple SSTables
        for (int batch = 0; batch < 3; batch++)
        {
            for (int i = 0; i < 100; i++)
            {
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", batch * 100 + i, "test data " + i);
            }
            flush();
        }

        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .maxTotalSampleSize(10 * 1024 * 1024) // 10MB
                                                                                        .chunkSize(64 * 1024)
                                                                                        .build();

        try (ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            assertThat(refViewFragment.sstables).hasSizeGreaterThanOrEqualTo(3);
            List<SSTableChunkInfo> sstableInfos = SSTableChunkSampler.buildSSTableInfos(refViewFragment.sstables, config);
            long totalChunks = sstableInfos.stream().mapToLong(info -> info.chunkCount).sum();
            long targetChunkCount = SSTableChunkSampler.calculateTargetChunkCount(sstableInfos, totalChunks, config);

            // Target should be based on maxTotalSampleSize divided by average chunk size
            assertThat(targetChunkCount).isGreaterThan(0);
            long totalDataSize = sstableInfos.stream().mapToLong(info -> info.dataLength).sum();
            int averageChunkSize = (int) (totalDataSize / totalChunks);
            long expectedTarget = config.maxTotalSampleSize / averageChunkSize;
            assertThat(targetChunkCount).isEqualTo(expectedTarget);
        }
    }

    /**
     * Regression: with many SSTables and a large chunk size, the per-SSTable proportional allocation
     * {@code (targetChunkCount * chunkCount) / totalChunks} truncates to zero (integer division), so every
     * SSTable below the rounding threshold was skipped and almost no samples were collected — failing training
     * despite abundant data. The sampler must now contribute at least one chunk per SSTable, up to the target.
     */
    @Test
    public void testManySSTablesWithLargeChunksAreNotStarvedByRounding() throws Exception
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = " +
                                   "{'class': 'LZ4Compressor', 'chunk_length_in_kb': '64'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        cfs.disableAutoCompaction(); // keep each flush as its own SSTable so the many-SSTable scenario forms

        // Create many SSTables (no compaction), each large enough for several full 64 KiB chunks.
        int sstableCount = 40;
        for (int s = 0; s < sstableCount; s++)
        {
            for (int i = 0; i < 128; i++)
            {
                int n = s * 128 + i;
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", n, ("payload-" + n + "-").repeat(160)); // ~2 KiB/row
            }
            flush();
        }

        // Budget covers >= the trainer's minimum samples (so training can succeed) but is far below the SSTable
        // count, so the buggy proportional share (target * chunkCount / totalChunks) rounds to zero everywhere.
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .maxTotalSampleSize(768 * 1024) // 768 KiB
                                                                                        .chunkSize(64 * 1024)
                                                                                        .build();

        try (ColumnFamilyStore.RefViewFragment ref = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            List<SSTableChunkInfo> infos = SSTableChunkSampler.buildSSTableInfos(ref.sstables, config);
            long totalChunks = infos.stream().mapToLong(i -> i.chunkCount).sum();
            long target = SSTableChunkSampler.calculateTargetChunkCount(infos, totalChunks, config);

            // Precondition that triggers the rounding bug: many more SSTables than the target chunk count.
            assertThat((long) infos.size()).isGreaterThan(target);

            // Train for real: a fresh trainer starts out SAMPLING and collects the sampled chunks.
            ICompressionDictionaryTrainer trainer = new ZstdDictionaryTrainer(keyspace(), table, 3);
            trainer.start(config);
            SSTableChunkSampler.sampleFromSSTables(ref.sstables, trainer, config);

            // With the fix every SSTable contributes >= 1 chunk, so sampling only stops once the byte budget is
            // spent.
            long chunksToExhaustBudget = config.maxTotalSampleSize / (64 * 1024);
            assertThat(trainer.getTrainingState().sampleCount)
            .describedAs("every SSTable must contribute a chunk rather than its share rounding to zero")
            .isGreaterThanOrEqualTo(chunksToExhaustBudget);

            assertThat(trainer.trainDictionary(true)).isNotNull();
        }
    }

    @Test
    public void testSelectRandomChunkIndices()
    {
        // test scenarios: select small portion, large portion and all
        for (int expectedChunkCount : List.of(10, 80, 100))
        {
            Set<Long> selected = SSTableChunkSampler.selectRandomChunkIndices(100, expectedChunkCount);

            assertThat(selected).hasSize(expectedChunkCount);
            assertThat(selected).allMatch(idx -> idx >= 0 && idx < 100);
        }
    }

    @Test
    public void testSelectRandomChunkIndicesDistribution()
    {
        // Test that selection is reasonably distributed
        int totalChunks = 100;
        int runs = 1000;
        int[] hitCount = new int[totalChunks];

        // Run many selections and count how often each chunk is selected
        for (int i = 0; i < runs; i++)
        {
            Set<Long> selected = SSTableChunkSampler.selectRandomChunkIndices(totalChunks, 10);
            for (long idx : selected)
            {
                hitCount[(int) idx]++;
            }
        }

        // Each chunk should be selected approximately 10% of the time (10 out of 100)
        // So in 1000 runs, expect ~100 hits per chunk
        // Allow for variance - between 50 and 150 hits
        for (int count : hitCount)
        {
            assertThat(count).isBetween(50, 150);
        }
    }

    @Test
    public void testSampleFromSSTablesWithTrainerNotReady()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'class': 'LZ4Compressor'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Insert data and flush to create an SSTable
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "test data " + i);
        }
        flush();

        try (ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            assertThat(refViewFragment.sstables).isNotEmpty();

            CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                            .chunkSize(64 * 1024)
                                                                                            .build();

            // Create a mock trainer that is not ready to sample
            ICompressionDictionaryTrainer trainer = mock(ICompressionDictionaryTrainer.class, RETURNS_DEEP_STUBS);
            when(trainer.getTrainingState().getStatus()).thenReturn(ICompressionDictionaryTrainer.TrainingStatus.NOT_STARTED);

            // Should throw IllegalStateException when trainer is not ready
            assertThatThrownBy(() -> SSTableChunkSampler.sampleFromSSTables(refViewFragment.sstables, trainer, config))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Trainer is not ready to accept samples");
        }
    }

    @Test
    public void testReadChunkThrowsOnInvalidPosition()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Insert data and flush to create an uncompressed SSTable
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "test data " + i);
        }
        flush();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).isNotEmpty();

        SSTableReader sstable = sstables.iterator().next();
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .chunkSize(64 * 1024)
                                                                                        .build();

        SSTableChunkInfo info = new SSTableChunkInfo(sstable, config);

        // Try to read at a position beyond the data length - should throw IOException
        long invalidPosition = info.dataLength + 1000;
        assertThatThrownBy(() -> SSTableChunkSampler.readUncompressedChunk(info, invalidPosition))
        .isInstanceOf(java.io.IOException.class)
        .hasMessageContaining("Invalid read size");
    }
}
