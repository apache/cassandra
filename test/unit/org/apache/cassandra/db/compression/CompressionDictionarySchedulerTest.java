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

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.assertj.core.api.Assertions.assertThat;

public class CompressionDictionarySchedulerTest extends CQLTester
{
    private CompressionDictionaryScheduler scheduler;
    private ICompressionDictionaryCache cache;

    @Before
    public void setUp()
    {
        cache = new CompressionDictionaryCache();
    }

    @After
    public void tearDown()
    {
        if (scheduler != null)
        {
            scheduler.close();
        }
    }

    @Test
    public void testScheduleSSTableBasedTrainingWithNoSSTables()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                                   "WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        scheduler = new CompressionDictionaryScheduler(KEYSPACE, table, cache, true);

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        Set<SSTableReader> sstables = new HashSet<>();
        CompressionDictionaryTrainingConfig config = createSampleAllTrainingConfig(cfs);

        // Should not throw, but task will complete quickly with no SSTables
        scheduler.scheduleSSTableBasedTraining(manager.trainer(), sstables, config, true);
        spinUntilTrue(() -> !scheduler.isManualTrainingRunning());
        assertThat(manager.getCurrent()).isNull();
    }

    @Test
    public void testScheduleSSTableBasedTrainingWithSSTables()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                                   "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}");
        scheduler = new CompressionDictionaryScheduler(KEYSPACE, table, cache, true);

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        CompressionDictionaryManager manager = cfs.compressionDictionaryManager();

        createSSTables();

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertThat(sstables).isNotEmpty();

        CompressionDictionaryTrainingConfig config = createSampleAllTrainingConfig(cfs);
        manager.trainer().start(true);

        assertThat(manager.getCurrent()).as("There should be no dictionary at this step").isNull();
        scheduler.scheduleSSTableBasedTraining(manager.trainer(), sstables, config, true);

        // Task should be scheduled
        assertThat(scheduler.isManualTrainingRunning()).isTrue();
        // A dictionary should be trained
        spinUntilTrue(() -> manager.getCurrent() != null);
    }

    private void createSSTables()
    {
        for (int file = 0; file < 10; file++)
        {
            int batchSize = 1000;
            for (int i = 0; i < batchSize; i++)
            {
                int index = i + file * batchSize;
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", index, "test data " + index);
            }
            flush();
        }
    }

    private static CompressionDictionaryTrainingConfig createSampleAllTrainingConfig(ColumnFamilyStore cfs) {
        return CompressionDictionaryTrainingConfig
               .builder()
               .maxDictionarySize(DatabaseDescriptor.getCompressionDictionaryTrainingMaxDictionarySize())
               .maxTotalSampleSize(DatabaseDescriptor.getCompressionDictionaryTrainingMaxTotalSampleSize())
               .samplingRate(1.0f)
               .chunkSize(cfs.metadata().params.compression.chunkLength())
               .build();
    }
}
