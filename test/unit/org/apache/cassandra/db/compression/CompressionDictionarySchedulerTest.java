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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.schema.SystemDistributedKeyspace;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE;
import static org.apache.cassandra.io.compress.IDictionaryCompressor.DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE;
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
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        scheduler = new CompressionDictionaryScheduler(KEYSPACE, table, cfs.metadata.id.toLongString(), cache, true);

        try (CompressionDictionaryManager manager = cfs.compressionDictionaryManager())
        {
            ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.select(SSTableSet.CANONICAL, (x) -> false));
            CompressionDictionaryTrainingConfig config = createSampleAllTrainingConfig(cfs);

            // Should not throw, but task will complete quickly with no SSTables
            scheduler.scheduleSSTableBasedTraining(refViewFragment,
                                                   cfs.metadata.get().params.compression,
                                                   config,
                                                   dictionary -> manager.handleNewDictionary(dictionary, true),
                                                   true);
            spinUntilTrue(() -> !scheduler.isTrainingRunning());
            assertThat(manager.getCurrent()).isNull();
        }
    }

    @Test
    public void testScheduleSSTableBasedTrainingWithSSTables()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                                   "WITH compression = {'class': 'ZstdDictionaryCompressor', 'chunk_length_in_kb': '4'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);
        scheduler = new CompressionDictionaryScheduler(KEYSPACE, table, cfs.metadata.id.toLongString(), cache, true);

        cfs.disableAutoCompaction();
        try (CompressionDictionaryManager manager = cfs.compressionDictionaryManager())
        {
            createSSTables();

            ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
            assertThat(refViewFragment.sstables).isNotEmpty();

            CompressionDictionaryTrainingConfig config = createSampleAllTrainingConfig(cfs);

            assertThat(manager.getCurrent()).as("There should be no dictionary at this step").isNull();
            scheduler.scheduleSSTableBasedTraining(refViewFragment,
                                                   cfs.metadata.get().params.compression,
                                                   config,
                                                   dictionary -> manager.handleNewDictionary(dictionary, true),
                                                   true);

            // Task should be scheduled
            assertThat(scheduler.isTrainingRunning()).isTrue();
            // A dictionary should be trained
            spinUntilTrue(() -> manager.getCurrent() != null);
        }
    }

    @Test
    public void testRefreshDoesNotRevertStoredDictionary() throws Exception
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                                   "WITH compression = {'class': 'ZstdDictionaryCompressor'}");
        String tableId = Keyspace.open(keyspace()).getColumnFamilyStore(table).metadata.id.toLongString();
        CompressionDictionary older = new ZstdCompressionDictionary(new CompressionDictionary.DictId(CompressionDictionary.Kind.ZSTD, 1), "older".getBytes());
        CompressionDictionary newer = new ZstdCompressionDictionary(new CompressionDictionary.DictId(CompressionDictionary.Kind.ZSTD, 2), "newer".getBytes());
        SystemDistributedKeyspace.storeCompressionDictionary(keyspace(), table, tableId, older);

        CountDownLatch read = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CompressionDictionaryCache pausingCache = new CompressionDictionaryCache()
        {
            @Override
            public CompressionDictionary getCurrent()
            {
                // called by a refresh between reading the stored dictionaries and setting the current one
                read.countDown();
                Uninterruptibles.awaitUninterruptibly(resume);
                return super.getCurrent();
            }
        };
        scheduler = new CompressionDictionaryScheduler(keyspace(), table, tableId, pausingCache, true);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try
        {
            Future<?> refresh = executor.submit(scheduler::refreshCurrentDictionary);
            spinUntilTrue(() -> read.getCount() == 0 || refresh.isDone());
            SystemDistributedKeyspace.storeCompressionDictionary(keyspace(), table, tableId, newer);
            AtomicReference<Thread> adding = new AtomicReference<>();
            Future<?> add = executor.submit(() -> {
                adding.set(Thread.currentThread());
                scheduler.addStoredDictionary(newer);
            });
            spinUntilTrue(() -> add.isDone() || (adding.get() != null && adding.get().getState() == Thread.State.BLOCKED));
            resume.countDown();
            refresh.get();
            add.get();

            assertThat(pausingCache.getCurrent().dictId()).isEqualTo(newer.dictId());
        }
        finally
        {
            resume.countDown();
            executor.shutdownNow();
            pausingCache.close();
        }
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
               .maxDictionarySize(new DataStorageSpec.IntKibibytesBound(DEFAULT_TRAINING_MAX_DICTIONARY_SIZE_PARAMETER_VALUE).toBytes())
               .maxTotalSampleSize(new DataStorageSpec.IntKibibytesBound(DEFAULT_TRAINING_MAX_TOTAL_SAMPLE_SIZE_PARAMETER_VALUE).toBytes())
               .chunkSize(cfs.metadata().params.compression.chunkLength())
               .build();
    }
}
