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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the manual-vs-automatic training mutual exclusion: a user's training always has priority, so if a
 * training is already in progress when the auto-training cycle reaches a table it must back off and not train.
 * <p>
 * This uses a real dictionary-compressed table (so {@code checkTable()} runs its real logic) but simulates the
 * "a training is already running" state rather than running a real training.
 */
public class CompressionDictionaryAutoTrainingConcurrencyTest extends CQLTester
{
    @Test
    public void autoTrainingBacksOffWhenATrainingIsAlreadyInProgress()
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) WITH compression = " +
                                   "{'class':'ZstdDictionaryCompressor', 'auto_training_enabled':'true'} " +
                                   "AND compaction = {'class':'TimeWindowCompactionStrategy'}");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(table);

        // Stand-in for the table's manager whose training state we drive; it plays the role of a manager on
        // which a user has (or has not) invoked training.
        CompressionDictionaryManager manager = mock(CompressionDictionaryManager.class);
        when(manager.isEnabled()).thenReturn(true);
        when(manager.isAutoTrainingEnabled()).thenReturn(true);

        // A non-null "latest" dictionary so that, absent a back-off, checkTable() would proceed to training
        // (i.e. the only thing that can stop it here is the in-progress-training guard).
        CompressionDictionary latest = mock(CompressionDictionary.class);
        when(latest.kind()).thenReturn(CompressionDictionary.Kind.ZSTD);
        // The recency-bias resolver currently returns null (auto-training paused), so inject a non-null view
        // fragment here; that keeps the in-progress-training guard as the only thing that can stop checkTable().
        // A real fragment rather than a mock: checkTable() re-references its SSTables, and a Mockito mock
        // bypasses the constructor and so leaves the final sstables field null, which cannot happen in production.
        ColumnFamilyStore.RefViewFragment refViewFragment =
            new ColumnFamilyStore.RefViewFragment(Collections.emptyList(),
                                                  Collections.emptyList(),
                                                  Refs.tryRef(Collections.<SSTableReader>emptyList()));

        AtomicInteger scheduleTrainingCalls = new AtomicInteger();
        CompressionDictionaryAutoTrainingManager autoTrainer = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            ColumnFamilyStore.RefViewFragment resolveViewFragment(ColumnFamilyStore c)
            {
                return refViewFragment;
            }

            @Override
            CompressionDictionaryManager getCompressionDictionaryManager(ColumnFamilyStore c)
            {
                return manager;
            }

            @Override
            CompressionDictionary retrieveLatestDictionary(ColumnFamilyStore c)
            {
                return latest;
            }

            @Override
            CompressionDictionary scheduleTraining(ColumnFamilyStore c,
                                                   ColumnFamilyStore.RefViewFragment fragment,
                                                   CompressionDictionaryManager m)
            {
                scheduleTrainingCalls.incrementAndGet();
                return null;
            }
        };

        try (autoTrainer)
        {
            // control: no training in progress -> auto-training proceeds all the way to scheduling a training
            when(manager.isTrainingRunning()).thenReturn(false);
            autoTrainer.checkTable(cfs);
            assertThat(scheduleTrainingCalls.get())
            .as("with no training in progress, auto-training should proceed to training")
            .isEqualTo(1);

            // the assertion under test: a training is already running (user has priority) -> auto-training backs off
            scheduleTrainingCalls.set(0);
            when(manager.isTrainingRunning()).thenReturn(true);
            autoTrainer.checkTable(cfs);
            assertThat(scheduleTrainingCalls.get())
            .as("a training is already in progress -> auto-training must back off and NOT train")
            .isZero();
        }
    }
}
