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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The adoption decision compares a candidate against the latest stored dictionary by running both through a
 * trainer built from the table's current compression params. That is only meaningful if the current compressor
 * can consume the latest dictionary: the ratio computation feeds it raw bytes, and it accepts arbitrary bytes
 * as a raw content dictionary, so a dictionary of a foreign kind yields a meaningless baseline rather than an
 * error. Since the latest dictionary is the newest dict_id for the table regardless of kind, a table whose
 * dictionary compressor kind changed can see one. Such a round must be skipped.
 */
public class CompressionDictionaryAutoTrainingDictionaryKindTest extends CQLTester
{
    private static final String DICT_AUTO = "{'class':'ZstdDictionaryCompressor','auto_training_enabled':'true'}";
    private static final String TWCS = "{'class':'TimeWindowCompactionStrategy'}";

    @Test
    public void zstdCompressorConsumesZstdDictionary()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);

        assertThat(CompressionDictionaryAutoTrainingManager.canConsume(getCurrentColumnFamilyStore(),
                                                                       dictionaryOfKind(CompressionDictionary.Kind.ZSTD)))
        .isTrue();
    }

    @Test
    public void nonDictionaryCompressorConsumesNothing()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = {'class':'LZ4Compressor'} AND compaction = " + TWCS);

        assertThat(CompressionDictionaryAutoTrainingManager.canConsume(getCurrentColumnFamilyStore(),
                                                                       dictionaryOfKind(CompressionDictionary.Kind.ZSTD)))
        .isFalse();
    }

    @Test
    public void unconsumableLatestDictionarySkipsRound()
    {
        assertThat(trainedWithLatest(unconsumableDictionary()))
        .describedAs("latest dictionary of a kind the compressor cannot consume, nothing comparable to evaluate")
        .isZero();
    }

    @Test
    public void consumableLatestDictionaryProceeds()
    {
        assertThat(trainedWithLatest(dictionaryOfKind(CompressionDictionary.Kind.ZSTD)))
        .describedAs("latest dictionary the compressor can consume, round proceeds")
        .isEqualTo(1);
    }

    /**
     * Runs a check cycle over a real dictionary-compressed table with {@code latest} as the stored dictionary,
     * and returns how many times training was scheduled. Everything checkTable() consults after the kind guard is
     * stubbed out, so the guard is the only thing that can stop it.
     */
    private int trainedWithLatest(CompressionDictionary latest)
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        AtomicInteger scheduled = new AtomicInteger();
        CompressionDictionaryAutoTrainingManager autoTrainer = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            CompressionDictionary retrieveLatestDictionary(ColumnFamilyStore c)
            {
                return latest;
            }

            @Override
            ColumnFamilyStore.RefViewFragment resolveViewFragment(ColumnFamilyStore c)
            {
                return new ColumnFamilyStore.RefViewFragment(Collections.emptyList(),
                                                             Collections.emptyList(),
                                                             Refs.tryRef(Collections.<SSTableReader>emptyList()));
            }

            @Override
            CompressionDictionary scheduleTraining(ColumnFamilyStore c,
                                                   ColumnFamilyStore.RefViewFragment refViewFragment,
                                                   CompressionDictionaryManager m)
            {
                scheduled.incrementAndGet();
                refViewFragment.close();
                return null;
            }
        };

        autoTrainer.checkTable(cfs);
        return scheduled.get();
    }

    private static CompressionDictionary dictionaryOfKind(CompressionDictionary.Kind kind)
    {
        CompressionDictionary dictionary = mock(CompressionDictionary.class);
        when(dictionary.kind()).thenReturn(kind);
        return dictionary;
    }

    /**
     * Kind has a single constant today, so a dictionary of a foreign kind cannot be built; a dictionary reporting
     * no kind stands in for one, since it drives canConsumeDictionary() down the same rejecting branch.
     */
    private static CompressionDictionary unconsumableDictionary()
    {
        return dictionaryOfKind(null);
    }
}
