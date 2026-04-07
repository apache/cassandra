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
import java.util.concurrent.RejectedExecutionException;
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
 * ALTER TABLE must be able to turn compression dictionary auto-training on and off without a node restart.
 */
public class CompressionDictionaryAutoTrainingAlterTest extends CQLTester
{
    private static final String DICT_AUTO = "{'class':'ZstdDictionaryCompressor','auto_training_enabled':'true'}";
    private static final String DICT_PLAIN = "{'class':'ZstdDictionaryCompressor'}";
    private static final String TWCS = "{'class':'TimeWindowCompactionStrategy'}";

    @Test
    public void alterEnablesAndDisablesAutoTraining()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_PLAIN + " AND compaction = " + TWCS);
        assertAutoTrainingEnabled(false);

        alterTable("ALTER TABLE %s WITH compression = " + DICT_AUTO);
        assertAutoTrainingEnabled(true);

        alterTable("ALTER TABLE %s WITH compression = " + DICT_PLAIN);
        assertAutoTrainingEnabled(false);
    }

    @Test
    public void autoTrainingEnabledAtCreateIsVisible()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        assertAutoTrainingEnabled(true);
    }

    /**
     * Disabling dictionary compression tears the scheduler down; re-enabling must leave the table able to train
     * again. A scheduler that owned and shut down a training executor here would reject every later submit,
     * permanently disabling training for the table.
     */
    @Test
    public void disableThenReEnableLeavesTrainingUsable() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        assertAutoTrainingEnabled(true);

        execute("INSERT INTO %s (id, v) VALUES (?, ?)", 1, "v1");
        flush();

        alterTable("ALTER TABLE %s WITH compression = {'class':'LZ4Compressor'}");
        assertThat(manager().isEnabled())
        .describedAs("dictionary compression disabled")
        .isFalse();

        alterTable("ALTER TABLE %s WITH compression = " + DICT_AUTO);
        assertThat(manager().isEnabled())
        .describedAs("dictionary compression re-enabled")
        .isTrue();
        assertAutoTrainingEnabled(true);
        assertThat(manager().isTrainingRunning()).isFalse();

        // Training may still fail for legitimate reasons here (too few samples, for instance); what must not
        // happen is the submit being rejected because the executor was torn down by the disable.
        try
        {
            manager().train(false, Map.of());
        }
        catch (Throwable t)
        {
            assertThat(t)
            .describedAs("training must not be rejected by a shut-down executor after a disable/enable cycle")
            .isNotInstanceOf(RejectedExecutionException.class);
            assertThat(t.getCause())
            .describedAs("training must not be rejected by a shut-down executor after a disable/enable cycle")
            .isNotInstanceOf(RejectedExecutionException.class);
        }
    }

    /**
     * Turning auto-training off with ALTER must exclude the table from the check cycle: checkTable() reads the
     * flag from the live schema and returns before scheduling any training.
     */
    @Test
    public void alterDisablingAutoTrainingExcludesTableFromChecks()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) " +
                    "WITH compression = " + DICT_AUTO + " AND compaction = " + TWCS);
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        AtomicInteger scheduled = new AtomicInteger();
        // Everything checkTable() consults after the flag is stubbed out, so the flag is the only thing that can
        // stop it: a non-null latest dictionary to compare against, and a fragment to train on.
        CompressionDictionaryAutoTrainingManager autoTrainer = new CompressionDictionaryAutoTrainingManager()
        {
            @Override
            ColumnFamilyStore.RefViewFragment resolveViewFragment(ColumnFamilyStore c)
            {
                return new ColumnFamilyStore.RefViewFragment(Collections.emptyList(),
                                                             Collections.emptyList(),
                                                             Refs.tryRef(Collections.<SSTableReader>emptyList()));
            }

            @Override
            CompressionDictionary retrieveLatestDictionary(ColumnFamilyStore c)
            {
                CompressionDictionary latest = mock(CompressionDictionary.class);
                when(latest.kind()).thenReturn(CompressionDictionary.Kind.ZSTD);
                return latest;
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
        assertThat(scheduled.get()).describedAs("auto training enabled, table is trained").isEqualTo(1);

        alterTable("ALTER TABLE %s WITH compression = " + DICT_PLAIN);
        assertAutoTrainingEnabled(false);

        scheduled.set(0);
        autoTrainer.checkTable(cfs);
        assertThat(scheduled.get()).describedAs("auto training disabled, table is excluded").isZero();
    }

    private CompressionDictionaryManager manager()
    {
        return getCurrentColumnFamilyStore().compressionDictionaryManager();
    }

    /**
     * The MBean getter and the training config must not disagree; they read the same option.
     */
    private void assertAutoTrainingEnabled(boolean expected)
    {
        CompressionDictionaryManager manager = manager();

        assertThat(manager.isAutoTrainingEnabled())
        .describedAs("CompressionDictionaryManager.isAutoTrainingEnabled()")
        .isEqualTo(expected);

        assertThat(manager.createTrainingConfig(Map.of()).autoTrainingEnabled)
        .describedAs("CompressionDictionaryTrainingConfig.autoTrainingEnabled")
        .isEqualTo(expected);
    }
}
