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

package org.apache.cassandra.db.virtual;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compression.CompressionDictionary;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingHistory;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that auto-training decisions recorded by {@link CompressionDictionaryAutoTrainingHistory} are visible
 * through the virtual table, newest first.
 */
public class CompressionDictionaryAutoTrainingTableTest extends CQLTester
{
    private static final String KS = "vts_auto_training";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        VirtualKeyspaceRegistry.instance.register(
            new VirtualKeyspace(KS, ImmutableList.of(new CompressionDictionaryAutoTrainingTable(KS))));
    }

    @Before
    public void clearHistory()
    {
        CompressionDictionaryAutoTrainingHistory.instance.clear();
    }

    @Test
    public void emptyHistoryYieldsNoRows() throws Throwable
    {
        assertEmpty(execute(select()));
    }

    @Test
    public void decisionIsVisibleWithItsRatiosThresholdAndOutcome() throws Throwable
    {
        CompressionDictionaryAutoTrainingHistory.instance.record(1000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.30, 0.40, 0.15, true);

        UntypedResultSet result = execute(select());
        assertThat(result.size()).isEqualTo(1);

        UntypedResultSet.Row row = result.one();
        assertThat(row.getString("keyspace_name")).isEqualTo("ks1");
        assertThat(row.getString("table_name")).isEqualTo("tbl1");
        assertThat(row.getTimestamp("trained_at").getTime()).isEqualTo(1000L);
        assertThat(row.getInetAddress("node")).isEqualTo(FBUtilities.getBroadcastAddressAndPort().getAddress());
        assertThat(row.getString("kind")).isEqualTo(CompressionDictionary.Kind.ZSTD.name());
        assertThat(row.getDouble("baseline_ratio")).isEqualTo(0.50);
        assertThat(row.getDouble("candidate_ratio")).isEqualTo(0.30);
        assertThat(row.getDouble("improvement")).isEqualTo(0.40);
        assertThat(row.getDouble("threshold")).isEqualTo(0.15);
        assertThat(row.getBoolean("promoted")).isTrue();
    }

    @Test
    public void rejectedCandidateIsRecordedToo() throws Throwable
    {
        CompressionDictionaryAutoTrainingHistory.instance.record(1000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.49, 0.02, 0.15, false);

        UntypedResultSet.Row row = execute(select()).one();
        assertThat(row.getDouble("improvement")).isEqualTo(0.02);
        assertThat(row.getBoolean("promoted")).isFalse();
    }

    @Test
    public void decisionsForOneTableComeBackNewestFirst() throws Throwable
    {
        CompressionDictionaryAutoTrainingHistory.instance.record(1000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.49, 0.02, 0.15, false);
        CompressionDictionaryAutoTrainingHistory.instance.record(3000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.30, 0.40, 0.15, true);
        CompressionDictionaryAutoTrainingHistory.instance.record(2000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.45, 0.10, 0.15, false);

        List<Long> times = new ArrayList<>();
        for (UntypedResultSet.Row row : execute(select() + " WHERE keyspace_name = 'ks1' AND table_name = 'tbl1'"))
            times.add(row.getTimestamp("trained_at").getTime());

        assertThat(times).containsExactly(3000L, 2000L, 1000L);
    }

    @Test
    public void decisionsArePartitionedByKeyspace() throws Throwable
    {
        CompressionDictionaryAutoTrainingHistory.instance.record(1000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.50, 0.30, 0.40, 0.15, true);
        CompressionDictionaryAutoTrainingHistory.instance.record(1000L, "ks2", "tbl2", CompressionDictionary.Kind.ZSTD, 0.60, 0.20, 0.66, 0.15, true);

        assertThat(execute(select()).size()).isEqualTo(2);

        UntypedResultSet ks2 = execute(select() + " WHERE keyspace_name = 'ks2'");
        assertThat(ks2.size()).isEqualTo(1);
        assertThat(ks2.one().getString("table_name")).isEqualTo("tbl2");
    }

    @Test
    public void oldestEntryIsEvictedOnceTheBoundIsReached()
    {
        CompressionDictionaryAutoTrainingHistory history = new CompressionDictionaryAutoTrainingHistory(2);
        history.record(1000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.5, 0.4, 0.2, 0.15, true);
        history.record(2000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.5, 0.4, 0.2, 0.15, true);
        history.record(3000L, "ks1", "tbl1", CompressionDictionary.Kind.ZSTD, 0.5, 0.4, 0.2, 0.15, true);

        assertThat(history.entries()).hasSize(2);
        assertThat(history.entries().get(0).timestampMillis).isEqualTo(3000L);
        assertThat(history.entries().get(1).timestampMillis).isEqualTo(2000L);
    }

    private static String select()
    {
        return "SELECT * FROM " + KS + '.' + CompressionDictionaryAutoTrainingTable.TABLE_NAME;
    }
}
