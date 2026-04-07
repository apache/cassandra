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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Isolated test of {@link RecencyBiasViewFragmentResolver}: it only exercises which SSTables the resolver selects,
 * with no training involved. The resolver walks TWCS windows newest-first, adding each window whole, until it has
 * gathered {@code acceptableTotalSampleSize} uncompressed bytes or has reached back {@code auto_training_twcs_max_windows}
 * windows. It resolves to {@code null} (and the auto-trainer backs off) when the table is not on TWCS, has no
 * SSTables, or the windows within the cap do not hold enough data to reach the sample floor.
 */
public class CompressionDictionaryAutoTrainingRecencyBiasViewFragmentResolverTest extends CQLTester
{
    private static final long MICROS_PER_DAY = 86_400L * 1_000_000L;
    private static final String TWO_KIB_VALUE = "x".repeat(2048);
    private static final int ROWS_PER_WINDOW = 50;
    private static final long UNREACHABLE_TARGET = 100L << 20; // 100 MiB, far more than the ~400 KiB of test data

    @Test
    public void newestWindowAloneCanMeetTheTarget() throws Throwable
    {
        ColumnFamilyStore cfs = fourEqualDayWindows();
        long newest = maxTimestampOfDay(4);

        // a tiny target is satisfied by the newest window alone, and the default cap of 1 stops it right there
        try (ColumnFamilyStore.RefViewFragment fragment =
             new RecencyBiasViewFragmentResolver(cfs, config(8, 1)).resolveViewFragment())
        {
            assertThat(fragment).isNotNull();
            assertThat(fragment.sstables)
            .as("newest window already meets the target under a 1-window cap -> just the newest window")
            .hasSize(1)
            .allMatch(sstable -> sstable.getMaxTimestamp() == newest);
        }
    }

    @Test
    public void keepsAddingWholeWindowsUntilTheTargetIsMet() throws Throwable
    {
        ColumnFamilyStore cfs = fourEqualDayWindows();
        long[] cumulative = cumulativeUncompressedNewestFirst(cfs);

        // target that one window does not satisfy but two do (midpoint of the 1- and 2-window totals)
        CompressionDictionaryTrainingConfig config = configTargeting(midpoint(cumulative[0], cumulative[1]));
        assertThat((long) config.acceptableTotalSampleSize).isGreaterThan(cumulative[0]).isLessThanOrEqualTo(cumulative[1]);

        try (ColumnFamilyStore.RefViewFragment fragment = new RecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment())
        {
            assertThat(fragment.sstables)
            .as("one window short, two enough -> exactly the two newest windows")
            .hasSize(2)
            .allMatch(sstable -> sstable.getMaxTimestamp() >= 3 * MICROS_PER_DAY);
        }
    }

    @Test
    public void reachesBackAcrossThreeWindowsWhenNeeded() throws Throwable
    {
        ColumnFamilyStore cfs = fourEqualDayWindows();
        long[] cumulative = cumulativeUncompressedNewestFirst(cfs);

        // target that two windows do not satisfy but three do (midpoint of the 2- and 3-window totals)
        CompressionDictionaryTrainingConfig config = configTargeting(midpoint(cumulative[1], cumulative[2]));
        assertThat((long) config.acceptableTotalSampleSize).isGreaterThan(cumulative[1]).isLessThanOrEqualTo(cumulative[2]);

        try (ColumnFamilyStore.RefViewFragment fragment = new RecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment())
        {
            assertThat(fragment.sstables)
            .as("two windows short, three enough -> exactly the three newest windows")
            .hasSize(3)
            .allMatch(sstable -> sstable.getMaxTimestamp() >= 2 * MICROS_PER_DAY);
        }
    }

    @Test
    public void backsOffWhenTheWindowCapCannotReachEnoughData() throws Throwable
    {
        ColumnFamilyStore cfs = fourEqualDayWindows();
        long[] cumulative = cumulativeUncompressedNewestFirst(cfs);

        // this target needs three windows, but the cap allows only two -> not enough -> back off
        long needsThreeWindows = midpoint(cumulative[1], cumulative[2]);
        int maxTotalSampleSize = (int) Math.max(10, Math.ceil(needsThreeWindows * 10.0 / 8.0));
        CompressionDictionaryTrainingConfig config = CompressionDictionaryTrainingConfig.builder()
                                                                                        .maxTotalSampleSize(maxTotalSampleSize)
                                                                                        .autoTrainingTwcsMaxWindows(2)
                                                                                        .build();

        assertThat(new RecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment())
        .as("two windows do not hold enough and the cap forbids a third -> back off (null)")
        .isNull();
    }

    @Test
    public void backsOffWhenNoRecentWindowsHoldEnough() throws Throwable
    {
        ColumnFamilyStore cfs = fourEqualDayWindows();

        // even reaching back across all windows (generous cap) cannot meet an unreachable target -> back off
        assertThat(new RecencyBiasViewFragmentResolver(cfs, config(UNREACHABLE_TARGET, 10)).resolveViewFragment())
        .as("not enough data anywhere in reach -> back off (null)")
        .isNull();
    }

    @Test
    public void skipsEmptyWindowsWithoutSpendingTheCap() throws Throwable
    {
        // three windows with wide gaps between them (days 2-4 and 6-9 hold no SSTables)
        ColumnFamilyStore cfs = twcsDayWindowTable();
        writeDayWindow(1);
        writeDayWindow(5);
        writeDayWindow(10);
        assertThat(cfs.getLiveSSTables()).hasSize(3);

        long[] cumulative = cumulativeUncompressedNewestFirst(cfs); // newest-first: day 10, day 5, day 1

        // need two windows' worth of data, cap of two windows: the gap windows between day 10 and day 5 must not
        // count against the cap, otherwise it would stop at day 10 alone and fall short of the target
        CompressionDictionaryTrainingConfig config = config(midpoint(cumulative[0], cumulative[1]), 2);
        assertThat((long) config.acceptableTotalSampleSize).isGreaterThan(cumulative[0]).isLessThanOrEqualTo(cumulative[1]);

        try (ColumnFamilyStore.RefViewFragment fragment = new RecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment())
        {
            assertThat(fragment.sstables)
            .as("empty windows are skipped for free -> the two newest data-bearing windows (days 10 and 5)")
            .hasSize(2)
            .allMatch(sstable -> sstable.getMaxTimestamp() >= 5 * MICROS_PER_DAY);
        }
    }

    @Test
    public void resolvesNothingForNonTwcs()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) WITH compaction = " +
                    "{'class':'SizeTieredCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertThat(new RecencyBiasViewFragmentResolver(cfs, config(8, 5)).resolveViewFragment())
        .as("recency-bias resolution is only defined for TWCS; anything else resolves to null")
        .isNull();
    }

    @Test
    public void resolvesNothingWhenTwcsHasNoSSTables()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) WITH compaction = " +
                    "{'class':'TimeWindowCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        assertThat(new RecencyBiasViewFragmentResolver(cfs, config(8, 5)).resolveViewFragment())
        .as("a TWCS table with no SSTables has nothing to train on and must resolve to null")
        .isNull();
    }

    /**
     * Builds a training config whose {@code acceptableTotalSampleSize} floor is approximately the requested number of
     * bytes (inverting {@code acceptable = maxTotalSampleSize / 10 * 8}), with the given TWCS window cap.
     */
    private static CompressionDictionaryTrainingConfig config(long desiredAcceptableBytes, int maxWindows)
    {
        int maxTotalSampleSize = (int) Math.max(10, Math.ceil(desiredAcceptableBytes * 10.0 / 8.0));
        return CompressionDictionaryTrainingConfig.builder()
                                                  .maxTotalSampleSize(maxTotalSampleSize)
                                                  .autoTrainingTwcsMaxWindows(maxWindows)
                                                  .build();
    }

    /**
     * As {@link #config} but with a generous window cap, for tests that isolate the sample-size stopping condition.
     */
    private static CompressionDictionaryTrainingConfig configTargeting(long desiredAcceptableBytes)
    {
        return config(desiredAcceptableBytes, 10);
    }

    private static long midpoint(long lower, long upper)
    {
        return lower + (upper - lower) / 2;
    }

    /**
     * A TWCS table (1-day windows) with four windows (days 1..4), one similarly-sized SSTable per window.
     */
    private ColumnFamilyStore fourEqualDayWindows() throws Throwable
    {
        ColumnFamilyStore cfs = twcsDayWindowTable();
        for (int day = 1; day <= 4; day++)
            writeDayWindow(day);

        assertThat(cfs.getLiveSSTables()).as("four windows -> four SSTables").hasSize(4);
        return cfs;
    }

    /**
     * A TWCS table with 1-day windows and auto-compaction disabled (so each flush is its own, window-aligned SSTable).
     */
    private ColumnFamilyStore twcsDayWindowTable()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v text) WITH compaction = " +
                    "{'class':'TimeWindowCompactionStrategy','compaction_window_unit':'DAYS','compaction_window_size':1}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    /**
     * Writes one similarly-sized SSTable whose rows all fall in the given day's TWCS window.
     */
    private void writeDayWindow(int day) throws Throwable
    {
        long base = day * MICROS_PER_DAY;
        for (int i = 0; i < ROWS_PER_WINDOW; i++)
            execute("INSERT INTO %s (id, v) VALUES (?, ?) USING TIMESTAMP ?", day * 1000 + i, TWO_KIB_VALUE, base + i);
        flush();
    }

    private static long maxTimestampOfDay(int day)
    {
        return day * MICROS_PER_DAY + (ROWS_PER_WINDOW - 1);
    }

    /**
     * Cumulative uncompressed bytes over the SSTables ordered newest-first: index i = sum of the (i+1) newest.
     */
    private static long[] cumulativeUncompressedNewestFirst(ColumnFamilyStore cfs)
    {
        List<SSTableReader> sstables = new ArrayList<>(cfs.getLiveSSTables());
        sstables.sort(Comparator.comparingLong(SSTableReader::getMaxTimestamp).reversed());

        long[] cumulative = new long[sstables.size()];
        long running = 0;
        for (int i = 0; i < sstables.size(); i++)
        {
            running += sstables.get(i).uncompressedLength();
            cumulative[i] = running;
        }
        return cumulative;
    }
}
