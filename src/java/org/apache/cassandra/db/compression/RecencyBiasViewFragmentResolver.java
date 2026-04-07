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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TimeWindowCompactionStrategyOptions;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Resolves the SSTable {@link ColumnFamilyStore.RefViewFragment} that auto-training should sample from, biased
 * toward recent data so training can detect that the data has drifted (training over the whole dataset would just
 * relearn the mostly-old distribution and never notice the change).
 * <p>
 * Only {@link TimeWindowCompactionStrategy} is supported for now. A TWCS time window is the unit of selection: the
 * resolver walks windows newest-first and adds each window <em>whole</em> (all of its SSTables), accumulating their
 * uncompressed size, and stops as soon as it has gathered {@link CompressionDictionaryTrainingConfig#acceptableTotalSampleSize}
 * bytes - the same floor the trainer requires to consider a sample big enough to train on. It never reaches back more
 * than {@code auto_training_twcs_max_windows} windows (default 1, i.e. the newest window only), so recency is bounded
 * even when the data is thin.
 * <p>
 * Resolution yields {@code null} - which the caller treats as "nothing to train on this round" and backs off - when
 * the table is not on TWCS, when it has no SSTables, or when the recent windows within the cap do not hold enough
 * data to reach the sample floor (training on a too-small sample is worse than waiting for more data to arrive).
 */
public class RecencyBiasViewFragmentResolver
{
    private static final Logger logger = LoggerFactory.getLogger(RecencyBiasViewFragmentResolver.class);

    protected final ColumnFamilyStore cfs;
    protected final CompressionDictionaryTrainingConfig config;

    public RecencyBiasViewFragmentResolver(ColumnFamilyStore cfs, CompressionDictionaryTrainingConfig config)
    {
        this.cfs = cfs;
        this.config = config;
    }

    public ColumnFamilyStore.RefViewFragment resolveViewFragment()
    {
        Class<?> compactionStrategyClass = cfs.getCompactionStrategyManager().getCompactionParams().klass();

        if (TimeWindowCompactionStrategy.class.isAssignableFrom(compactionStrategyClass))
            return new TimeWindowRecencyBiasViewFragmentResolver(cfs, config).resolveViewFragment();

        return null;
    }

    /**
     * Selects whole TWCS time windows, newest-first, until enough uncompressed data has been gathered to train on
     * (or the configured window cap is hit).
     */
    public static class TimeWindowRecencyBiasViewFragmentResolver extends RecencyBiasViewFragmentResolver
    {
        public TimeWindowRecencyBiasViewFragmentResolver(ColumnFamilyStore cfs, CompressionDictionaryTrainingConfig config)
        {
            super(cfs, config);
        }

        @Override
        public ColumnFamilyStore.RefViewFragment resolveViewFragment()
        {
            Map<String, String> options = cfs.getCompactionStrategyManager().getCompactionParams().options();

            TimeUnit windowUnit = options.containsKey(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY)
                                  ? TimeUnit.valueOf(options.get(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY))
                                  : TimeUnit.DAYS;
            int windowSize = options.containsKey(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY)
                             ? Integer.parseInt(options.get(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY))
                             : 1;
            TimeUnit timestampResolution = options.containsKey(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY)
                                           ? TimeUnit.valueOf(options.get(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY))
                                           : TimeUnit.MICROSECONDS;

            // Bucket SSTables into TWCS windows and total the uncompressed bytes each window holds. Only windows that
            // actually contain SSTables become keys, so empty windows - gaps in the timeline, including an as-yet-
            // unflushed current window - are simply absent here. The map is ordered by window lower bound, so its
            // descending key set is the non-empty windows, newest-first.
            NavigableMap<Long, Long> uncompressedBytesByWindow = new TreeMap<>();
            for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            {
                long window = windowLowerBound(sstable, windowUnit, windowSize, timestampResolution);
                uncompressedBytesByWindow.merge(window, sstable.uncompressedLength(), Long::sum);
            }

            if (uncompressedBytesByWindow.isEmpty())
                return null;

            // Add whole windows, newest-first, until we have enough sample data to train on or we have reached back
            // as far as the configured cap allows. A window is never split: it contributes all of its SSTables. Since
            // we only ever iterate non-empty windows, empty windows are skipped for free and never spend the cap - the
            // cap bounds how many windows holding data we consume, not how far back in time we look.
            Set<Long> selectedWindows = new HashSet<>();
            long accumulatedBytes = 0;
            for (Long window : uncompressedBytesByWindow.descendingKeySet())
            {
                if (selectedWindows.size() >= config.autoTrainingTwcsMaxWindows)
                    break;

                selectedWindows.add(window);
                accumulatedBytes += uncompressedBytesByWindow.get(window);

                if (accumulatedBytes >= config.acceptableTotalSampleSize)
                    break;
            }

            // Even after reaching back as far as the cap allows, the recent windows may not hold enough data to train
            // on. Rather than train on a too-small sample, back off: return null so the auto-trainer skips this round.
            if (accumulatedBytes < config.acceptableTotalSampleSize)
            {
                logger.debug("Recency-biased sampling for {}.{} gathered only {} across the {} most-recent TWCS window(s) " +
                             "(auto_training_twcs_max_windows={}), below the {} needed to train; backing off this round.",
                             cfs.getKeyspaceName(), cfs.getTableName(),
                             FileUtils.stringifyFileSize(accumulatedBytes, true),
                             selectedWindows.size(), config.autoTrainingTwcsMaxWindows,
                             FileUtils.stringifyFileSize(config.acceptableTotalSampleSize, true));
                return null;
            }

            ColumnFamilyStore.RefViewFragment fragment =
                cfs.selectAndReference(View.select(SSTableSet.CANONICAL,
                                                   sstable -> selectedWindows.contains(windowLowerBound(sstable, windowUnit, windowSize, timestampResolution))));

            // The window accounting above ran against an earlier, unreferenced view. A compaction completing since
            // then can leave nothing matching the predicate, so return null as documented rather than an empty
            // fragment the caller would mistake for work to do.
            if (fragment.sstables.isEmpty())
            {
                logger.debug("Recency-biased sampling for {}.{} selected no SSTables; they were compacted away while resolving.",
                             cfs.getKeyspaceName(), cfs.getTableName());
                fragment.close();
                return null;
            }

            return fragment;
        }

        /**
         * The lower bound (in ms) of the TWCS time window an SSTable falls into, computed the same way TWCS
         * buckets SSTables: its max timestamp, converted from the table's timestamp resolution to ms, snapped
         * down to the window.
         */
        private static long windowLowerBound(SSTableReader sstable, TimeUnit windowUnit, int windowSize, TimeUnit timestampResolution)
        {
            long timestampInMillis = TimeUnit.MILLISECONDS.convert(sstable.getMaxTimestamp(), timestampResolution);
            return TimeWindowCompactionStrategy.getWindowBoundsInMillis(windowUnit, windowSize, timestampInMillis).left;
        }
    }
}
