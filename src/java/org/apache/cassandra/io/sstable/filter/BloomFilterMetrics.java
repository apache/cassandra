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

package org.apache.cassandra.io.sstable.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

import org.apache.cassandra.io.sstable.AbstractMetricsProviders;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;

public class BloomFilterMetrics<R extends SSTableReaderWithFilter> extends AbstractMetricsProviders<R>
{
    public final static BloomFilterMetrics<?> instance = new BloomFilterMetrics<>();

    @Override
    protected R map(SSTableReader r)
    {
        if (r instanceof SSTableReaderWithFilter)
            return (R) r;
        return null;
    }

    /**
     * The trackers of the given readers, each tracker being included only once.
     * <p>
     * A reader gets its tracker from its {@link org.apache.cassandra.io.sstable.SSTable.Owner}, so all the readers of a
     * table share the table's tracker. Summing a tracker's counters once per reader would therefore multiply them by
     * the number of live sstables, and would weight keyspace/global aggregates by the number of sstables of each table.
     */
    private static List<BloomFilterTracker> distinctTrackers(Iterable<? extends SSTableReaderWithFilter> readers)
    {
        Set<BloomFilterTracker> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<BloomFilterTracker> trackers = new ArrayList<>();
        for (SSTableReaderWithFilter reader : readers)
        {
            BloomFilterTracker tracker = reader.getFilterTracker();
            if (seen.add(tracker))
                trackers.add(tracker);
        }
        return trackers;
    }

    private static long sumOverTrackers(Iterable<? extends SSTableReaderWithFilter> readers, ToLongFunction<BloomFilterTracker> counter)
    {
        long total = 0L;
        for (BloomFilterTracker tracker : distinctTrackers(readers))
            total += counter.applyAsLong(tracker);
        return total;
    }

    /**
     * Number of false positives in bloom filter
     */
    public final GaugeProvider<Long> bloomFilterFalsePositives = newGaugeProvider("BloomFilterFalsePositives",
                                                                                  readers -> sumOverTrackers(readers, BloomFilterTracker::getFalsePositiveCount));

    /**
     * Number of false positives in bloom filter from last read
     */
    public final GaugeProvider<Long> recentBloomFilterFalsePositives = newGaugeProvider("RecentBloomFilterFalsePositives",
                                                                                        readers -> sumOverTrackers(readers, BloomFilterTracker::getRecentFalsePositiveCount));

    /**
     * Disk space used by bloom filter
     */
    public final GaugeProvider<Long> bloomFilterDiskSpaceUsed = newGaugeProvider("BloomFilterDiskSpaceUsed",
                                                                                 0L,
                                                                                 SSTableReaderWithFilter::getFilterSerializedSize,
                                                                                 Long::sum);

    /**
     * Off heap memory used by bloom filter
     */
    public final GaugeProvider<Long> bloomFilterOffHeapMemoryUsed = bloomFilterOffHeapMemoryUsedAwareCFSGaugeProvider("BloomFilterOffHeapMemoryUsed",
                                                                                     0L,
                                                                                     SSTableReaderWithFilter::getFilterOffHeapSize,
                                                                                     Long::sum);

    public final GaugeProvider<Long> approximateBloomFilterOffHeapMemoryUsed = newGaugeProvider("ApproximateBloomFilterOffHeapMemoryUsed",
                                                                                                0L,
                                                                                                SSTableReaderWithFilter::getApproximateBloomFilterMemorySize,
                                                                                                Long::sum);

    public final GaugeProvider<Long> loadedBloomFilter = newGaugeProvider("LoadedBloomFilter",
                                                                          0L,
                                                                          r -> r.isBloomFilterLoaded() ? 1L : 0L,
                                                                          Long::sum);

    public final GaugeProvider<Long> lazyBloomFilter = newGaugeProvider("LazyBloomFilter",
                                                                        0L,
                                                                        r -> r.isLazyBloomFilter() ? 1L : 0L,
                                                                        Long::sum);

    public final GaugeProvider<Long> passThroughBloomFilter = newGaugeProvider("PassThroughBloomFilter",
                                                                               0L,
                                                                               r -> r.isPassThroughBloomFilter() ? 1L : 0L,
                                                                               Long::sum);

    public final GaugeProvider<Long> lazyBloomFilterByRequestRate = newGaugeProvider("LazyBloomFilterByRequestRate",
                                                                                     0L,
                                                                                     r -> r.isLazyBloomFilterByRequestRateCriteria() ? 1L : 0L,
                                                                                     Long::sum);

    public final GaugeProvider<Long> lazyBloomFilterByRequestCount = newGaugeProvider("LazyBloomFilterByRequestCount",
                                                                                      0L,
                                                                                      r -> r.isLazyBloomFilterByRequestCountCriteria() ? 1L : 0L,
                                                                                      Long::sum);

    public final GaugeProvider<Long> lazyBloomFilterHits = newGaugeProvider("LazyBloomFilterHits",
                                                                            readers -> sumOverTrackers(readers, BloomFilterTracker::getLazyBloomFilterHitCount));

    public final GaugeProvider<Long> loadedBloomFilterHits = newGaugeProvider("LoadedBloomFilterHits",
                                                                              readers -> sumOverTrackers(readers, BloomFilterTracker::getLoadedBloomFilterHitCount));

    public final GaugeProvider<Long> passThroughBloomFilterHits = newGaugeProvider("PassThroughBloomFilterHits",
                                                                                   readers -> sumOverTrackers(readers, BloomFilterTracker::getPassThroughBloomFilterHitCount));

    /**
     * False positive ratio of bloom filter
     */
    public final GaugeProvider<Double> bloomFilterFalseRatio = newGaugeProvider("BloomFilterFalseRatio", readers -> {
        long falsePositiveCount = 0L;
        long truePositiveCount = 0L;
        long trueNegativeCount = 0L;
        for (BloomFilterTracker tracker : distinctTrackers(readers))
        {
            falsePositiveCount += tracker.getFalsePositiveCount();
            truePositiveCount += tracker.getTruePositiveCount();
            trueNegativeCount += tracker.getTrueNegativeCount();
        }
        if (falsePositiveCount == 0L && truePositiveCount == 0L)
            return 0d;
        return (double) falsePositiveCount / (truePositiveCount + falsePositiveCount + trueNegativeCount);
    });

    /**
     * False positive ratio of bloom filter from last read
     */
    public final GaugeProvider<Double> recentBloomFilterFalseRatio = newGaugeProvider("RecentBloomFilterFalseRatio", readers -> {
        long falsePositiveCount = 0L;
        long truePositiveCount = 0L;
        long trueNegativeCount = 0L;
        for (BloomFilterTracker tracker : distinctTrackers(readers))
        {
            falsePositiveCount += tracker.getRecentFalsePositiveCount();
            truePositiveCount += tracker.getRecentTruePositiveCount();
            trueNegativeCount += tracker.getRecentTrueNegativeCount();
        }
        if (falsePositiveCount == 0L && truePositiveCount == 0L)
            return 0d;
        return (double) falsePositiveCount / (truePositiveCount + falsePositiveCount + trueNegativeCount);
    });

    private final List<GaugeProvider<?>> gaugeProviders = Arrays.asList(bloomFilterFalsePositives,
                                                                        recentBloomFilterFalsePositives,
                                                                        bloomFilterDiskSpaceUsed,
                                                                        bloomFilterOffHeapMemoryUsed,
                                                                        approximateBloomFilterOffHeapMemoryUsed,
                                                                        loadedBloomFilter,
                                                                        lazyBloomFilter,
                                                                        passThroughBloomFilter,
                                                                        lazyBloomFilterByRequestRate,
                                                                        lazyBloomFilterByRequestCount,
                                                                        lazyBloomFilterHits,
                                                                        loadedBloomFilterHits,
                                                                        passThroughBloomFilterHits,
                                                                        bloomFilterFalseRatio,
                                                                        recentBloomFilterFalseRatio);

    public List<GaugeProvider<?>> getGaugeProviders()
    {
        return gaugeProviders;
    }
}
