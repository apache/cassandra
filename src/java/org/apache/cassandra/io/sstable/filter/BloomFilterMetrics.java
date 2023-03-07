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

import java.util.Arrays;
import java.util.List;

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
     * Number of false positives in bloom filter
     */
    public final GaugeProvider<Long> bloomFilterFalsePositives = newGaugeProvider("BloomFilterFalsePositives",
                                                                                  0L,
                                                                                  r -> r.getFilterTracker().getFalsePositiveCount(),
                                                                                  Long::sum);

    /**
     * Number of false positives in bloom filter from last read
     */
    public final GaugeProvider<Long> recentBloomFilterFalsePositives = newGaugeProvider("RecentBloomFilterFalsePositives",
                                                                                        0L,
                                                                                        r -> r.getFilterTracker().getRecentFalsePositiveCount(),
                                                                                        Long::sum);

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
    public final GaugeProvider<Long> bloomFilterOffHeapMemoryUsed = newGaugeProvider("BloomFilterOffHeapMemoryUsed",
                                                                                     0L,
                                                                                     SSTableReaderWithFilter::getFilterOffHeapSize,
                                                                                     Long::sum);

    /**
     * False positive ratio of bloom filter
     */
    public final GaugeProvider<Double> bloomFilterFalseRatio = newGaugeProvider("BloomFilterFalseRatio", readers -> {
        long falsePositiveCount = 0L;
        long truePositiveCount = 0L;
        long trueNegativeCount = 0L;
        for (SSTableReaderWithFilter sstable : readers)
        {
            falsePositiveCount += sstable.getFilterTracker().getFalsePositiveCount();
            truePositiveCount += sstable.getFilterTracker().getTruePositiveCount();
            trueNegativeCount += sstable.getFilterTracker().getTrueNegativeCount();
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
        for (SSTableReaderWithFilter sstable : readers)
        {
            falsePositiveCount += sstable.getFilterTracker().getRecentFalsePositiveCount();
            truePositiveCount += sstable.getFilterTracker().getRecentTruePositiveCount();
            trueNegativeCount += sstable.getFilterTracker().getRecentTrueNegativeCount();
        }
        if (falsePositiveCount == 0L && truePositiveCount == 0L)
            return 0d;
        return (double) falsePositiveCount / (truePositiveCount + falsePositiveCount + trueNegativeCount);
    });

    private final List<GaugeProvider<?>> gaugeProviders = Arrays.asList(bloomFilterFalsePositives,
                                                                        recentBloomFilterFalsePositives,
                                                                        bloomFilterDiskSpaceUsed,
                                                                        bloomFilterOffHeapMemoryUsed,
                                                                        bloomFilterFalseRatio,
                                                                        recentBloomFilterFalseRatio);

    public List<GaugeProvider<?>> getGaugeProviders()
    {
        return gaugeProviders;
    }
}
