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
package org.apache.cassandra.io.sstable;

import com.codahale.metrics.Meter;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public abstract class BloomFilterTracker
{
    public abstract void addFalsePositive();
    public abstract void addTruePositive();
    public abstract void addTrueNegative();
    public abstract long getFalsePositiveCount();
    public abstract double getRecentFalsePositiveRate();
    public abstract long getTruePositiveCount();
    public abstract double getRecentTruePositiveRate();
    public abstract long getTrueNegativeCount();
    public abstract double getRecentTrueNegativeRate();

    public static BloomFilterTracker createNoopTracker()
    {
        return NoopBloomFilterTracker.instance;
    }

    public static BloomFilterTracker createMeterTracker()
    {
        return new MeterBloomFilterTracker();
    }

    private static class MeterBloomFilterTracker extends BloomFilterTracker
    {
        private final Meter falsePositiveCount = new Meter();
        private final Meter truePositiveCount = new Meter();
        private final Meter trueNegativeCount = new Meter();

        @Override
        public void addFalsePositive()
        {
            falsePositiveCount.mark();
        }

        @Override
        public void addTruePositive()
        {
            truePositiveCount.mark();
        }

        @Override
        public void addTrueNegative()
        {
            trueNegativeCount.mark();
        }

        @Override
        public long getFalsePositiveCount()
        {
            return falsePositiveCount.getCount();
        }

        @Override
        public double getRecentFalsePositiveRate()
        {
            return falsePositiveCount.getFifteenMinuteRate();
        }

        @Override
        public long getTruePositiveCount()
        {
            return truePositiveCount.getCount();
        }

        @Override
        public double getRecentTruePositiveRate()
        {
            return truePositiveCount.getFifteenMinuteRate();
        }

        @Override
        public long getTrueNegativeCount()
        {
            return trueNegativeCount.getCount();
        }

        @Override
        public double getRecentTrueNegativeRate()
        {
            return trueNegativeCount.getFifteenMinuteRate();
        }
    }

    /**
     * Bloom filter tracker that does nothing and always returns 0 for all counters.
     *
     * Bloom Filter tracking is managed on the CFS level, so there is no reason to count anything if an SSTable does not
     * belong (yet) to a CFS. This tracker is used initially on SSTableReaders and is overwritten during setup
     * in {@link SSTableReader#setupOnline()} or {@link SSTableReader#setupOnline(ColumnFamilyStore)}}.
     */
    private static class NoopBloomFilterTracker extends BloomFilterTracker
    {
        static final NoopBloomFilterTracker instance = new NoopBloomFilterTracker();

        @Override
        public void addFalsePositive() {}

        @Override
        public void addTruePositive() {}

        @Override
        public void addTrueNegative() {}

        @Override
        public long getFalsePositiveCount()
        {
            return 0;
        }

        @Override
        public double getRecentFalsePositiveRate()
        {
            return 0;
        }

        @Override
        public long getTruePositiveCount()
        {
            return 0;
        }

        @Override
        public double getRecentTruePositiveRate()
        {
            return 0;
        }

        @Override
        public long getTrueNegativeCount()
        {
            return 0;
        }

        @Override
        public double getRecentTrueNegativeRate()
        {
            return 0;
        }
    }
}
