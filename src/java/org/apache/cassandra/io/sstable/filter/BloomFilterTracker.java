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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class BloomFilterTracker
{
    private final LongAdder falsePositiveCount = new LongAdder();
    private final LongAdder truePositiveCount = new LongAdder();
    private final LongAdder trueNegativeCount = new LongAdder();
    private final AtomicLong lastFalsePositiveCount = new AtomicLong();
    private final AtomicLong lastTruePositiveCount = new AtomicLong();
    private final AtomicLong lastTrueNegativeCount = new AtomicLong();

    public void addFalsePositive()
    {
        falsePositiveCount.increment();
    }

    public void addTruePositive()
    {
        truePositiveCount.increment();
    }

    public void addTrueNegative()
    {
        trueNegativeCount.increment();
    }

    public long getFalsePositiveCount()
    {
        return falsePositiveCount.sum();
    }

    public long getRecentFalsePositiveCount()
    {
        long fpc = getFalsePositiveCount();
        long last = lastFalsePositiveCount.getAndSet(fpc);
        return fpc - last;
    }

    public long getTruePositiveCount()
    {
        return truePositiveCount.sum();
    }

    public long getRecentTruePositiveCount()
    {
        long tpc = getTruePositiveCount();
        long last = lastTruePositiveCount.getAndSet(tpc);
        return tpc - last;
    }

    public long getTrueNegativeCount()
    {
        return trueNegativeCount.sum();
    }

    public long getRecentTrueNegativeCount()
    {
        long tnc = getTrueNegativeCount();
        long last = lastTrueNegativeCount.getAndSet(tnc);
        return tnc - last;
    }
}
