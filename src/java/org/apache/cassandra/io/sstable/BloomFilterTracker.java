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

import java.util.concurrent.atomic.AtomicLong;

public class BloomFilterTracker
{
    private final AtomicLong falsePositiveCount = new AtomicLong(0);
    private final AtomicLong truePositiveCount = new AtomicLong(0);
    private long lastFalsePositiveCount = 0L;
    private long lastTruePositiveCount = 0L;

    public void addFalsePositive()
    {
        falsePositiveCount.incrementAndGet();
    }

    public void addTruePositive()
    {
        truePositiveCount.incrementAndGet();
    }

    public long getFalsePositiveCount()
    {
        return falsePositiveCount.get();
    }

    public long getRecentFalsePositiveCount()
    {
        long fpc = getFalsePositiveCount();
        try
        {
            return (fpc - lastFalsePositiveCount);
        }
        finally
        {
            lastFalsePositiveCount = fpc;
        }
    }

    public long getTruePositiveCount()
    {
        return truePositiveCount.get();
    }

    public long getRecentTruePositiveCount()
    {
        long tpc = getTruePositiveCount();
        try
        {
            return (tpc - lastTruePositiveCount);
        }
        finally
        {
            lastTruePositiveCount = tpc;
        }
    }
}
