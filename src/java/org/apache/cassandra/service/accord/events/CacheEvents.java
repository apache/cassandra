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

package org.apache.cassandra.service.accord.events;

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Percentage;
import jdk.jfr.StackTrace;

@Category({"Accord", "Accord Cache"})
@StackTrace(false)
public abstract class CacheEvents extends Event
{
    public int store;
    public String instance;
    public String key;
    public String status;
    @DataAmount(DataAmount.BYTES)
    public int lastQueriedEstimatedSizeOnHeap;

    // instance
    @DataAmount(DataAmount.BYTES)
    public long instanceAllocated;
    public long instanceStatsQueries, instanceStatsHits, instanceStatsMisses;

    @Percentage
    public double instanceStatsHitRate;

    // cache
    @DataAmount(DataAmount.BYTES)
    public long globalCapacity, globalAllocated;
    public int globalSize, globalReferenced, globalUnreferenced;

    public long globalStatsQueries, globalStatsHits, globalStatsMisses;

    @Percentage
    public double globalStatsHitRate;

    @Percentage
    public double globalFree;
    public void update()
    {
        instanceStatsHitRate =  1D - (instanceStatsHits / (double) instanceStatsQueries);
        globalStatsHitRate =  1D - (globalStatsHits / (double) globalStatsQueries);
        globalFree =  1.0D - (globalAllocated / (double) globalCapacity);
    }

    @Name("cassandra.accord.cache.Add")
    @Label("Accord Cache Add")
    public static class Add extends CacheEvents { }

    @Name("cassandra.accord.cache.Release")
    @Label("Accord Cache Release")
    public static class Release extends CacheEvents { }

    @Name("cassandra.accord.cache.Evict")
    @Label("Accord Cache Evict")
    public static class Evict extends CacheEvents { }
}
