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

package org.apache.cassandra.io.sstable.keycache;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.io.sstable.AbstractMetricsProviders;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class KeyCacheMetrics<R extends SSTableReader & KeyCacheSupport<R>> extends AbstractMetricsProviders<R>
{
    public final static KeyCacheMetrics<?> instance = new KeyCacheMetrics<>();

    @Override
    protected R map(SSTableReader r)
    {
        if (r instanceof KeyCacheSupport<?>)
            return (R) r;
        return null;
    }

    /** Key cache hit rate  for this CF */
    private final GaugeProvider<Double> keyCacheHitRate = newGaugeProvider("KeyCacheHitRate", readers -> {
        long hits = 0L;
        long requests = 0L;
        for (R sstable : readers)
        {
            hits += sstable.getKeyCache().getHits();
            requests += sstable.getKeyCache().getRequests();
        }

        return (double) hits / (double) Math.max(1, requests);
    });

    private final List<GaugeProvider<?>> gaugeProviders = Arrays.asList(keyCacheHitRate);

    public List<GaugeProvider<?>> getGaugeProviders()
    {
        return gaugeProviders;
    }
}
