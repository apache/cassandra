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

package org.apache.cassandra.io.sstable.indexsummary;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.io.sstable.AbstractMetricsProviders;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class IndexSummaryMetrics<R extends SSTableReader & IndexSummarySupport<R>> extends AbstractMetricsProviders<R>
{
    public final static IndexSummaryMetrics<?> instance = new IndexSummaryMetrics<>();

    @Override
    protected R map(SSTableReader r)
    {
        if (r instanceof IndexSummarySupport<?>)
            return (R) r;
        return null;
    }

    private final GaugeProvider<Long> indexSummaryOffHeapMemoryUsed = newGaugeProvider("IndexSummaryOffHeapMemoryUsed",
                                                                                       0L,
                                                                                       r -> r.getIndexSummary().getOffHeapSize(),
                                                                                       Long::sum);

    private final List<GaugeProvider<?>> gaugeProviders = Arrays.asList(indexSummaryOffHeapMemoryUsed);

    public List<GaugeProvider<?>> getGaugeProviders()
    {
        return gaugeProviders;
    }
}
