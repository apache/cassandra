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
package org.apache.cassandra.index.sai.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.memory.MemtableIndexManager;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class IndexMetrics extends AbstractMetrics
{
    public final Timer memtableIndexWriteLatency;

    public final Counter memtableIndexFlushCount;
    public final Counter compactionCount;
    public final Counter memtableIndexFlushErrors;
    public final Counter segmentFlushErrors;

    public final Histogram memtableFlushCellsPerSecond;
    public final Histogram segmentsPerCompaction;
    public final Histogram compactionSegmentCellsPerSecond;
    public final Histogram compactionSegmentBytesPerSecond;

    public IndexMetrics(StorageAttachedIndex index, MemtableIndexManager memtableIndexManager)
    {
        super(index.identifier(), "IndexMetrics");

        memtableIndexWriteLatency = Metrics.timer(createMetricName("MemtableIndexWriteLatency"));
        compactionSegmentCellsPerSecond = Metrics.histogram(createMetricName("CompactionSegmentCellsPerSecond"), false);
        compactionSegmentBytesPerSecond = Metrics.histogram(createMetricName("CompactionSegmentBytesPerSecond"), false);
        memtableFlushCellsPerSecond = Metrics.histogram(createMetricName("MemtableIndexFlushCellsPerSecond"), false);
        segmentsPerCompaction = Metrics.histogram(createMetricName("SegmentsPerCompaction"), false);
        memtableIndexFlushCount = Metrics.counter(createMetricName("MemtableIndexFlushCount"));
        compactionCount = Metrics.counter(createMetricName("CompactionCount"));
        memtableIndexFlushErrors = Metrics.counter(createMetricName("MemtableIndexFlushErrors"));
        segmentFlushErrors = Metrics.counter(createMetricName("CompactionSegmentFlushErrors"));
        Metrics.register(createMetricName("SSTableCellCount"), (Gauge<Long>) index::cellCount);
        Metrics.register(createMetricName("LiveMemtableIndexWriteCount"), (Gauge<Long>) memtableIndexManager::liveMemtableWriteCount);
        Metrics.register(createMetricName("MemtableIndexBytes"), (Gauge<Long>) memtableIndexManager::estimatedMemIndexMemoryUsed);
        Metrics.register(createMetricName("DiskUsedBytes"), (Gauge<Long>) index::diskUsage);
        Metrics.register(createMetricName("IndexFileCacheBytes"), (Gauge<Long>) index::indexFileCacheSize);
    }
}
