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
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class IndexMetrics extends AbstractMetrics
{
    public final Timer memtableIndexWriteLatency;
    
    public final Gauge ssTableCellCount;
    public final Gauge liveMemtableIndexWriteCount;
    public final Gauge diskUsedBytes;
    public final Gauge memtableIndexBytes;
    public final Gauge indexFileCacheBytes;
    
    public final Counter memtableIndexFlushCount;
    public final Counter compactionCount;
    public final Counter memtableIndexFlushErrors;
    public final Counter segmentFlushErrors;
    
    public final Histogram memtableFlushCellsPerSecond;
    public final Histogram segmentsPerCompaction;
    public final Histogram compactionSegmentCellsPerSecond;
    public final Histogram compactionSegmentBytesPerSecond;

    public IndexMetrics(ColumnContext context, TableMetadata table)
    {
        super(table, context.getIndexName(), "IndexMetrics");

        memtableIndexWriteLatency = Metrics.timer(createMetricName("MemtableIndexWriteLatency"));
        compactionSegmentCellsPerSecond = Metrics.histogram(createMetricName("CompactionSegmentCellsPerSecond"), false);
        compactionSegmentBytesPerSecond = Metrics.histogram(createMetricName("CompactionSegmentBytesPerSecond"), false);
        memtableFlushCellsPerSecond = Metrics.histogram(createMetricName("MemtableIndexFlushCellsPerSecond"), false);
        segmentsPerCompaction = Metrics.histogram(createMetricName("SegmentsPerCompaction"), false);
        ssTableCellCount = Metrics.register(createMetricName("SSTableCellCount"), context::getCellCount);
        memtableIndexFlushCount = Metrics.counter(createMetricName("MemtableIndexFlushCount"));
        compactionCount = Metrics.counter(createMetricName("CompactionCount"));
        memtableIndexFlushErrors = Metrics.counter(createMetricName("MemtableIndexFlushErrors"));
        segmentFlushErrors = Metrics.counter(createMetricName("CompactionSegmentFlushErrors"));
        liveMemtableIndexWriteCount = Metrics.register(createMetricName("LiveMemtableIndexWriteCount"), context::liveMemtableWriteCount);
        memtableIndexBytes = Metrics.register(createMetricName("MemtableIndexBytes"), context::estimatedMemIndexMemoryUsed);
        diskUsedBytes = Metrics.register(createMetricName("DiskUsedBytes"), context::diskUsage);
        indexFileCacheBytes = Metrics.register(createMetricName("IndexFileCacheBytes"), context::indexFileCacheSize);
    }
}
