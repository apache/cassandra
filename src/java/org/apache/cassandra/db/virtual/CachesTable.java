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
package org.apache.cassandra.db.virtual;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;

class CachesTable extends AbstractVirtualTable
{
    public static final String NAME_COLUMN = "name";
    public static final String CAPACITY_BYTES_COLUMN = "capacity_bytes";
    public static final String SIZE_BYTES_COLUMN = "size_bytes";
    public static final String ENTRY_COUNT_COLUMN = "entry_count";
    public static final String REQUEST_COUNT_COLUMN = "request_count";
    public static final String HIT_COUNT_COLUMN = "hit_count";
    public static final String HIT_RATIO_COLUMN = "hit_ratio";
    public static final String RECENT_REQUEST_RATE_PER_SECOND_COLUMN = "recent_request_rate_per_second";
    public static final String RECENT_HIT_RATE_PER_SECOND_COLUMN = "recent_hit_rate_per_second";

    CachesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "caches")
                           .comment("system caches")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(CAPACITY_BYTES_COLUMN, LongType.instance)
                           .addRegularColumn(SIZE_BYTES_COLUMN, LongType.instance)
                           .addRegularColumn(ENTRY_COUNT_COLUMN, Int32Type.instance)
                           .addRegularColumn(REQUEST_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_RATIO_COLUMN, DoubleType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .build());
    }

    private void addRow(SimpleDataSet result, String name, CacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY_BYTES_COLUMN, metrics.capacity.getValue())
              .column(SIZE_BYTES_COLUMN, metrics.size.getValue())
              .column(ENTRY_COUNT_COLUMN, metrics.entries.getValue())
              .column(REQUEST_COUNT_COLUMN, metrics.requests.getCount())
              .column(HIT_COUNT_COLUMN, metrics.hits.getCount())
              .column(HIT_RATIO_COLUMN, metrics.hitRate.getValue())
              .column(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, (long) metrics.requests.getFifteenMinuteRate())
              .column(RECENT_HIT_RATE_PER_SECOND_COLUMN, (long) metrics.hits.getFifteenMinuteRate());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        if (ChunkCache.instance != null)
            addRow(result, "chunks", getChunkCacheMetrics());

        addRow(result, "counters", getCounterCacheMetrics());
        addRow(result, "keys", getKeyCacheMetrics());
        addRow(result, "rows", getRowCacheMetrics());

        return result;
    }

    @VisibleForTesting
    CacheMetrics getChunkCacheMetrics()
    {
        return ChunkCache.instance.metrics;
    }

    @VisibleForTesting
    CacheMetrics getCounterCacheMetrics()
    {
        return CacheService.instance.counterCache.getMetrics();
    }

    @VisibleForTesting
    CacheMetrics getKeyCacheMetrics()
    {
        return CacheService.instance.keyCache.getMetrics();
    }

    @VisibleForTesting
    CacheMetrics getRowCacheMetrics()
    {
        return CacheService.instance.rowCache.getMetrics();
    }
}
