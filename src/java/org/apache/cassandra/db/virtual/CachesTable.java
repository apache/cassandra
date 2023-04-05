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

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;

final class CachesTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String CAPACITY_BYTES = "capacity_bytes";
    private static final String SIZE_BYTES = "size_bytes";
    private static final String ENTRY_COUNT = "entry_count";
    private static final String REQUEST_COUNT = "request_count";
    private static final String HIT_COUNT = "hit_count";
    private static final String HIT_RATIO = "hit_ratio";
    private static final String RECENT_REQUEST_RATE_PER_SECOND = "recent_request_rate_per_second";
    private static final String RECENT_HIT_RATE_PER_SECOND = "recent_hit_rate_per_second";

    CachesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "caches")
                           .comment("system caches")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(CAPACITY_BYTES, LongType.instance)
                           .addRegularColumn(SIZE_BYTES, LongType.instance)
                           .addRegularColumn(ENTRY_COUNT, Int32Type.instance)
                           .addRegularColumn(REQUEST_COUNT, LongType.instance)
                           .addRegularColumn(HIT_COUNT, LongType.instance)
                           .addRegularColumn(HIT_RATIO, DoubleType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND, LongType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND, LongType.instance)
                           .build());
    }

    private void addRow(SimpleDataSet result, String name, CacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY_BYTES, metrics.capacity.getValue())
              .column(SIZE_BYTES, metrics.size.getValue())
              .column(ENTRY_COUNT, metrics.entries.getValue())
              .column(REQUEST_COUNT, metrics.requests.getCount())
              .column(HIT_COUNT, metrics.hits.getCount())
              .column(HIT_RATIO, metrics.hitRate.getValue())
              .column(RECENT_REQUEST_RATE_PER_SECOND, (long) metrics.requests.getFifteenMinuteRate())
              .column(RECENT_HIT_RATE_PER_SECOND, (long) metrics.hits.getFifteenMinuteRate());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        if (null != ChunkCache.instance)
            addRow(result, "chunks", ChunkCache.instance.metrics);
        addRow(result, "counters", CacheService.instance.counterCache.getMetrics());
        addRow(result, "keys", CacheService.instance.keyCache.getMetrics());
        addRow(result, "rows", CacheService.instance.rowCache.getMetrics());

        return result;
    }
}
