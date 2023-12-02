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

import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.CacheMetricsRegister;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;
import org.apache.cassandra.schema.TableMetadata;

final class UnweightedCacheMetricsTable extends AbstractCacheMetricsTable<UnweightedCacheMetrics>
{
    public static final String TABLE_NAME = "unweighted_caches";
    public static final String TABLE_DESCRIPTION = "system unweighted caches";

    public static final String CAPACITY_COLUMN = "capacity";

    UnweightedCacheMetricsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_DESCRIPTION)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME_COLUMN, UTF8Type.instance)
                           .addRegularColumn(CAPACITY_COLUMN, Int32Type.instance)
                           .addRegularColumn(ENTRY_COUNT_COLUMN, Int32Type.instance)
                           .addRegularColumn(HIT_COUNT_COLUMN, LongType.instance)
                           .addRegularColumn(HIT_RATIO_COLUMN, DoubleType.instance)
                           .addRegularColumn(RECENT_HIT_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, LongType.instance)
                           .addRegularColumn(REQUEST_COUNT_COLUMN, LongType.instance)
                           .build(),
              () -> CacheMetricsRegister.getInstance().getUnweightedCacheMetrics());
    }

    @Override
    protected void addRow(SimpleDataSet result, String name, UnweightedCacheMetrics metrics)
    {
        result.row(name)
              .column(CAPACITY_COLUMN, metrics.maxEntries.getValue())
              .column(ENTRY_COUNT_COLUMN, metrics.entries.getValue())
              .column(HIT_COUNT_COLUMN, metrics.hits.getCount())
              .column(HIT_RATIO_COLUMN, metrics.hitRate.getValue())
              .column(RECENT_HIT_RATE_PER_SECOND_COLUMN, (long) metrics.hits.getFifteenMinuteRate())
              .column(RECENT_REQUEST_RATE_PER_SECOND_COLUMN, (long) metrics.requests.getFifteenMinuteRate())
              .column(REQUEST_COUNT_COLUMN, metrics.requests.getCount());
    }
}
