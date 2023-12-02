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

import java.util.Collection;
import java.util.function.Supplier;

import org.apache.cassandra.metrics.AbstractCacheMetrics;
import org.apache.cassandra.schema.TableMetadata;

abstract class AbstractCacheMetricsTable<T extends AbstractCacheMetrics> extends AbstractVirtualTable
{
    public static final String NAME_COLUMN = "name";
    public static final String ENTRY_COUNT_COLUMN = "entry_count";
    public static final String HIT_COUNT_COLUMN = "hit_count";
    public static final String HIT_RATIO_COLUMN = "hit_ratio";
    public static final String REQUEST_COUNT_COLUMN = "request_count";
    public static final String RECENT_REQUEST_RATE_PER_SECOND_COLUMN = "recent_request_rate_per_second";
    public static final String RECENT_HIT_RATE_PER_SECOND_COLUMN = "recent_hit_rate_per_second";

    private final Supplier<Collection<T>> metricsSuppliers;

    protected AbstractCacheMetricsTable(TableMetadata metadata, Supplier<Collection<T>> metricsSuppliers)
    {
        super(metadata);
        this.metricsSuppliers = metricsSuppliers;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (T metrics : metricsSuppliers.get())
            addRow(result, metrics.type, metrics);

        return result;
    }

    protected abstract void addRow(SimpleDataSet result, String name, T metrics);
}
