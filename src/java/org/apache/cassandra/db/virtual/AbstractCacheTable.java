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
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.cassandra.metrics.AbstractCacheMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

abstract class AbstractCacheTable<METRICS_TYPE extends AbstractCacheMetrics> extends AbstractVirtualTable
{
    protected final Collection<Supplier<Optional<Pair<String, METRICS_TYPE>>>> metricsSuppliers;

    protected AbstractCacheTable(TableMetadata metadata, Collection<Supplier<Optional<Pair<String, METRICS_TYPE>>>> metricsSuppliers)
    {
        super(metadata);
        this.metricsSuppliers = metricsSuppliers;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Supplier<Optional<Pair<String, METRICS_TYPE>>> metricsSupplier : metricsSuppliers)
        {
            Optional<Pair<String, METRICS_TYPE>> optionalPair = metricsSupplier.get();
            if (optionalPair.isPresent())
            {
                Pair<String, METRICS_TYPE> pair = optionalPair.get();
                addRow(result, pair.left, pair.right);
            }
        }
        return result;
    }

    protected abstract void addRow(SimpleDataSet result, String name, METRICS_TYPE metrics);
}
