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

package org.apache.cassandra.io.sstable;

import java.util.function.BiFunction;
import java.util.function.Function;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TableMetrics;

public abstract class AbstractMetricsProviders<R extends SSTableReader> implements MetricsProviders
{
    protected final <T extends Number> GaugeProvider<T> newGaugeProvider(String name, Function<Iterable<R>, T> combiner)
    {
        return new SimpleGaugeProvider<>(this::map, name, combiner);
    }

    protected final <T extends Number> GaugeProvider<T> newGaugeProvider(String name, T neutralValue, Function<R, T> extractor, BiFunction<T, T, T> combiner)
    {
        return new SimpleGaugeProvider<>(this::map, name, readers -> {
            T total = neutralValue;
            for (R reader : readers)
                total = combiner.apply(total, extractor.apply(reader));
            return total;
        });
    }

    /**
     * Creates a gauge provider that combines the values extracted from the readers with the in-flight bloom filter
     * off-heap memory of the tables, that is the memory of the filters of sstables that are being written and have not
     * been opened yet. The in-flight memory is added for every table covered by the gauge, whether or not the table has
     * any live sstable.
     */
    protected final GaugeProvider<Long> bloomFilterOffHeapMemoryUsedAwareCFSGaugeProvider(String name, Long neutralValue, Function<R, Long> extractor, BiFunction<Long, Long, Long> combiner)
    {
        GaugeProvider<Long> readersProvider = newGaugeProvider(name, neutralValue, extractor, combiner);

        return new GaugeProvider<Long>(name)
        {
            @Override
            public Gauge<Long> getTableGauge(ColumnFamilyStore cfs)
            {
                Gauge<Long> readers = readersProvider.getTableGauge(cfs);
                return () -> combiner.apply(readers.getValue(), inFlight(cfs));
            }

            @Override
            public Gauge<Long> getKeyspaceGauge(Keyspace keyspace)
            {
                Gauge<Long> readers = readersProvider.getKeyspaceGauge(keyspace);
                return () -> combiner.apply(readers.getValue(), inFlight(keyspace));
            }

            @Override
            public Gauge<Long> getGlobalGauge()
            {
                Gauge<Long> readers = readersProvider.getGlobalGauge();
                return () -> {
                    long inFlight = 0;
                    for (Keyspace keyspace : Keyspace.all())
                        inFlight += inFlight(keyspace);
                    return combiner.apply(readers.getValue(), inFlight);
                };
            }

            private long inFlight(Keyspace keyspace)
            {
                long total = 0;
                for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    total += inFlight(cfs);
                return total;
            }

            private long inFlight(ColumnFamilyStore cfs)
            {
                // cfs.metric is assigned only after the TableMetrics constructor returns, while gauges can be
                // read by metric listeners as soon as they are registered
                TableMetrics metric = cfs.metric;
                return metric == null ? 0 : metric.inFlightBloomFilterOffHeapMemoryUsed.get();
            }
        };
    }

    protected abstract R map(SSTableReader r);
}
