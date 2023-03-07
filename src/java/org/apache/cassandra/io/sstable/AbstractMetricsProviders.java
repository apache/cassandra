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

import org.apache.cassandra.io.sstable.format.SSTableReader;

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

    protected abstract R map(SSTableReader r);
}
