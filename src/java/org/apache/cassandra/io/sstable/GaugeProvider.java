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
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public abstract class GaugeProvider<T extends Number, R extends SSTableReader>
{
    public final SSTableFormat<R, ?> format;
    public final String name;
    public final T neutralValue;
    private final Function<R, T> extractor;
    private final BiFunction<T, T, T> combiner;

    public GaugeProvider(SSTableFormat<R, ?> format, String name, T neutralValue, Function<R, T> extractor, BiFunction<T, T, T> combiner)
    {
        this.format = format;
        this.name = name;
        this.neutralValue = neutralValue;
        this.extractor = extractor;
        this.combiner = combiner;
    }

    public Gauge<T> getTableGauge(ColumnFamilyStore cfs)
    {
        return () -> combine(cfs.getLiveSSTables());
    }

    public Gauge<T> getKeyspaceGauge(Keyspace keyspace)
    {
        return () -> {
            T total = neutralValue;
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                total = combiner.apply(total, combine(cfs.getLiveSSTables()));
            return total;
        };
    }

    private T combine(Iterable<SSTableReader> readers)
    {
        T total = neutralValue;
        for (SSTableReader sstr : readers)
        {
            if (sstr.descriptor.formatType == format.getType())
                total = combiner.apply(total, extractor.apply(format.cast(sstr)));
        }
        return total;
    }
}
