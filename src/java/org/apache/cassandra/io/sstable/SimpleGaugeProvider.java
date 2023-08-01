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

import java.util.Objects;
import java.util.function.Function;

import com.google.common.collect.Iterables;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class SimpleGaugeProvider<T extends Number, R extends SSTableReader> extends GaugeProvider<T>
{
    private final Function<SSTableReader, R> mapper;
    private final Function<Iterable<R>, T> combiner;

    public SimpleGaugeProvider(Function<SSTableReader, R> mapper, String name, Function<Iterable<R>, T> combiner)
    {
        super(name);
        this.mapper = mapper;
        this.combiner = combiner;
    }

    @Override
    public Gauge<T> getTableGauge(ColumnFamilyStore cfs)
    {
        return () -> combine(cfs.getLiveSSTables());
    }

    @Override
    public Gauge<T> getKeyspaceGauge(Keyspace keyspace)
    {
        return () -> combine(getAllReaders(keyspace));
    }

    @Override
    public Gauge<T> getGlobalGauge()
    {
        return () -> combine(Iterables.concat(Iterables.transform(Keyspace.all(), SimpleGaugeProvider::getAllReaders)));
    }

    private T combine(Iterable<SSTableReader> allReaders)
    {
        Iterable<R> readers = Iterables.filter(Iterables.transform(allReaders, mapper::apply), Objects::nonNull);
        return combiner.apply(readers);
    }

    private static Iterable<SSTableReader> getAllReaders(Keyspace keyspace)
    {
        return Iterables.concat(Iterables.transform(keyspace.getColumnFamilyStores(), ColumnFamilyStore::getLiveSSTables));
    }
}
