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

package org.apache.cassandra.service.accord.db;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import accord.api.Data;
import accord.api.Result;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.FilteredPartition;

public class AccordData implements Data, Result
{
    private final NavigableMap<DecoratedKey, FilteredPartition> partitions = new TreeMap<>();

    void put(FilteredPartition partition)
    {
        DecoratedKey key = partition.partitionKey();
        Preconditions.checkArgument(!partitions.containsKey(key) || partitions.get(key).equals(partition));
        partitions.put(key, partition);
    }

    FilteredPartition get(DecoratedKey key)
    {
        return partitions.get(key);
    }

    @Override
    public Data merge(Data data)
    {
        AccordData that = (AccordData) data;
        AccordData merged = new AccordData();
        this.forEach(merged::put);
        that.forEach(merged::put);
        return merged;
    }

    public void forEach(Consumer<FilteredPartition> consumer)
    {
        partitions.values().forEach(consumer);
    }
}
