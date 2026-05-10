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

package org.apache.cassandra.service.writes.thresholds;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.schema.TableId;

public class WriteThresholdCounter
{
    private static final WriteThresholdCounter EMPTY = new WriteThresholdCounter(ImmutableMap.of());
    public final ImmutableMap<TableId, Long> tableValues;

    private WriteThresholdCounter(ImmutableMap<TableId, Long> tableValues)
    {
        this.tableValues = tableValues;
    }

    public static WriteThresholdCounter empty()
    {
        return EMPTY;
    }

    public boolean isEmpty()
    {
        return tableValues.isEmpty();
    }

    public static WriteThresholdCounter create(Map<TableId, Long> snapshot)
    {
        if (snapshot.isEmpty())
            return EMPTY;
        return new WriteThresholdCounter(ImmutableMap.copyOf(snapshot));
    }

    public WriteThresholdCounter merge(WriteThresholdCounter other)
    {
        if (other == EMPTY)
            return this;
        if (this == EMPTY)
            return other;
        Map<TableId, Long> merged = new HashMap<>(tableValues);
        for (Map.Entry<TableId, Long> entry : other.tableValues.entrySet())
            merged.merge(entry.getKey(), entry.getValue(), Math::max);
        return new WriteThresholdCounter(ImmutableMap.copyOf(merged));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WriteThresholdCounter that = (WriteThresholdCounter) o;
        return Objects.equals(tableValues, that.tableValues);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableValues);
    }

    @Override
    public String toString()
    {
        return "WriteThresholdCounter{tableValues=" + tableValues + '}';
    }
}
