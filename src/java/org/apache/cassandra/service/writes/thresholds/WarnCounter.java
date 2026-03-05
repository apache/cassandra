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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.schema.TableId;

public class WarnCounter
{
    private final ConcurrentHashMap<TableId, AtomicLong> tableValues = new ConcurrentHashMap<>();

    void addWarning(Map<TableId, Long> incoming)
    {
        for (Map.Entry<TableId, Long> entry : incoming.entrySet())
            tableValues.computeIfAbsent(entry.getKey(), k -> new AtomicLong())
                       .accumulateAndGet(entry.getValue(), Math::max);
    }

    public WriteThresholdCounter snapshot()
    {
        ImmutableMap.Builder<TableId, Long> builder = ImmutableMap.builder();
        for (Map.Entry<TableId, AtomicLong> entry : tableValues.entrySet())
            builder.put(entry.getKey(), entry.getValue().get());

        return WriteThresholdCounter.create(builder.build());
    }
}
