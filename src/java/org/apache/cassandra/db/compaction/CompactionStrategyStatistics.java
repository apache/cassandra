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

package org.apache.cassandra.db.compaction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.cassandra.schema.TableMetadata;

/**
 * The statistics for a compaction strategy, to be published over JMX and insights.
 * <p/>
 * Implements serializable to allow structured info to be returned via JMX.  The JSON
 * properties are published to insights so changing them has a downstream impact.
 */
public class CompactionStrategyStatistics implements Serializable
{
    private static final long serialVersionUID = 3695927592357744816L;

    private final String keyspace;
    private final String table;
    private final String strategy;
    private final List<CompactionAggregateStatistics> aggregates;

    CompactionStrategyStatistics(TableMetadata metadata,
                                 String strategy,
                                 List<CompactionAggregateStatistics> aggregates)
    {
        this.keyspace = metadata.keyspace;
        this.table = metadata.name;
        this.strategy = strategy;
        this.aggregates = new ArrayList<>(aggregates);
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    @JsonProperty
    public String strategy()
    {
        return strategy;
    }

    @JsonProperty
    public List<CompactionAggregateStatistics> aggregates()
    {
        return aggregates;
    }

    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder(1024);
        ret.append(keyspace)
           .append('.')
           .append(table)
           .append('/')
           .append(strategy)
           .append('\n');

        if (!aggregates.isEmpty())
        {
            Collection<String> header = aggregates.get(0).header(); // all headers are identical
            int[] lengths = new int[header.size()]; // the max lengths of each column
            Iterator<String> it = header.iterator();

            for (int i = 0; i < lengths.length; i++)
                lengths[i] = it.next().length();

            Map<String, List<String[]>> rowsByShard = new LinkedHashMap<>();
            for (CompactionAggregateStatistics aggregate : aggregates)
            {
                String shard = aggregate.shard();
                List<String[]> rows = rowsByShard.computeIfAbsent(shard, key -> new ArrayList<>(aggregates.size()));
                String[] data = new String[header.size()];

                it = aggregate.data().iterator();
                for (int i = 0; i < lengths.length; i++)
                {
                    data[i] = it.next();
                    if (data[i].length() > lengths[i])
                        lengths[i] = data[i].length();
                }

                rows.add(data);
            }

            for (Map.Entry<String, List<String[]>> entry : rowsByShard.entrySet())
            {
                // optional shard
                if (!entry.getKey().isEmpty())
                    ret.append("Shard/").append(entry.getKey()).append('\n');

                // header
                it = header.iterator();
                for (int i = 0; i < header.size(); i++)
                    ret.append(String.format("%-" + lengths[i] + "s\t", it.next()));

                ret.append('\n');

                // rows
                for (String[] row : entry.getValue())
                {
                    for (int i = 0; i < row.length; i++)
                        ret.append(String.format("%-" + lengths[i] + "s\t", row[i]));

                    ret.append('\n');
                }

                ret.append('\n');
            }
        }

        return ret.toString();
    }

    Collection<String> getHeader()
    {
        return aggregates.isEmpty() ? ImmutableList.of() : aggregates.get(0).header();
    }

    Collection<Collection<String>> getData()
    {
        return aggregates.stream().map(CompactionAggregateStatistics::data).collect(Collectors.toList());
    }
}