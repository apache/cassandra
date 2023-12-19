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

package org.apache.cassandra.service.accord.fastpath;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import accord.local.Node;
import accord.topology.Shard;
import accord.utils.Invariants;

public class SimpleFastPathStrategy implements FastPathStrategy
{
    static final SimpleFastPathStrategy instance = new SimpleFastPathStrategy();

    private static final Map<String, String> SCHEMA_PARAMS = ImmutableMap.of(Kind.KEY, Kind.SIMPLE.name());

    private SimpleFastPathStrategy() {}

    @Override
    public Set<Node.Id> calculateFastPath(List<Node.Id> nodes, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap)
    {
        int maxFailures = Shard.maxToleratedFailures(nodes.size());
        int discarded = 0;

        ImmutableSet.Builder<Node.Id> builder = ImmutableSet.builder();

        for (int i=0,mi=nodes.size(); i<mi; i++)
        {
            Node.Id node = nodes.get(i);
            if (unavailable.contains(node) && discarded < maxFailures)
            {
                discarded++;
                continue;
            }

            builder.add(node);
        }

        Set<Node.Id> fastPath = builder.build();
        Invariants.checkState(fastPath.size() >= Shard.slowPathQuorumSize(nodes.size()));
        return fastPath;
    }

    @Override
    public Kind kind()
    {
        return Kind.SIMPLE;
    }

    @Override
    public String toString()
    {
        return "simple";
    }

    public Map<String, String> asMap()
    {
        return SCHEMA_PARAMS;
    }

    @Override
    public String asCQL()
    {
        return "'simple'";
    }
}
