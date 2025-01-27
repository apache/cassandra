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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import accord.local.Node;
import accord.topology.Shard;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;

public class SimpleFastPathStrategy implements FastPathStrategy
{
    public static final SimpleFastPathStrategy instance = new SimpleFastPathStrategy();

    private static final Map<String, String> SCHEMA_PARAMS = ImmutableMap.of(Kind.KEY, Kind.SIMPLE.name());

    private SimpleFastPathStrategy() {}

    @Override
    public SortedArrayList<Node.Id> calculateFastPath(SortedArrayList<Node.Id> nodes, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap)
    {
        int maxFailures = Shard.maxToleratedFailures(nodes.size());
        int discarded = 0;

        if (unavailable.isEmpty())
            return nodes;

        Object[] tmp = ArrayBuffers.cachedAny().get(nodes.size());
        for (int i=0,mi=nodes.size(); i<mi; i++)
        {
            Node.Id node = nodes.get(i);
            if (unavailable.contains(node) && discarded < maxFailures)
                discarded++;
            else
                tmp[i - discarded] = node;
        }

        Node.Id[] array = new Node.Id[nodes.size() - discarded];
        System.arraycopy(tmp, 0, array, 0, nodes.size() - discarded);
        SortedArrayList<Node.Id> fastPath = new SortedArrayList<>(array);
        Invariants.require(fastPath.size() >= Shard.slowQuorumSize(nodes.size()));
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
