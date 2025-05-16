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
package org.apache.cassandra.replication;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntObjConsumer;

/**
 * A mapping of nodes to Offsets; currently we only need an internally mutable implementation.
 */
public class Node2OffsetsMap
{
    private final Int2ObjectHashMap<Offsets> map = new Int2ObjectHashMap<>();

    public void add(int node, Offsets offsets)
    {
        Offsets.Mutable current = (Offsets.Mutable) map.get(node);
        if (current != null)
            current.addAll(offsets);
        else
            map.put(node, Offsets.Mutable.copy(offsets));
    }

    public void forEach(IntObjConsumer<Offsets> consumer)
    {
        map.forEachInt(consumer);
    }

    public void clear()
    {
        map.clear();
    }
}
