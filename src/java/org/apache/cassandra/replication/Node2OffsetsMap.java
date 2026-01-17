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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntObjConsumer;

/**
 * A mapping of nodes to Offsets; currently we only need an internally mutable implementation.
 */
public class Node2OffsetsMap
{
    private final Int2ObjectHashMap<Offsets.Mutable> offsetsMap;

    public Node2OffsetsMap()
    {
        offsetsMap = new Int2ObjectHashMap<>(8, 0.65f, false);
    }

    public static Node2OffsetsMap forParticipants(CoordinatorLogId logId, Participants participants)
    {
        Node2OffsetsMap map = new Node2OffsetsMap();
        for (int i = 0; i < participants.size(); i++)
            map.set(participants.get(i), new Offsets.Mutable(logId));
        return map;
    }

    void set(int node, Offsets.Mutable offsets)
    {
        offsetsMap.put(node, offsets);
    }

    @Nonnull
    Offsets.Mutable get(int node)
    {
        return Preconditions.checkNotNull(offsetsMap.get(node));
    }

    Offsets.Mutable intersection()
    {
        Iterator<Offsets.Mutable> iter = offsetsMap.values().iterator();

        Preconditions.checkArgument(iter.hasNext());
        if (offsetsMap.size() == 1)
            return Offsets.Mutable.copy(iter.next());

        Offsets.Mutable intersection = iter.next();
        while (iter.hasNext())
            intersection = Offsets.Mutable.intersection(intersection, iter.next());
        return intersection;
    }

    Offsets.Mutable union()
    {
        if (offsetsMap.isEmpty())
            throw new IllegalStateException("Cannot compute union of empty offsets map");

        Iterator<Offsets.Mutable> iter = offsetsMap.values().iterator();
        if (offsetsMap.size() == 1)
            return Offsets.Mutable.copy(iter.next());

        Offsets.Mutable union = Offsets.Mutable.copy(iter.next());
        while (iter.hasNext())
            union.addAll(iter.next());

        return union;
    }

    public void add(int node, Offsets offsets)
    {
        Offsets.Mutable current = offsetsMap.get(node);
        if (current != null)
            current.addAll(offsets);
        else
            offsetsMap.put(node, Offsets.Mutable.copy(offsets));
    }

    public void forEach(IntObjConsumer<Offsets.Mutable> consumer)
    {
        offsetsMap.forEachInt(consumer);
    }

    public void clear()
    {
        offsetsMap.clear();
    }

    public int size()
    {
        return offsetsMap.size();
    }

    void convertToPrimitiveMap(Map<Integer, List<Integer>> into)
    {
        for (Int2ObjectHashMap<Offsets.Mutable>.EntryIterator iter = offsetsMap.entrySet().iterator(); iter.hasNext();)
        {
            iter.next();
            into.put(iter.getIntKey(), iter.getValue().asList());
        }
    }

    static Node2OffsetsMap fromPrimitiveMap(CoordinatorLogId logId, Map<Integer, List<Integer>> from)
    {
        Node2OffsetsMap map = new Node2OffsetsMap();
        for (Map.Entry<Integer, List<Integer>> entry : from.entrySet())
            map.set(entry.getKey(), Offsets.fromList(logId, entry.getValue()));
        return map;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Node2OffsetsMap))
            return false;
        Node2OffsetsMap that = (Node2OffsetsMap) o;
        return this.offsetsMap.equals(that.offsetsMap);
    }

    @Override
    public String toString()
    {
        return "Node2OffsetsMap{" +
               "offsetsMap=" + offsetsMap +
               '}';
    }
}
