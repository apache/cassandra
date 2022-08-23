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
package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

/**
 * A directed graph. Main usage is the {@link #findPaths(Object, Object)} method which is used to find all paths between
 * 2 vertices.
 */
public class SimpleGraph<V>
{
    private final ImmutableMap<V, ImmutableSet<V>> edges;

    private SimpleGraph(ImmutableMap<V, ImmutableSet<V>> edges)
    {
        if (edges == null || edges.isEmpty())
            throw new AssertionError("Edges empty");
        this.edges = edges;
    }

    public static <T extends Comparable<T>> NavigableSet<T> sortedVertices(SimpleGraph<T> graph)
    {
        return new TreeSet<>(graph.vertices());
    }

    public static <T extends Comparable<T>> T min(SimpleGraph<T> graph)
    {
        return Ordering.natural().min(graph.vertices());
    }

    public static <T extends Comparable<T>> T max(SimpleGraph<T> graph)
    {
        return Ordering.natural().max(graph.vertices());
    }

    public boolean hasEdge(V a, V b)
    {
        ImmutableSet<V> matches = edges.get(a);
        return matches != null && matches.contains(b);
    }

    public ImmutableSet<V> vertices()
    {
        ImmutableSet.Builder<V> b = ImmutableSet.builder();
        b.addAll(edges.keySet());
        edges.values().forEach(b::addAll);
        return b.build();
    }

    public List<List<V>> findPaths(V from, V to)
    {
        List<List<V>> matches = new ArrayList<>();
        findPaths0(Collections.singletonList(from), from, to, matches::add);
        return matches;
    }

    private void findPaths0(List<V> accum, V from, V to, Consumer<List<V>> onMatch)
    {
        ImmutableSet<V> check = edges.get(from);
        if (check == null)
            return; // no matches
        for (V next : check)
        {
            if (accum.contains(next))
                return; // ignore walking recursive
            List<V> nextAccum = new ArrayList<>(accum);
            nextAccum.add(next);
            if (next.equals(to))
            {
                onMatch.accept(nextAccum);
            }
            else
            {
                findPaths0(nextAccum, next, to, onMatch);
            }
        }
    }

    public static class Builder<V>
    {
        private final Map<V, Set<V>> edges = new HashMap<>();

        public Builder<V> addEdge(V from, V to)
        {
            edges.computeIfAbsent(from, ignore -> new HashSet<>()).add(to);
            return this;
        }

        public SimpleGraph<V> build()
        {
            ImmutableMap.Builder<V, ImmutableSet<V>> builder = ImmutableMap.builder();
            for (Map.Entry<V, Set<V>> e : edges.entrySet())
                builder.put(e.getKey(), ImmutableSet.copyOf(e.getValue()));
            return new SimpleGraph(builder.build());
        }
    }
}
