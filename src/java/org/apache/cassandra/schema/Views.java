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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import static com.google.common.collect.Iterables.filter;

public final class Views implements Iterable<ViewMetadata>
{
    private final ImmutableMap<String, ViewMetadata> views;

    private Views(Builder builder)
    {
        views = builder.views.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Views none()
    {
        return builder().build();
    }

    public Iterator<ViewMetadata> iterator()
    {
        return views.values().iterator();
    }

    public Iterable<TableMetadata> metadatas()
    {
        return Iterables.transform(views.values(), view -> view.metadata);
    }

    public int size()
    {
        return views.size();
    }

    public boolean isEmpty()
    {
        return views.isEmpty();
    }

    public Iterable<ViewMetadata> forTable(UUID tableId)
    {
        return Iterables.filter(this, v -> v.baseTableId.asUUID().equals(tableId));
    }

    /**
     * Get the materialized view with the specified name
     *
     * @param name a non-qualified materialized view name
     * @return an empty {@link Optional} if the materialized view name is not found; a non-empty optional of {@link ViewMetadata} otherwise
     */
    public Optional<ViewMetadata> get(String name)
    {
        return Optional.ofNullable(views.get(name));
    }

    /**
     * Get the view with the specified name
     *
     * @param name a non-qualified view name
     * @return null if the view name is not found; the found {@link ViewMetadata} otherwise
     */
    @Nullable
    public ViewMetadata getNullable(String name)
    {
        return views.get(name);
    }

    /**
     * Create a MaterializedViews instance with the provided materialized view added
     */
    public Views with(ViewMetadata view)
    {
        if (get(view.name).isPresent())
            throw new IllegalStateException(String.format("Materialized View %s already exists", view.name));

        return builder().add(this).add(view).build();
    }

    public Views withSwapped(ViewMetadata view)
    {
        return without(view.name).with(view);
    }

    /**
     * Creates a MaterializedViews instance with the materializedView with the provided name removed
     */
    public Views without(String name)
    {
        ViewMetadata materializedView =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Materialized View %s doesn't exists", name)));

        return builder().add(filter(this, v -> v != materializedView)).build();
    }

    MapDifference<TableId, ViewMetadata> diff(Views other)
    {
        Map<TableId, ViewMetadata> thisViews = new HashMap<>();
        this.forEach(v -> thisViews.put(v.metadata.id, v));

        Map<TableId, ViewMetadata> otherViews = new HashMap<>();
        other.forEach(v -> otherViews.put(v.metadata.id, v));

        return Maps.difference(thisViews, otherViews);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Views && views.equals(((Views) o).views));
    }

    @Override
    public int hashCode()
    {
        return views.hashCode();
    }

    @Override
    public String toString()
    {
        return views.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, ViewMetadata> views = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Views build()
        {
            return new Views(this);
        }


        public Builder add(ViewMetadata view)
        {
            views.put(view.name, view);
            return this;
        }

        public Builder add(Iterable<ViewMetadata> views)
        {
            views.forEach(this::add);
            return this;
        }
    }
}