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


import java.util.Iterator;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ViewDefinition;

import static com.google.common.collect.Iterables.filter;

public final class Views implements Iterable<ViewDefinition>
{
    private final ImmutableMap<String, ViewDefinition> views;

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

    public Iterator<ViewDefinition> iterator()
    {
        return views.values().iterator();
    }

    public Iterable<CFMetaData> metadatas()
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

    /**
     * Get the materialized view with the specified name
     *
     * @param name a non-qualified materialized view name
     * @return an empty {@link Optional} if the materialized view name is not found; a non-empty optional of {@link ViewDefinition} otherwise
     */
    public Optional<ViewDefinition> get(String name)
    {
        return Optional.ofNullable(views.get(name));
    }

    /**
     * Get the view with the specified name
     *
     * @param name a non-qualified view name
     * @return null if the view name is not found; the found {@link ViewDefinition} otherwise
     */
    @Nullable
    public ViewDefinition getNullable(String name)
    {
        return views.get(name);
    }

    /**
     * Create a MaterializedViews instance with the provided materialized view added
     */
    public Views with(ViewDefinition view)
    {
        if (get(view.viewName).isPresent())
            throw new IllegalStateException(String.format("Materialized View %s already exists", view.viewName));

        return builder().add(this).add(view).build();
    }

    /**
     * Creates a MaterializedViews instance with the materializedView with the provided name removed
     */
    public Views without(String name)
    {
        ViewDefinition materializedView =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Materialized View %s doesn't exists", name)));

        return builder().add(filter(this, v -> v != materializedView)).build();
    }

    /**
     * Creates a MaterializedViews instance which contains an updated materialized view
     */
    public Views replace(ViewDefinition view, CFMetaData cfm)
    {
        return without(view.viewName).with(view);
    }

    MapDifference<String, ViewDefinition> diff(Views other)
    {
        return Maps.difference(views, other.views);
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
        final ImmutableMap.Builder<String, ViewDefinition> views = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Views build()
        {
            return new Views(this);
        }


        public Builder add(ViewDefinition view)
        {
            views.put(view.viewName, view);
            return this;
        }

        public Builder add(Iterable<ViewDefinition> views)
        {
            views.forEach(this::add);
            return this;
        }
    }
}