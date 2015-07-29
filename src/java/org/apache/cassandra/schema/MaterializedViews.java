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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.MaterializedViewDefinition;

import static com.google.common.collect.Iterables.filter;

public final class MaterializedViews implements Iterable<MaterializedViewDefinition>
{
    private final ImmutableMap<String, MaterializedViewDefinition> materializedViews;

    private MaterializedViews(Builder builder)
    {
        materializedViews = builder.materializedViews.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static MaterializedViews none()
    {
        return builder().build();
    }

    public Iterator<MaterializedViewDefinition> iterator()
    {
        return materializedViews.values().iterator();
    }

    public int size()
    {
        return materializedViews.size();
    }

    public boolean isEmpty()
    {
        return materializedViews.isEmpty();
    }

    /**
     * Get the materialized view with the specified name
     *
     * @param name a non-qualified materialized view name
     * @return an empty {@link Optional} if the materialized view name is not found; a non-empty optional of {@link MaterializedViewDefinition} otherwise
     */
    public Optional<MaterializedViewDefinition> get(String name)
    {
        return Optional.ofNullable(materializedViews.get(name));
    }

    /**
     * Create a MaterializedViews instance with the provided materialized view added
     */
    public MaterializedViews with(MaterializedViewDefinition materializedView)
    {
        if (get(materializedView.viewName).isPresent())
            throw new IllegalStateException(String.format("Materialized View %s already exists", materializedView.viewName));

        return builder().add(this).add(materializedView).build();
    }

    /**
     * Creates a MaterializedViews instance with the materializedView with the provided name removed
     */
    public MaterializedViews without(String name)
    {
        MaterializedViewDefinition materializedView =
        get(name).orElseThrow(() -> new IllegalStateException(String.format("Materialized View %s doesn't exists", name)));

        return builder().add(filter(this, v -> v != materializedView)).build();
    }

    /**
     * Creates a MaterializedViews instance which contains an updated materialized view
     */
    public MaterializedViews replace(MaterializedViewDefinition materializedView)
    {
        return without(materializedView.viewName).with(materializedView);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof MaterializedViews && materializedViews.equals(((MaterializedViews) o).materializedViews));
    }

    @Override
    public int hashCode()
    {
        return materializedViews.hashCode();
    }

    @Override
    public String toString()
    {
        return materializedViews.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, MaterializedViewDefinition> materializedViews = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public MaterializedViews build()
        {
            return new MaterializedViews(this);
        }

        public Builder add(MaterializedViewDefinition materializedView)
        {
            materializedViews.put(materializedView.viewName, materializedView);
            return this;
        }

        public Builder add(Iterable<MaterializedViewDefinition> materializedViews)
        {
            materializedViews.forEach(this::add);
            return this;
        }
    }
}