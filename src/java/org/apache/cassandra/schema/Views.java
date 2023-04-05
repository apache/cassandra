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
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.google.common.collect.*;

import org.apache.cassandra.db.marshal.UserType;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

public final class Views implements Iterable<ViewMetadata>
{
    private static final Views NONE = builder().build();

    private final ImmutableMap<String, ViewMetadata> views;

    private Views(Builder builder)
    {
        views = ImmutableMap.copyOf(builder.views);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Builder unbuild()
    {
        return builder().put(this);
    }

    public static Views none()
    {
        return NONE;
    }

    public Iterator<ViewMetadata> iterator()
    {
        return views.values().iterator();
    }

    Iterable<TableMetadata> allTableMetadata()
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

    public Iterable<ViewMetadata> forTable(TableId tableId)
    {
        return Iterables.filter(this, v -> v.baseTableId.equals(tableId));
    }

    public Stream<ViewMetadata> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    public Stream<ViewMetadata> stream(TableId tableId)
    {
        return stream().filter(v -> v.baseTableId.equals(tableId));
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

    boolean containsView(String name)
    {
        return views.containsKey(name);
    }

    Views filter(Predicate<ViewMetadata> predicate)
    {
        Builder builder = builder();
        views.values().stream().filter(predicate).forEach(builder::put);
        return builder.build();
    }

    /**
     * Create a MaterializedViews instance with the provided materialized view added
     */
    public Views with(ViewMetadata view)
    {
        if (get(view.name()).isPresent())
            throw new IllegalStateException(String.format("Materialized View %s already exists", view.name()));

        return builder().put(this).put(view).build();
    }

    public Views withSwapped(ViewMetadata view)
    {
        return without(view.name()).with(view);
    }

    /**
     * Creates a MaterializedViews instance with the materializedView with the provided name removed
     */
    public Views without(String name)
    {
        ViewMetadata materializedView =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Materialized View %s doesn't exists", name)));

        return filter(v -> v != materializedView);
    }

    Views withUpdatedUserTypes(UserType udt)
    {
        return any(this, v -> v.referencesUserType(udt.name))
             ? builder().put(transform(this, v -> v.withUpdatedUserType(udt))).build()
             : this;
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
        final Map<String, ViewMetadata> views = new HashMap<>();

        private Builder()
        {
        }

        public Views build()
        {
            return new Views(this);
        }

        public ViewMetadata get(String name)
        {
            return views.get(name);
        }

        public Builder put(ViewMetadata view)
        {
            views.put(view.name(), view);
            return this;
        }

        public Builder remove(String name)
        {
            views.remove(name);
            return this;
        }

        public Builder put(Iterable<ViewMetadata> views)
        {
            views.forEach(this::put);
            return this;
        }
    }

    static ViewsDiff diff(Views before, Views after)
    {
        return ViewsDiff.diff(before, after);
    }

    static final class ViewsDiff extends Diff<Views, ViewMetadata>
    {
        private static final ViewsDiff NONE = new ViewsDiff(Views.none(), Views.none(), ImmutableList.of());

        private ViewsDiff(Views created, Views dropped, ImmutableCollection<Altered<ViewMetadata>> altered)
        {
            super(created, dropped, altered);
        }

        private static ViewsDiff diff(Views before, Views after)
        {
            if (before == after)
                return NONE;

            Views created = after.filter(v -> !before.containsView(v.name()));
            Views dropped = before.filter(v -> !after.containsView(v.name()));

            ImmutableList.Builder<Altered<ViewMetadata>> altered = ImmutableList.builder();
            before.forEach(viewBefore ->
            {
                ViewMetadata viewAfter = after.getNullable(viewBefore.name());
                if (null != viewAfter)
                    viewBefore.compare(viewAfter).ifPresent(kind -> altered.add(new Altered<>(viewBefore, viewAfter, kind)));
            });

            return new ViewsDiff(created, dropped, altered.build());
        }
    }
}
