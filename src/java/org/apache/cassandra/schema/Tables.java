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

import java.nio.ByteBuffer;
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
import org.apache.cassandra.index.internal.CassandraIndex;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

/**
 * An immutable container for a keyspace's Tables.
 */
public final class Tables implements Iterable<TableMetadata>
{
    private static final Tables NONE = builder().build();

    private final ImmutableMap<String, TableMetadata> tables;
    private final ImmutableMap<TableId, TableMetadata> tablesById;
    private final ImmutableMap<String, TableMetadata> indexTables;

    private Tables(Builder builder)
    {
        tables = builder.tables.build();
        tablesById = builder.tablesById.build();
        indexTables = builder.indexTables.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Tables none()
    {
        return NONE;
    }

    public static Tables of(TableMetadata... tables)
    {
        return builder().add(tables).build();
    }

    public static Tables of(Iterable<TableMetadata> tables)
    {
        return builder().add(tables).build();
    }

    public Iterator<TableMetadata> iterator()
    {
        return tables.values().iterator();
    }

    public Stream<TableMetadata> stream()
    {
        return StreamSupport.stream(spliterator(), false);
    }

    public Iterable<TableMetadata> referencingUserType(ByteBuffer name)
    {
        return Iterables.filter(tables.values(), t -> t.referencesUserType(name));
    }

    ImmutableMap<String, TableMetadata> indexTables()
    {
        return indexTables;
    }

    public int size()
    {
        return tables.size();
    }

    /**
     * Get the table with the specified name
     *
     * @param name a non-qualified table name
     * @return an empty {@link Optional} if the table name is not found; a non-empty optional of {@link TableMetadataRef} otherwise
     */
    public Optional<TableMetadata> get(String name)
    {
        return Optional.ofNullable(tables.get(name));
    }

    /**
     * Get the table with the specified name
     *
     * @param name a non-qualified table name
     * @return null if the table name is not found; the found {@link TableMetadataRef} otherwise
     */
    @Nullable
    public TableMetadata getNullable(String name)
    {
        return tables.get(name);
    }

    @Nullable
    public TableMetadata getNullable(TableId id)
    {
        return tablesById.get(id);
    }

    boolean containsTable(TableId id)
    {
        return tablesById.containsKey(id);
    }

    public Tables filter(Predicate<TableMetadata> predicate)
    {
        Builder builder = builder();
        tables.values().stream().filter(predicate).forEach(builder::add);
        return builder.build();
    }

    /**
     * Create a Tables instance with the provided table added
     */
    public Tables with(TableMetadata table)
    {
        if (get(table.name).isPresent())
            throw new IllegalStateException(String.format("Table %s already exists", table.name));

        return builder().add(this).add(table).build();
    }

    public Tables withSwapped(TableMetadata table)
    {
        return without(table.name).with(table);
    }

    /**
     * Creates a Tables instance with the table with the provided name removed
     */
    public Tables without(String name)
    {
        TableMetadata table =
            get(name).orElseThrow(() -> new IllegalStateException(String.format("Table %s doesn't exists", name)));

        return without(table);
    }

    public Tables without(TableMetadata table)
    {
        return filter(t -> t != table);
    }

    public Tables withUpdatedUserType(UserType udt)
    {
        return any(this, t -> t.referencesUserType(udt.name))
             ? builder().add(transform(this, t -> t.withUpdatedUserType(udt))).build()
             : this;
    }

    MapDifference<String, TableMetadata> indexesDiff(Tables other)
    {
        Map<String, TableMetadata> thisIndexTables = new HashMap<>();
        this.indexTables.values().forEach(t -> thisIndexTables.put(t.indexName().get(), t));

        Map<String, TableMetadata> otherIndexTables = new HashMap<>();
        other.indexTables.values().forEach(t -> otherIndexTables.put(t.indexName().get(), t));

        return Maps.difference(thisIndexTables, otherIndexTables);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Tables && tables.equals(((Tables) o).tables));
    }

    @Override
    public int hashCode()
    {
        return tables.hashCode();
    }

    @Override
    public String toString()
    {
        return tables.values().toString();
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, TableMetadata> tables = new ImmutableMap.Builder<>();
        final ImmutableMap.Builder<TableId, TableMetadata> tablesById = new ImmutableMap.Builder<>();
        final ImmutableMap.Builder<String, TableMetadata> indexTables = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Tables build()
        {
            return new Tables(this);
        }

        public Builder add(TableMetadata table)
        {
            tables.put(table.name, table);

            tablesById.put(table.id, table);

            table.indexes
                 .stream()
                 .filter(i -> !i.isCustom())
                 .map(i -> CassandraIndex.indexCfsMetadata(table, i))
                 .forEach(i -> indexTables.put(i.indexName().get(), i));

            return this;
        }

        public Builder add(TableMetadata... tables)
        {
            for (TableMetadata table : tables)
                add(table);
            return this;
        }

        public Builder add(Iterable<TableMetadata> tables)
        {
            tables.forEach(this::add);
            return this;
        }
    }

    static TablesDiff diff(Tables before, Tables after)
    {
        return TablesDiff.diff(before, after);
    }

    public static final class TablesDiff extends Diff<Tables, TableMetadata>
    {
        private final static TablesDiff NONE = new TablesDiff(Tables.none(), Tables.none(), ImmutableList.of());

        private TablesDiff(Tables created, Tables dropped, ImmutableCollection<Altered<TableMetadata>> altered)
        {
            super(created, dropped, altered);
        }

        private static TablesDiff diff(Tables before, Tables after)
        {
            if (before == after)
                return NONE;

            Tables created = after.filter(t -> !before.containsTable(t.id));
            Tables dropped = before.filter(t -> !after.containsTable(t.id));

            ImmutableList.Builder<Altered<TableMetadata>> altered = ImmutableList.builder();
            before.forEach(tableBefore ->
            {
                TableMetadata tableAfter = after.getNullable(tableBefore.id);
                if (null != tableAfter)
                    tableBefore.compare(tableAfter).ifPresent(kind -> altered.add(new Altered<>(tableBefore, tableAfter, kind)));
            });

            return new TablesDiff(created, dropped, altered.build());
        }
    }
}
