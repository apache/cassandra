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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.UDTAndFunctionsAwareMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.btree.BTreeMap;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

/**
 * An immutable container for a keyspace's Tables.
 */
public final class Tables implements Iterable<TableMetadata>
{
    public static final Serializer serializer = new Serializer();

    private static final Tables NONE = builder().build();

    private final BTreeMap<String, TableMetadata> tables;
    private final BTreeMap<TableId, TableMetadata> tablesById;
    private final BTreeMap<String, TableMetadata> indexTables;

    private Tables(Builder builder)
    {
        this(builder.tables, builder.tablesById, builder.indexTables);
    }

    private Tables(BTreeMap<String, TableMetadata> tables,
                   BTreeMap<TableId, TableMetadata> tablesById,
                   BTreeMap<String, TableMetadata> indexTables)
    {
        this.tables = tables;
        this.tablesById = tablesById;
        this.indexTables = indexTables;
    }

    /**
     * Index tables are derived from their base table rather than stored independently, so they are added and removed
     * alongside it. The key is the index name: {@link TableMetadata#indexTableName} builds the index table's name by
     * appending it to the base table's, and {@link TableMetadata#indexName()} strips it back off again. Keying off
     * {@link IndexMetadata#name} directly is therefore the same key without building the metadata to derive it, so
     * removal costs a lookup per index. Either direction is bounded by the indexes on the one table being changed
     * rather than by the size of the collection.
     */
    private static BTreeMap<String, TableMetadata> withIndexesOf(BTreeMap<String, TableMetadata> indexTables, TableMetadata table)
    {
        for (IndexMetadata index : table.indexes)
        {
            if (index.isCustom())
                continue;
            indexTables = indexTables.with(index.name, CassandraIndex.indexCfsMetadata(table, index));
        }
        return indexTables;
    }

    private static BTreeMap<String, TableMetadata> withoutIndexesOf(BTreeMap<String, TableMetadata> indexTables, TableMetadata table)
    {
        for (IndexMetadata index : table.indexes)
        {
            if (index.isCustom())
                continue;
            indexTables = indexTables.without(index.name);
        }
        return indexTables;
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

    Map<String, TableMetadata> indexTables()
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

        return new Tables(tables.with(table.name, table),
                          tablesById.with(table.id, table),
                          withIndexesOf(indexTables, table));
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
        return new Tables(tables.without(table.name),
                          tablesById.without(table.id),
                          withoutIndexesOf(indexTables, table));
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
        BTreeMap<String, TableMetadata> tables = BTreeMap.empty();
        BTreeMap<TableId, TableMetadata> tablesById = BTreeMap.empty();
        BTreeMap<String, TableMetadata> indexTables = BTreeMap.empty();

        private Builder()
        {
        }

        public Tables build()
        {
            return new Tables(this);
        }

        public Builder add(TableMetadata table)
        {
            // ImmutableMap.Builder rejected duplicates when it was built; a persistent map would silently overwrite,
            // so the check is made explicit rather than dropped.
            if (tables.containsKey(table.name))
                throw new IllegalArgumentException(String.format("Table %s already exists", table.name));

            tables = tables.with(table.name, table);
            tablesById = tablesById.with(table.id, table);
            indexTables = withIndexesOf(indexTables, table);

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

            // Collect the differences directly instead of filtering whole collections: a schema change touches a
            // handful of tables, so allocating only for those keeps the cost of a diff proportional to what actually
            // changed rather than to the size of the schema.
            Builder created = null;
            Builder dropped = null;
            ImmutableList.Builder<Altered<TableMetadata>> altered = ImmutableList.builder();

            for (TableMetadata tableAfter : after)
            {
                if (!before.containsTable(tableAfter.id))
                {
                    if (created == null)
                        created = builder();
                    created.add(tableAfter);
                }
            }

            for (TableMetadata tableBefore : before)
            {
                TableMetadata tableAfter = after.getNullable(tableBefore.id);
                if (null == tableAfter)
                {
                    if (dropped == null)
                        dropped = builder();
                    dropped.add(tableBefore);
                }
                else if (tableAfter != tableBefore)
                {
                    // Untouched tables are carried over by reference (Builder.add stores the instance verbatim), and
                    // compare() of an instance against itself is empty by construction, so identity is a sound and
                    // exact substitute for the comparison here.
                    tableBefore.compare(tableAfter).ifPresent(kind -> altered.add(new Altered<>(tableBefore, tableAfter, kind)));
                }
            }

            ImmutableList<Altered<TableMetadata>> alteredTables = altered.build();
            if (created == null && dropped == null && alteredTables.isEmpty())
                return NONE;

            return new TablesDiff(created == null ? Tables.none() : created.build(),
                                  dropped == null ? Tables.none() : dropped.build(),
                                  alteredTables);
        }
    }

    public static class Serializer implements UDTAndFunctionsAwareMetadataSerializer<Tables>
    {
        public void serialize(Tables t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.tables.size());
            for (TableMetadata tm : t.tables.values())
                TableMetadata.serializer.serialize(tm, out, version);
        }

        public Tables deserialize(DataInputPlus in, Types types, UserFunctions functions, Version version) throws IOException
        {
            int count = in.readInt();
            Tables.Builder builder = Tables.builder();
            for (int i = 0; i < count; i++)
            {
                TableMetadata tm = TableMetadata.serializer.deserialize(in, types, functions, version);
                builder.add(tm);
            }
            return builder.build();
        }

        public long serializedSize(Tables t, Version version)
        {
            int size = TypeSizes.sizeof(t.tables.size());
            for (TableMetadata tm : t.tables.values())
                size += TableMetadata.serializer.serializedSize(tm, version);
            return size;
        }
    }
}
