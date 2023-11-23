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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.*;

import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.btree.BTreeMap;

public final class Keyspaces implements Iterable<KeyspaceMetadata>
{
    public static final Keyspaces NONE = new Keyspaces(BTreeMap.empty(), BTreeMap.empty());

    private final BTreeMap<String, KeyspaceMetadata> keyspaces;
    private final BTreeMap<TableId, TableMetadata> tables;

    private Keyspaces(BTreeMap<String, KeyspaceMetadata> keyspaces,
                      BTreeMap<TableId, TableMetadata> tables)
    {
        this.keyspaces = keyspaces;
        this.tables = tables;
    }

    public static Keyspaces none()
    {
        return NONE;
    }

    public static Keyspaces of(KeyspaceMetadata... keyspaces)
    {
        BTreeMap<String, KeyspaceMetadata> newKeyspaces = BTreeMap.empty();
        BTreeMap<TableId, TableMetadata> newTables = BTreeMap.empty();
        for (KeyspaceMetadata ks : keyspaces)
        {
            newKeyspaces = newKeyspaces.with(ks.name, ks);
            newTables = withTablesViews(newTables, ks);
        }
        return new Keyspaces(newKeyspaces, newTables);
    }

    public Keyspaces with(KeyspaceMetadata ks)
    {
        assert !keyspaces.containsKey(ks.name) : "Keyspace already exists: "+ks.name;
        return new Keyspaces(keyspaces.with(ks.name, ks), withTablesViews(tables, ks));
    }

    public Keyspaces with(Iterable<KeyspaceMetadata> kss)
    {
        Keyspaces k = this;
        for (KeyspaceMetadata ks : kss)
            k = k.with(ks);
        return k;
    }

    public Iterator<KeyspaceMetadata> iterator()
    {
        return keyspaces.values().iterator();
    }

    public Stream<KeyspaceMetadata> stream()
    {
        return keyspaces.values().stream();
    }

    public Set<String> names()
    {
        return keyspaces.keySet();
    }

    /**
     * Get the keyspace with the specified name
     *
     * @param name a non-qualified keyspace name
     * @return an empty {@link Optional} if the table name is not found; a non-empty optional of {@link KeyspaceMetadata} otherwise
     */
    public Optional<KeyspaceMetadata> get(String name)
    {
        return Optional.ofNullable(keyspaces.get(name));
    }

    @Nullable
    public KeyspaceMetadata getNullable(String name)
    {
        return keyspaces.get(name);
    }

    public boolean containsKeyspace(String name)
    {
        return keyspaces.containsKey(name);
    }

    @Nullable
    public TableMetadata getTableOrViewNullable(TableId id)
    {
        return tables.get(id);
    }

    public boolean isEmpty()
    {
        return keyspaces.isEmpty();
    }

    public Keyspaces filter(Predicate<KeyspaceMetadata> predicate)
    {
        BTreeMap<String, KeyspaceMetadata> kss = keyspaces;
        BTreeMap<TableId, TableMetadata> tbls = tables;
        // todo: bulk removals from BTreeMap
        for (Map.Entry<String, KeyspaceMetadata> entry : keyspaces.entrySet())
        {
            if (!predicate.test(entry.getValue()))
            {
                kss = kss.without(entry.getKey());
                tbls = withoutKsTablesViews(tbls, entry.getValue());
            }
        }

        return new Keyspaces(kss, tbls);
    }

    /**
     * Creates a Keyspaces instance with the keyspace with the provided name removed
     */
    public Keyspaces without(String name)
    {
        KeyspaceMetadata keyspace = getNullable(name);
        if (keyspace == null)
            throw new IllegalStateException(String.format("Keyspace %s doesn't exists", name));

        return filter(k -> k != keyspace);
    }

    public Keyspaces without(Collection<String> names)
    {
        return filter(k -> !names.contains(k.name));
    }

    public Keyspaces withAddedOrUpdated(KeyspaceMetadata keyspace)
    {
        Keyspaces updated = keyspaces.containsKey(keyspace.name) ? without(keyspace.name) : this;
        return updated.with(keyspace);
    }

    private static BTreeMap<TableId, TableMetadata> withoutKsTablesViews(BTreeMap<TableId, TableMetadata> tables, KeyspaceMetadata ks)
    {
        // todo: bulk ops
        BTreeMap<TableId, TableMetadata> tbls = tables;
        for (TableMetadata table : ks.tables)
            tbls = tbls.without(table.id);
        for (ViewMetadata view : ks.views)
            tbls = tbls.without(view.metadata.id);
        return tbls;
    }

    private static BTreeMap<TableId, TableMetadata> withTablesViews(BTreeMap<TableId, TableMetadata> tables, KeyspaceMetadata ks)
    {
        BTreeMap<TableId, TableMetadata> tbls = tables;
        for (TableMetadata table : ks.tables)
            tbls = tbls.with(table.id, table);
        for (ViewMetadata view : ks.views)
            tbls = tbls.with(view.metadata.id, view.metadata);
        return tbls;
    }

    /**
     * Returns a new {@link Keyspaces} equivalent to this one, but with the provided keyspace metadata either added (if
     * this {@link Keyspaces} does not have that keyspace), or replaced by the provided definition.
     *
     * <p>Note that if this contains the provided keyspace, its pre-existing definition is discarded and completely
     * replaced with the newly provided one. See {@link #withAddedOrUpdated(KeyspaceMetadata)} if you wish the provided
     * definition to be "merged" with the existing one instead.
     *
     * @param keyspace the keyspace metadata to add, or replace the existing definition with.
     * @return the newly created object.
     */
    public Keyspaces withAddedOrReplaced(KeyspaceMetadata keyspace)
    {
        return filter(ksm -> !ksm.name.equals(keyspace.name)).with(keyspace);
    }

    /**
     * Calls {@link #withAddedOrReplaced(KeyspaceMetadata)} on all the keyspaces of the provided {@link Keyspaces}.
     *
     * @param keyspaces the keyspaces to add, or replace if existing.
     * @return the newly created object.
     */
    public Keyspaces withAddedOrReplaced(Keyspaces keyspaces)
    {
        Keyspaces kss = this;

        for (KeyspaceMetadata ksm : keyspaces)
            kss = kss.withAddedOrReplaced(ksm);
        return kss;
    }

    public void validate()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        keyspaces.values().forEach((ksm) -> ksm.validate(metadata));
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Keyspaces && keyspaces.equals(((Keyspaces) o).keyspaces));
    }

    @Override
    public int hashCode()
    {
        return keyspaces.hashCode();
    }

    @Override
    public String toString()
    {
        return keyspaces.values().toString();
    }

    public int size()
    {
        return keyspaces.size();
    }

    public static KeyspacesDiff diff(Keyspaces before, Keyspaces after)
    {
        return KeyspacesDiff.diff(before, after);
    }

    public static final class KeyspacesDiff
    {
        public static final KeyspacesDiff NONE = new KeyspacesDiff(Keyspaces.none(), Keyspaces.none(), ImmutableList.of());

        public final Keyspaces created;
        public final Keyspaces dropped;
        public final ImmutableList<KeyspaceDiff> altered;

        private KeyspacesDiff(Keyspaces created, Keyspaces dropped, ImmutableList<KeyspaceDiff> altered)
        {
            this.created = created;
            this.dropped = dropped;
            this.altered = altered;
        }

        private static KeyspacesDiff diff(Keyspaces before, Keyspaces after)
        {
            if (before == after)
                return NONE;

            Keyspaces created = after.filter(k -> !before.containsKeyspace(k.name));
            Keyspaces dropped = before.filter(k -> !after.containsKeyspace(k.name));

            ImmutableList.Builder<KeyspaceDiff> altered = ImmutableList.builder();
            before.forEach(keyspaceBefore ->
            {
                KeyspaceMetadata keyspaceAfter = after.getNullable(keyspaceBefore.name);
                if (null != keyspaceAfter)
                    KeyspaceMetadata.diff(keyspaceBefore, keyspaceAfter).ifPresent(altered::add);
            });

            return new KeyspacesDiff(created, dropped, altered.build());
        }

        public boolean isEmpty()
        {
            return created.isEmpty() && dropped.isEmpty() && altered.isEmpty();
        }

        @Override
        public String toString()
        {
            return "KeyspacesDiff{" +
                   "created=" + created +
                   ", dropped=" + dropped +
                   ", altered=" + altered +
                   '}';
        }
    }
}
