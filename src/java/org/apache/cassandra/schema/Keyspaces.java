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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

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

    public static Keyspaces of(Iterable<KeyspaceMetadata> keyspaces)
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

    public KeyspaceMetadata getContainingKeyspaceMetadata(TableId tableId)
    {
        TableMetadata tableMetadata = getTableOrViewNullable(tableId);
        if (tableMetadata == null)
            throw new IllegalStateException("Can't find table " + tableId);

        return keyspaces.get(tableMetadata.keyspace);
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

    /**
     * Returns a new {@link Keyspaces} equivalent to this one, but with the provided keyspace metadata either added
     * (if this {@link Keyspaces} does not have that keyspace), or updated to the provided definition.
     *
     * <p>When the keyspace already exists, this computes the delta between the old and new {@link KeyspaceMetadata}
     * by {@link TableId} rather than removing every one of the old keyspace's tables and views and re-adding every
     * one of the new keyspace's - {@link Tables.Builder} (and the equivalent for views) stores {@link TableMetadata}
     * instances verbatim, so reference identity exactly captures "this table/view did not change" (the same argument
     * {@link KeyspacesDiff#diff} relies on). Only tables/views that were added, removed, or
     * whose instance actually changed touch the by-{@link TableId} map.
     */
    public Keyspaces withAddedOrUpdated(KeyspaceMetadata keyspace)
    {
        KeyspaceMetadata existing = getNullable(keyspace.name);
        if (existing == null)
            return with(keyspace);

        return new Keyspaces(keyspaces.withForce(keyspace.name, keyspace),
                             deltaTablesViews(tables, existing, keyspace));
    }

    /**
     * Computes {@code tables} updated from {@code before}'s set of tables/views to {@code after}'s: entries whose
     * {@link TableId} is no longer present are removed, entries whose instance differs from what is already in
     * {@code tables} are added or replaced, and everything else is left untouched.
     *
     * <p>Membership in {@code before}/{@code after} is tested against their own {@link Tables}/{@link Views}
     * (each an O(log n), non-allocating lookup) rather than by collecting ids into a new set first - the latter
     * would allocate in proportion to the keyspace size on every call, defeating the point.
     *
     * <p>An id that is new to this keyspace (not present in {@code before}) but already present in {@code tables}
     * (i.e. belongs to some other keyspace) is added with {@link BTreeMap#with}, preserving the same
     * already-exists guard {@link #withTablesViews} relies on; an id that is being replaced within this same
     * keyspace uses {@link BTreeMap#withForce} instead, since it is expected to already be present.
     */
    private static BTreeMap<TableId, TableMetadata> deltaTablesViews(BTreeMap<TableId, TableMetadata> tables,
                                                                      KeyspaceMetadata before,
                                                                      KeyspaceMetadata after)
    {
        BTreeMap<TableId, TableMetadata> tbls = tables;

        for (TableMetadata table : after.tablesAndViews())
        {
            if (tbls.get(table.id) == table)
                continue;

            boolean existedBefore = before.tables.containsTable(table.id) || containsViewId(before.views, table.id);
            tbls = existedBefore ? tbls.withForce(table.id, table) : tbls.with(table.id, table);
        }

        for (TableMetadata table : before.tablesAndViews())
        {
            boolean stillExists = after.tables.containsTable(table.id) || containsViewId(after.views, table.id);
            if (!stillExists)
                tbls = tbls.without(table.id);
        }

        return tbls;
    }

    /**
     * {@link Views} has no by-{@link TableId} index (unlike {@link Tables}), so this is a linear scan - acceptable
     * since it is only reached for entries {@link Tables#containsTable} did not already resolve, i.e. views, and a
     * keyspace's view count does not grow with the size of the schema the way its table count does.
     */
    private static boolean containsViewId(Views views, TableId id)
    {
        for (ViewMetadata view : views)
            if (view.metadata.id.equals(id))
                return true;
        return false;
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

            // Collect created and dropped keyspaces directly. filter() removes non-matching keyspaces from a copy of
            // the by-TableId map one table at a time, so building these by filtering costs one BTreeMap removal per
            // table in the cluster - on every diff, and several diffs are performed per schema change.
            List<KeyspaceMetadata> created = null;
            List<KeyspaceMetadata> dropped = null;
            ImmutableList.Builder<KeyspaceDiff> altered = ImmutableList.builder();

            for (KeyspaceMetadata keyspaceAfter : after)
            {
                if (!before.containsKeyspace(keyspaceAfter.name))
                {
                    if (created == null)
                        created = new ArrayList<>();
                    created.add(keyspaceAfter);
                }
            }

            for (KeyspaceMetadata keyspaceBefore : before)
            {
                KeyspaceMetadata keyspaceAfter = after.getNullable(keyspaceBefore.name);
                if (null == keyspaceAfter)
                {
                    if (dropped == null)
                        dropped = new ArrayList<>();
                    dropped.add(keyspaceBefore);
                }
                else if (keyspaceAfter != keyspaceBefore)
                {
                    // Identity means nothing in this keyspace changed; KeyspaceDiff.diff would reach the same
                    // conclusion, but only after walking the keyspace.
                    KeyspaceMetadata.diff(keyspaceBefore, keyspaceAfter).ifPresent(altered::add);
                }
            }

            return new KeyspacesDiff(created == null ? Keyspaces.none() : Keyspaces.of(created),
                                     dropped == null ? Keyspaces.none() : Keyspaces.of(dropped),
                                     altered.build());
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
