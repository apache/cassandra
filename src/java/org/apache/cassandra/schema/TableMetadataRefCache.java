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

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.cassandra.utils.Pair;

/**
 * Manages the cached {@link TableMetadataRef} objects which holds the references to {@link TableMetadata} objects.
 * <p>
 * The purpose of {@link TableMetadataRef} is that the reference to {@link TableMetadataRef} remains unchanged when
 * the metadata of the table changes. {@link TableMetadata} is immutable, so when it changes, we only switch
 * the reference inside the existing {@link TableMetadataRef} object.
 */
class TableMetadataRefCache
{
    public final static TableMetadataRefCache EMPTY = new TableMetadataRefCache(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

    // UUID -> mutable metadata ref map. We have to update these in place every time a table changes.
    private final Map<TableId, TableMetadataRef> metadataRefs;

    // keyspace and table names -> mutable metadata ref map.
    private final Map<Pair<String, String>, TableMetadataRef> metadataRefsByName;

    // (keyspace name, index name) -> mutable metadata ref map. We have to update these in place every time an index changes.
    private final Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs;

    public TableMetadataRefCache(Map<TableId, TableMetadataRef> metadataRefs,
                                 Map<Pair<String, String>, TableMetadataRef> metadataRefsByName,
                                 Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs)
    {
        this.metadataRefs = Collections.unmodifiableMap(metadataRefs);
        this.metadataRefsByName = Collections.unmodifiableMap(metadataRefsByName);
        this.indexMetadataRefs = Collections.unmodifiableMap(indexMetadataRefs);
    }

    /**
     * Returns cache copy with added the {@link TableMetadataRef} corresponding to the provided keyspace to {@link #metadataRefs} and
     * {@link #indexMetadataRefs}, assuming the keyspace is new (in the sense of not being tracked by the manager yet).
     */
    TableMetadataRefCache withNewRefs(KeyspaceMetadata ksm)
    {
        return withUpdatedRefs(ksm.empty(), ksm);
    }

    /**
     * Returns cache copy with updated the {@link TableMetadataRef} in {@link #metadataRefs} and {@link #indexMetadataRefs},
     * for an existing updated keyspace given it's previous and new definition.
     * <p>
     * Note that {@link TableMetadataRef} are not duplicated and table metadata is altered in the existing refs.
     */
    TableMetadataRefCache withUpdatedRefs(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Tables.TablesDiff tablesDiff = Tables.diff(previous.tables, updated.tables);
        Views.ViewsDiff viewsDiff = Views.diff(previous.views, updated.views);

        MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);

        boolean hasCreatedOrDroppedTablesOrViews = tablesDiff.created.size() > 0 || tablesDiff.dropped.size() > 0 || viewsDiff.created.size() > 0 || viewsDiff.dropped.size() > 0;
        boolean hasCreatedOrDroppedIndexes = !indexesDiff.entriesOnlyOnRight().isEmpty() || !indexesDiff.entriesOnlyOnLeft().isEmpty();

        Map<TableId, TableMetadataRef> metadataRefs = hasCreatedOrDroppedTablesOrViews ? Maps.newHashMap(this.metadataRefs) : this.metadataRefs;
        Map<Pair<String, String>, TableMetadataRef> metadataRefsByName = hasCreatedOrDroppedTablesOrViews ? Maps.newHashMap(this.metadataRefsByName) : this.metadataRefsByName;
        Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs = hasCreatedOrDroppedIndexes ? Maps.newHashMap(this.indexMetadataRefs) : this.indexMetadataRefs;

        // clean up after removed entries
        tablesDiff.dropped.forEach(ref -> removeRef(metadataRefs, metadataRefsByName, ref));
        viewsDiff.dropped.forEach(view -> removeRef(metadataRefs, metadataRefsByName, view.metadata));
        indexesDiff.entriesOnlyOnLeft()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.remove(Pair.create(indexTable.keyspace, indexTable.indexName().get())));

        // load up new entries
        tablesDiff.created.forEach(table -> putRef(metadataRefs, metadataRefsByName, new TableMetadataRef(table)));
        viewsDiff.created.forEach(view -> putRef(metadataRefs, metadataRefsByName, new TableMetadataRef(view.metadata)));
        indexesDiff.entriesOnlyOnRight()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.put(Pair.create(indexTable.keyspace, indexTable.indexName().get()), new TableMetadataRef(indexTable)));

        // refresh refs to updated ones
        tablesDiff.altered.forEach(diff -> metadataRefs.get(diff.after.id).set(diff.after));
        viewsDiff.altered.forEach(diff -> metadataRefs.get(diff.after.metadata.id).set(diff.after.metadata));
        indexesDiff.entriesDiffering()
                   .values()
                   .stream()
                   .map(MapDifference.ValueDifference::rightValue)
                   .forEach(indexTable -> indexMetadataRefs.get(Pair.create(indexTable.keyspace, indexTable.indexName().get())).set(indexTable));

        return new TableMetadataRefCache(metadataRefs, metadataRefsByName, indexMetadataRefs);
    }

    private void putRef(Map<TableId, TableMetadataRef> metadataRefs,
                        Map<Pair<String, String>, TableMetadataRef> metadataRefsByName,
                        TableMetadataRef ref)
    {
        metadataRefs.put(ref.id, ref);
        metadataRefsByName.put(Pair.create(ref.keyspace, ref.name), ref);
    }

    private void removeRef(Map<TableId, TableMetadataRef> metadataRefs,
                           Map<Pair<String, String>, TableMetadataRef> metadataRefsByName,
                           TableMetadata tm)
    {
        metadataRefs.remove(tm.id);
        metadataRefsByName.remove(Pair.create(tm.keyspace, tm.name));
    }

    /**
     * Returns cache copy with removed the {@link TableMetadataRef} from {@link #metadataRefs} and {@link #indexMetadataRefs}
     * for the provided (dropped) keyspace.
     */
    TableMetadataRefCache withRemovedRefs(KeyspaceMetadata ksm)
    {
        return withUpdatedRefs(ksm, ksm.empty());
    }

    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        return metadataRefs.get(id);
    }

    public TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        return metadataRefsByName.get(Pair.create(keyspace, table));
    }

    public TableMetadataRef getIndexTableMetadataRef(String keyspace, String index)
    {
        return indexMetadataRefs.get(Pair.create(keyspace, index));
    }
}
