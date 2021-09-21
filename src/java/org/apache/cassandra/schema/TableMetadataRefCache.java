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

import java.util.Map;

import com.google.common.collect.MapDifference;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

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
    // UUID -> mutable metadata ref map. We have to update these in place every time a table changes.
    private final Map<TableId, TableMetadataRef> metadataRefs = new NonBlockingHashMap<>();

    // keyspace and table names -> mutable metadata ref map.
    private final Map<Pair<String, String>, TableMetadataRef> metadataRefsByName = new NonBlockingHashMap<>();

    // (keyspace name, index name) -> mutable metadata ref map. We have to update these in place every time an index changes.
    private final Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs = new NonBlockingHashMap<>();

    /**
     * Adds the {@link TableMetadataRef} corresponding to the provided keyspace to {@link #metadataRefs} and
     * {@link #indexMetadataRefs}, assuming the keyspace is new (in the sense of not being tracked by the manager yet).
     */
    synchronized void addNewRefs(KeyspaceMetadata ksm)
    {
        ksm.tablesAndViews()
           .forEach(metadata -> putRef(new TableMetadataRef(metadata)));

        ksm.tables.indexTables()
                  .forEach((name, metadata) -> indexMetadataRefs.put(Pair.create(ksm.name, name), new TableMetadataRef(metadata)));
    }

    /**
     * Updates the {@link TableMetadataRef} in {@link #metadataRefs} and {@link #indexMetadataRefs}, for an
     * existing updated keyspace given it's previous and new definition.
     */
    synchronized void updateRefs(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Tables.TablesDiff tablesDiff = Tables.diff(previous.tables, updated.tables);
        Views.ViewsDiff viewsDiff = Views.diff(previous.views, updated.views);

        MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);

        // clean up after removed entries
        tablesDiff.dropped.forEach(this::removeRef);
        viewsDiff.dropped.forEach(view -> removeRef(view.metadata));

        indexesDiff.entriesOnlyOnLeft()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.remove(Pair.create(indexTable.keyspace, indexTable.indexName().get())));

        // load up new entries
        tablesDiff.created.forEach(table -> putRef(new TableMetadataRef(table)));
        viewsDiff.created.forEach(view -> putRef(new TableMetadataRef(view.metadata)));

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
    }

    private void putRef(TableMetadataRef ref)
    {
        metadataRefs.put(ref.id, ref);
        metadataRefsByName.put(Pair.create(ref.keyspace, ref.name), ref);
    }

    private void removeRef(TableMetadata tm)
    {
        metadataRefs.remove(tm.id);
        metadataRefsByName.remove(Pair.create(tm.keyspace, tm.name));
    }

    /**
     * Removes the {@link TableMetadataRef} from {@link #metadataRefs} and {@link #indexMetadataRefs} for the provided
     * (dropped) keyspace.
     */
    synchronized void removeRefs(KeyspaceMetadata ksm)
    {
        ksm.tablesAndViews()
           .forEach(this::removeRef);

        ksm.tables.indexTables()
                  .keySet()
                  .forEach(name -> indexMetadataRefs.remove(Pair.create(ksm.name, name)));
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
