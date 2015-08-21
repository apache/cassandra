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

import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;

import static com.google.common.collect.Iterables.filter;

/**
 * For backwards compatibility, in the first instance an IndexMetadata must have
 * TargetType.COLUMN and its Set of target columns must contain only a single
 * ColumnIdentifier. Hence, this is what is enforced by the public factory methods
 * on IndexMetadata.
 * These constraints, along with the internal datastructures here will be relaxed as
 * support is added for multiple target columns per-index and for indexes with
 * TargetType.ROW
 */
public class Indexes implements Iterable<IndexMetadata>
{
    private final ImmutableMap<String, IndexMetadata> indexes;
    private final ImmutableMultimap<ColumnIdentifier, IndexMetadata> indexesByColumn;

    private Indexes(Builder builder)
    {
        indexes = builder.indexes.build();
        indexesByColumn = builder.indexesByColumn.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Indexes none()
    {
        return builder().build();
    }

    public Iterator<IndexMetadata> iterator()
    {
        return indexes.values().iterator();
    }

    public int size()
    {
        return indexes.size();
    }

    public boolean isEmpty()
    {
        return indexes.isEmpty();
    }

    /**
     * Get the index with the specified name
     *
     * @param name a non-qualified index name
     * @return an empty {@link Optional} if the named index is not found; a non-empty optional of {@link IndexMetadata} otherwise
     */
    public Optional<IndexMetadata> get(String name)
    {
        return indexes.values().stream().filter(def -> def.name.equals(name)).findFirst();
    }

    /**
     * Answer true if contains an index with the specified name.
     * @param name a non-qualified index name.
     * @return true if the named index is found; false otherwise
     */
    public boolean has(String name)
    {
        return get(name).isPresent();
    }

    /**
     * Get the index associated with the specified column. This may be removed or modified as support is added
     * for indexes with multiple target columns and with TargetType.ROW
     *
     * @param column a column definition for which an {@link IndexMetadata} is being sought
     * @return an empty {@link Optional} if the named index is not found; a non-empty optional of {@link IndexMetadata} otherwise
     */
    public Collection<IndexMetadata> get(ColumnDefinition column)
    {
        return indexesByColumn.get(column.name);
    }

    /**
     * Answer true if an index is associated with the specified column.
     * @param column
     * @return
     */
    public boolean hasIndexFor(ColumnDefinition column)
    {
        return !indexesByColumn.get(column.name).isEmpty();
    }

    /**
     * Create a SecondaryIndexes instance with the provided index added
     */
    public Indexes with(IndexMetadata index)
    {
        if (get(index.name).isPresent())
            throw new IllegalStateException(String.format("Index %s already exists", index.name));

        return builder().add(this).add(index).build();
    }

    /**
     * Creates a SecondaryIndexes instance with the index with the provided name removed
     */
    public Indexes without(String name)
    {
        IndexMetadata index = get(name).orElseThrow(() -> new IllegalStateException(String.format("Index %s doesn't exist", name)));
        return builder().add(filter(this, v -> v != index)).build();
    }

    /**
     * Creates a SecondaryIndexes instance which contains an updated index definition
     */
    public Indexes replace(IndexMetadata index)
    {
        return without(index.name).with(index);
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Indexes && indexes.equals(((Indexes) o).indexes));
    }

    @Override
    public int hashCode()
    {
        return indexes.hashCode();
    }

    @Override
    public String toString()
    {
        return indexes.values().toString();
    }

    public static String getAvailableIndexName(String ksName, String cfName, ColumnIdentifier columnName)
    {

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);
        Set<String> existingNames = ksm == null ? new HashSet<>() : ksm.existingIndexNames(null);
        String baseName = IndexMetadata.getDefaultIndexName(cfName, columnName);
        String acceptedName = baseName;
        int i = 0;
        while (existingNames.contains(acceptedName))
            acceptedName = baseName + '_' + (++i);

        return acceptedName;
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, IndexMetadata> indexes = new ImmutableMap.Builder<>();
        final ImmutableMultimap.Builder<ColumnIdentifier, IndexMetadata> indexesByColumn = new ImmutableMultimap.Builder<>();

        private Builder()
        {
        }

        public Indexes build()
        {
            return new Indexes(this);
        }

        public Builder add(IndexMetadata index)
        {
            indexes.put(index.name, index);
            // All indexes are column indexes at the moment
            if (index.isColumnIndex())
            {
                for (ColumnIdentifier target : index.columns)
                    indexesByColumn.put(target, index);

            }
            return this;
        }

        public Builder add(Iterable<IndexMetadata> indexes)
        {
            indexes.forEach(this::add);
            return this;
        }
    }
}
