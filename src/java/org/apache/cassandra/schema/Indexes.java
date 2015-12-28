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

import org.apache.cassandra.config.Schema;

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
    private final ImmutableMap<String, IndexMetadata> indexesByName;
    private final ImmutableMap<UUID, IndexMetadata> indexesById;

    private Indexes(Builder builder)
    {
        indexesByName = builder.indexesByName.build();
        indexesById = builder.indexesById.build();
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
        return indexesByName.values().iterator();
    }

    public int size()
    {
        return indexesByName.size();
    }

    public boolean isEmpty()
    {
        return indexesByName.isEmpty();
    }

    /**
     * Get the index with the specified name
     *
     * @param name a non-qualified index name
     * @return an empty {@link Optional} if the named index is not found; a non-empty optional of {@link IndexMetadata} otherwise
     */
    public Optional<IndexMetadata> get(String name)
    {
        return Optional.ofNullable(indexesByName.get(name));
    }

    /**
     * Answer true if contains an index with the specified name.
     * @param name a non-qualified index name.
     * @return true if the named index is found; false otherwise
     */
    public boolean has(String name)
    {
        return indexesByName.containsKey(name);
    }

    /**
     * Get the index with the specified id
     *
     * @param id a UUID which identifies an index
     * @return an empty {@link Optional} if no index with the specified id is found; a non-empty optional of
     *         {@link IndexMetadata} otherwise
     */

    public Optional<IndexMetadata> get(UUID id)
    {
        return Optional.ofNullable(indexesById.get(id));
    }

    /**
     * Answer true if contains an index with the specified id.
     * @param id a UUID which identifies an index.
     * @return true if an index with the specified id is found; false otherwise
     */
    public boolean has(UUID id)
    {
        return indexesById.containsKey(id);
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
        return this == o || (o instanceof Indexes && indexesByName.equals(((Indexes) o).indexesByName));
    }

    @Override
    public int hashCode()
    {
        return indexesByName.hashCode();
    }

    @Override
    public String toString()
    {
        return indexesByName.values().toString();
    }

    public static String getAvailableIndexName(String ksName, String cfName, String indexNameRoot)
    {

        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);
        Set<String> existingNames = ksm == null ? new HashSet<>() : ksm.existingIndexNames(null);
        String baseName = IndexMetadata.getDefaultIndexName(cfName, indexNameRoot);
        String acceptedName = baseName;
        int i = 0;
        while (existingNames.contains(acceptedName))
            acceptedName = baseName + '_' + (++i);

        return acceptedName;
    }

    public static final class Builder
    {
        final ImmutableMap.Builder<String, IndexMetadata> indexesByName = new ImmutableMap.Builder<>();
        final ImmutableMap.Builder<UUID, IndexMetadata> indexesById = new ImmutableMap.Builder<>();

        private Builder()
        {
        }

        public Indexes build()
        {
            return new Indexes(this);
        }

        public Builder add(IndexMetadata index)
        {
            indexesByName.put(index.name, index);
            indexesById.put(index.id, index);
            return this;
        }

        public Builder add(Iterable<IndexMetadata> indexes)
        {
            indexes.forEach(this::add);
            return this;
        }
    }
}
