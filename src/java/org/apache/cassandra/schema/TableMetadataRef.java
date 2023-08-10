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

import org.github.jamm.Unmetered;

/**
 * Encapsulates a volatile reference to an immutable {@link TableMetadata} instance.
 *
 * Used in classes that need up-to-date metadata to avoid the cost of looking up {@link Schema} hashmaps.
 */
@Unmetered
public final class TableMetadataRef
{
    public final TableId id;
    public final String keyspace;
    public final String name;

    private volatile TableMetadata metadata;
    private volatile TableMetadata localTableMetadata;

    TableMetadataRef(TableMetadata metadata)
    {
        this.metadata = metadata;

        id = metadata.id;
        keyspace = metadata.keyspace;
        name = metadata.name;
    }

    /**
     * Create a new ref to the passed {@link TableMetadata} for use by offline tools only.
     *
     * @param metadata {@link TableMetadata} to reference
     * @return a new TableMetadataRef instance linking to the passed {@link TableMetadata}
     */
    public static TableMetadataRef forOfflineTools(TableMetadata metadata)
    {
        return new TableMetadataRef(metadata);
    }

    public TableMetadata get()
    {
        return metadata;
    }

    /**
     * Returns node-local table metadata
     */
    public TableMetadata getLocal()
    {
        if (this.localTableMetadata != null)
            return localTableMetadata;

        return metadata;
    }

    /**
     * Update the reference with the most current version of {@link TableMetadata}
     * <p>
     * Must only be used by methods in {@link Schema}, *DO NOT* make public
     * even for testing purposes, it isn't safe.
     */
    void set(TableMetadata metadata)
    {
        metadata.validateCompatibility(get());
        this.metadata = metadata;
        this.localTableMetadata = null;
    }


    public void setLocalOverrides(TableMetadata metadata)
    {
        metadata.validateCompatibility(get());
        this.localTableMetadata = metadata;
    }

    @Override
    public String toString()
    {
        return get().toString();
    }
}
