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

import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.cql3.ColumnIdentifier;

import static java.lang.String.format;

public class TableMetadataRef
{
    private final SchemaProvider schema;
    public final TableId id;
    public final String keyspace;
    public final String name;

    private volatile TableMetadata localTableMetadata;

    private volatile TableMetadataCache cachedTableMetadata;

    private static class TableMetadataCache
    {
        private final UUID lastSeenSchemaVersion;
        private final TableMetadata tableMetadata;

        private TableMetadataCache(UUID lastSeenSchemaVersion, TableMetadata tableMetadata)
        {
            this.lastSeenSchemaVersion = lastSeenSchemaVersion;
            this.tableMetadata = tableMetadata;
        }
    }

    public static TableMetadataRef forIndex(SchemaProvider schema, TableMetadata initial, String keyspace, String name, TableId id)
    {
        return new TableMetadataRef(schema, keyspace, name, id)
        {
            @Override
            public TableMetadata get()
            {
                return getOrDefault(initial);
            }

            @Override
            public TableMetadata getOrDefault(TableMetadata dflt)
            {
                Optional<TableMetadata> metadata = schema.getIndexMetadata(keyspace, name);
                return metadata.orElse(dflt);
            }
        };
    }

    // takes a default metadata instance to provide a ref which can work before the Table has been added to
    // schema. This is used when creating the initial memtable for a new CFS instance during a schema change
    // event as the CFS is created before the updated schema is published.
    public static TableMetadataRef withInitialReference(TableMetadataRef wrapped, TableMetadata initial)
    {
        return new TableMetadataRef(Schema.instance, initial.keyspace, initial.name, initial.id)
        {
            @Override
            public TableMetadata get()
            {
                return wrapped.getOrDefault(initial);
            }

            @Override
            public TableMetadata getOrDefault(TableMetadata dflt)
            {
                return wrapped.getOrDefault(dflt);
            }
        };
    }

    /**
     * Create a new ref to the passed {@link TableMetadata} for use by offline tools only.
     *
     * @param metadata {@link TableMetadata} to reference
     * @return a new TableMetadataRef instance linking to the passed {@link TableMetadata}
     */
    public static TableMetadataRef forOfflineTools(TableMetadata metadata)
    {
        return new TableMetadataRef(null, metadata.keyspace, metadata.name, metadata.id)
        {
            @Override
            public TableMetadata get()
            {
                return metadata;
            }
        };
    }

    public static TableMetadataRef forSystemTable(TableMetadata metadata)
    {
        return new TableMetadataRef(null, metadata.keyspace, metadata.name, metadata.id) {
            @Override
            public TableMetadata get()
            {
                return metadata;
            }

            @Override
            public TableMetadata getOrDefault(TableMetadata dflt)
            {
                return metadata;
            }
        };
    }

    public TableMetadataRef(SchemaProvider schema, String keyspace, String name, TableId id)
    {
        this.schema = schema;
        this.keyspace = keyspace;
        this.name = name;
        this.id = id;
    }

    public TableMetadata get()
    {
        TableMetadata metadata = getWithCaching();
        if (metadata == null)
            throw new IllegalStateException(format("Can't deref metadata for %s.%s.", keyspace, name));
        return metadata;
    }

    public TableMetadata getOrDefault(TableMetadata dflt)
    {
        TableMetadata tableMetadata = getWithCaching();

        if (tableMetadata == null)
            return dflt;

        return tableMetadata;
    }

    private TableMetadata getWithCaching()
    {
        UUID schemaVersion = schema.getVersion();
        if (schemaVersion == null)
            return schema.getTableMetadata(keyspace, name);

        TableMetadataCache cache = cachedTableMetadata;
        // we assume that local keyspaces and virtual keyspaces are immutable, so we need to track only a distributed schema version
        TableMetadata metadata;
        if (cache != null && schemaVersion.equals(cache.lastSeenSchemaVersion) && cache.tableMetadata != null)
            metadata = cache.tableMetadata;
        else
        {
            // we always retrieve metadata after schema version and assume they are changed coherently
            // we may put new metadata + old schema version to the cache but not vice versa
            // it we put non-latest schema version + latest metadata then it will be just updated on the next get() invocation
            metadata = schema.getTableMetadata(keyspace, name);
            if (metadata != null)
                cachedTableMetadata = new TableMetadataCache(schemaVersion, metadata);
        }
        return metadata;
    }

    /**
     * Returns node-local table metadata
     */
    public TableMetadata getLocal()
    {
        if (this.localTableMetadata != null)
        {
            TableMetadata global = get();
            if (!this.localTableMetadata.epoch.equals(global.epoch))
            {
                this.localTableMetadata = null;
                return global;
            }
            return localTableMetadata;
        }

        return get();
    }

    public void setLocalOverrides(TableMetadata metadata)
    {
        metadata.validateCompatibility(get());
        this.localTableMetadata = metadata;
    }

    @Override
    public String toString()
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(name));
    }
}
