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

import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.Descriptor;

public interface SchemaProvider
{
    @Nullable
    Keyspace getKeyspaceInstance(String keyspaceName);

    Keyspace maybeAddKeyspaceInstance(String keyspaceName, Supplier<Keyspace> loadFunction);

    @Nullable
    KeyspaceMetadata getKeyspaceMetadata(String keyspaceName);

    @Nullable
    TableMetadata getTableMetadata(TableId id);

    @Nullable
    TableMetadata getTableMetadata(String keyspace, String table);

    default TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        String message =
            String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema"
                          + "not being fully propagated.  Please wait for schema agreement on table creation.",
                          id);
        throw new UnknownTableException(message, id);
    }

    @Nullable
    TableMetadataRef getTableMetadataRef(String keyspace, String table);

    @Nullable
    TableMetadataRef getTableMetadataRef(TableId id);

    @Nullable
    default TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadataRef(descriptor.ksname, descriptor.cfname);
    }
}
