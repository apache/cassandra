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
package org.apache.cassandra.db.virtual;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.marshal.UserType;

/**
 * Virtual table that exposes all comments on schema elements.
 * <p>
 * This table provides a unified view of documentation metadata across:
 * <ul>
 *   <li>Keyspaces - comments on keyspaces</li>
 *   <li>Tables - comments on tables</li>
 *   <li>Columns - comments on columns</li>
 *   <li>User-Defined Types (UDTs) - comments on UDTs</li>
 * </ul>
 * </p>
 * <p>
 * The table automatically reflects the current state of schema metadata without requiring
 * explicit updates. Data is read directly from {@link org.apache.cassandra.schema.Schema#instance} on each query.
 * </p>
 * <p>
 * Example queries:
 * <pre>
 * -- All comments in the system
 * SELECT * FROM system_views.schema_comments;
 *
 * -- All table comments in a specific keyspace
 * SELECT * FROM system_views.schema_comments
 * WHERE object_type = 'TABLE' AND keyspace_name = 'my_keyspace';
 *
 * -- All column comments across all keyspaces
 * SELECT * FROM system_views.schema_comments WHERE object_type = 'COLUMN';
 * </pre>
 * </p>
 */
final class SchemaCommentsTable extends AbstractSchemaMetadataTable
{
    SchemaCommentsTable(String keyspace)
    {
        super(keyspace, SchemaTableType.COMMENT);
    }

    @Override
    protected String extractKeyspaceMetadata(KeyspaceMetadata keyspace)
    {
        return keyspace.params.comment;
    }

    @Override
    protected String extractTableMetadata(TableMetadata table)
    {
        return table.params.comment;
    }

    @Override
    protected String extractColumnMetadata(ColumnMetadata column)
    {
        return column.comment;
    }

    @Override
    protected String extractUdtMetadata(UserType udt)
    {
        return udt.comment;
    }

    @Override
    protected String extractFieldMetadata(UserType udt, String fieldName)
    {
        return udt.fieldComment(org.apache.cassandra.cql3.FieldIdentifier.forUnquoted(fieldName));
    }
}
