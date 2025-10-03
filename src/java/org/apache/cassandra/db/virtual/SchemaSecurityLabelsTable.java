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
 * Virtual table that exposes all security labels on schema elements.
 * <p>
 * This table provides a unified view of security label metadata across:
 * <ul>
 *   <li>Keyspaces - security labels on keyspaces</li>
 *   <li>Tables - security labels on tables</li>
 *   <li>Columns - security labels on columns</li>
 *   <li>User-Defined Types (UDTs) - security labels on UDTs</li>
 * </ul>
 * </p>
 * <p>
 * The table automatically reflects the current state of schema metadata without requiring
 * explicit updates. Data is read directly from {@link org.apache.cassandra.schema.Schema#instance} on each query.
 * </p>
 * <p>
 * Example queries:
 * <pre>
 * -- All security labels in the system
 * SELECT * FROM system_views.schema_security_labels;
 *
 * -- All table security labels in a specific keyspace
 * SELECT * FROM system_views.schema_security_labels
 * WHERE object_type = 'TABLE' AND keyspace_name = 'my_keyspace';
 *
 * -- All column security labels across all keyspaces
 * SELECT * FROM system_views.schema_security_labels WHERE object_type = 'COLUMN';
 * </pre>
 * </p>
 */
final class SchemaSecurityLabelsTable extends AbstractSchemaMetadataTable
{
    SchemaSecurityLabelsTable(String keyspace)
    {
        super(keyspace, SchemaTableType.SECURITY_LABEL);
    }

    @Override
    protected String extractKeyspaceMetadata(KeyspaceMetadata keyspace)
    {
        return keyspace.params.securityLabel;
    }

    @Override
    protected String extractTableMetadata(TableMetadata table)
    {
        return table.params.securityLabel;
    }

    @Override
    protected String extractColumnMetadata(ColumnMetadata column)
    {
        return column.securityLabel;
    }

    @Override
    protected String extractUdtMetadata(UserType udt)
    {
        return udt.securityLabel;
    }
}