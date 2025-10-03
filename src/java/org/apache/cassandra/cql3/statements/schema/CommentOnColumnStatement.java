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
package org.apache.cassandra.cql3.statements.schema;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

/**
 * Handles the execution of COMMENT ON COLUMN CQL statements.
 * <p>
 * This statement allows users to add descriptive comments to individual columns for documentation purposes.
 * Comments are stored in the column metadata and displayed when using DESCRIBE TABLE.
 * </p>
 * <p>
 * Syntax: {@code COMMENT ON COLUMN [keyspace_name.]table_name.column_name IS 'comment text';}
 * </p>
 * <p>
 * Comments can be removed by setting them to an empty string or null.
 * </p>
 *
 * @see CommentOnKeyspaceStatement
 * @see CommentOnTableStatement
 * @see CommentOnUserTypeStatement
 */
public final class CommentOnColumnStatement extends SchemaDescriptionStatement
{
    private final String tableName;
    private final ColumnIdentifier columnName;

    public CommentOnColumnStatement(String keyspaceName, String tableName, ColumnIdentifier columnName, String comment)
    {
        super(keyspaceName, comment, DescriptionType.COMMENT);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        TableMetadata table = validateAndGetTable(schema, tableName);
        ColumnMetadata columnMetadata = table.getColumn(columnName);
        if (null == columnMetadata)
            throw ire("Column '%s' doesn't exist in table '%s.%s'", columnName, keyspaceName, tableName);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        TableMetadata newTable = table.unbuild().alterColumnComment(columnName, effectiveDescription()).build();
        KeyspaceMetadata newKeyspace = keyspace.withSwapped(keyspace.tables.withSwapped(newTable));

        return schema.withAddedOrUpdated(newKeyspace);
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.COMMENT_COLUMN, keyspaceName, tableName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s.%s, %s)", getClass().getSimpleName(), keyspaceName, tableName, columnName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName tableName;
        private final ColumnIdentifier columnName;
        private final String comment;

        public Raw(QualifiedName tableName, ColumnIdentifier columnName, String comment)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.comment = comment;
        }

        @Override
        public CommentOnColumnStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new CommentOnColumnStatement(keyspaceName,
                                                tableName.getName(),
                                                columnName,
                                                comment);
        }
    }
}
