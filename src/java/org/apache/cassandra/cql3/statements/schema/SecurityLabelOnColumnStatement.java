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
 * Handles the execution of SECURITY LABEL ON COLUMN CQL statements.
 * <p>
 * This statement allows users to add security classification labels to individual columns.
 * Security labels are stored in the column metadata and displayed when using DESCRIBE TABLE.
 * </p>
 * <p>
 * Syntax: {@code SECURITY LABEL [FOR provider_name] ON COLUMN [keyspace_name.]table_name.column_name IS 'label';}
 * </p>
 * <p>
 * Security labels can be used to mark data sensitivity levels or compliance requirements.
 * Labels can be removed by setting them to NULL.
 * </p>
 * <h3>Provider Support</h3>
 * <p>
 * The optional {@code FOR provider_name} clause is accepted in the syntax to support future
 * extensibility. Providers are intended to be pluggable security policy modules that can
 * interpret and enforce access controls based on security labels. However, provider
 * functionality is not currently implemented. If a provider is specified, a warning will be
 * issued but the security label will still be applied. The provider parameter is reserved
 * for future use.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * // Basic syntax without provider
 * SECURITY LABEL ON COLUMN users.ssn IS 'PII';
 *
 * // Syntax with provider (accepted but not yet implemented)
 * SECURITY LABEL FOR custom_provider ON COLUMN users.ssn IS 'sensitive';
 *
 * // Remove security label
 * SECURITY LABEL ON COLUMN users.ssn IS NULL;
 * </pre>
 * </p>
 *
 * @see SecurityLabelOnKeyspaceStatement
 * @see SecurityLabelOnTableStatement
 * @see SecurityLabelOnUserTypeStatement
 */
public final class SecurityLabelOnColumnStatement extends SchemaDescriptionStatement
{
    private final String tableName;
    private final ColumnIdentifier columnName;
    private final String provider;

    public SecurityLabelOnColumnStatement(String keyspaceName, String tableName, ColumnIdentifier columnName, String securityLabel, String provider)
    {
        super(keyspaceName, securityLabel, DescriptionType.SECURITY_LABEL);
        this.tableName = tableName;
        this.columnName = columnName;
        this.provider = provider;
    }


    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        TableMetadata table = validateAndGetTable(schema, tableName);

        ColumnMetadata column = table.getColumn(columnName);
        if (null == column)
            throw ire("Column '%s' doesn't exist in table '%s.%s'", columnName, keyspaceName, tableName);

        providerWarning(provider);

        TableMetadata newTable = table.unbuild().alterColumnSecurityLabel(columnName, effectiveDescription()).build();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
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
        return new AuditLogContext(AuditLogEntryType.SECURITY_LABEL_COLUMN, keyspaceName, tableName);
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
        private final String securityLabel;
        private final String provider;

        public Raw(QualifiedName tableName, ColumnIdentifier columnName, String securityLabel, String provider)
        {
            this.tableName = tableName;
            this.columnName = columnName;
            this.securityLabel = securityLabel;
            this.provider = provider;
        }

        @Override
        public SecurityLabelOnColumnStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new SecurityLabelOnColumnStatement(keyspaceName,
                                                      tableName.getName(),
                                                      columnName,
                                                      securityLabel,
                                                      provider);
        }
    }
}
