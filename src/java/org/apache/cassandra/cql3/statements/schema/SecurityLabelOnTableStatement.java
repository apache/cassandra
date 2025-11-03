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
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

/**
 * Handles the execution of SECURITY LABEL ON TABLE CQL statements.
 * <p>
 * This statement allows users to add security classification labels to tables.
 * Security labels are stored in the table metadata and displayed when using DESCRIBE TABLE.
 * </p>
 * <p>
 * Syntax: {@code SECURITY LABEL [FOR provider_name] ON TABLE [keyspace_name.]table_name IS 'label';}
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
 * SECURITY LABEL ON TABLE users IS 'restricted';
 *
 * // Syntax with provider (accepted but not yet implemented)
 * SECURITY LABEL FOR custom_provider ON TABLE users IS 'classified';
 *
 * // Remove security label
 * SECURITY LABEL ON TABLE users IS NULL;
 * </pre>
 * </p>
 *
 * @see SecurityLabelOnKeyspaceStatement
 * @see SecurityLabelOnColumnStatement
 * @see SecurityLabelOnUserTypeStatement
 */
public final class SecurityLabelOnTableStatement extends SchemaDescriptionStatement
{
    private final String tableName;
    private final String provider;

    public SecurityLabelOnTableStatement(String keyspaceName, String tableName, String securityLabel, String provider)
    {
        super(keyspaceName, securityLabel, DescriptionType.SECURITY_LABEL);
        this.tableName = tableName;
        this.provider = provider;
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();

        TableMetadata table = validateAndGetTable(schema, tableName);
        providerWarning(provider);

        TableParams newParams = table.params.unbuild().securityLabel(effectiveDescription()).build();
        TableMetadata newTable = table.withSwapped(newParams);

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
        return new AuditLogContext(AuditLogEntryType.SECURITY_LABEL_TABLE, keyspaceName, tableName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s, %s)", getClass().getSimpleName(), keyspaceName, tableName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName tableName;
        private final String securityLabel;
        private final String provider;

        public Raw(QualifiedName tableName, String securityLabel, String provider)
        {
            this.tableName = tableName;
            this.securityLabel = securityLabel;
            this.provider = provider;
        }

        @Override
        public SecurityLabelOnTableStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new SecurityLabelOnTableStatement(keyspaceName,
                                                     tableName.getName(),
                                                     securityLabel,
                                                     provider);
        }
    }
}
