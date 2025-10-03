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
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

/**
 * Handles the execution of SECURITY LABEL ON KEYSPACE CQL statements.
 * <p>
 * This statement allows users to add security classification labels to keyspaces.
 * Security labels are stored in the keyspace metadata and displayed when using DESCRIBE KEYSPACE.
 * </p>
 * <p>
 * Syntax: {@code SECURITY LABEL [FOR provider_name] ON KEYSPACE keyspace_name IS 'label';}
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
 * SECURITY LABEL ON KEYSPACE production IS 'confidential';
 *
 * // Syntax with provider (accepted but not yet implemented)
 * SECURITY LABEL FOR custom_provider ON KEYSPACE production IS 'top_secret';
 *
 * // Remove security label
 * SECURITY LABEL ON KEYSPACE production IS NULL;
 * </pre>
 * </p>
 *
 * @see SecurityLabelOnTableStatement
 * @see SecurityLabelOnColumnStatement
 * @see SecurityLabelOnUserTypeStatement
 */
public final class SecurityLabelOnKeyspaceStatement extends SchemaDescriptionStatement
{
    private final String provider;

    public SecurityLabelOnKeyspaceStatement(String keyspaceName, String securityLabel, String provider)
    {
        super(keyspaceName, securityLabel, DescriptionType.SECURITY_LABEL);
        this.provider = provider;
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = validateAndGetKeyspace(schema);

        providerWarning(provider);

        KeyspaceParams newParams = keyspace.params.withSecurityLabel(effectiveDescription());
        KeyspaceMetadata newKeyspace = keyspace.withSwapped(newParams);

        return schema.withAddedOrUpdated(newKeyspace);
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, keyspaceName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.SECURITY_LABEL_KEYSPACE, keyspaceName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final String keyspaceName;
        private final String securityLabel;
        private final String provider;

        public Raw(String keyspaceName, String securityLabel, String provider)
        {
            this.keyspaceName = keyspaceName;
            this.securityLabel = securityLabel;
            this.provider = provider;
        }

        @Override
        public SecurityLabelOnKeyspaceStatement prepare(ClientState state)
        {
            return new SecurityLabelOnKeyspaceStatement(keyspaceName, securityLabel, provider);
        }
    }
}
