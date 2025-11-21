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
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

/**
 * Handles the execution of SECURITY LABEL ON TYPE CQL statements.
 * <p>
 * This statement allows users to add security classification labels to user-defined types.
 * Security labels are stored in the type metadata and displayed when using DESCRIBE TYPE.
 * </p>
 * <p>
 * Syntax: {@code SECURITY LABEL [FOR provider_name] ON TYPE [keyspace_name.]type_name IS 'label';}
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
 * SECURITY LABEL ON TYPE address IS 'internal';
 *
 * // Syntax with provider (accepted but not yet implemented)
 * SECURITY LABEL FOR custom_provider ON TYPE address IS 'restricted';
 *
 * // Remove security label
 * SECURITY LABEL ON TYPE address IS NULL;
 * </pre>
 * </p>
 *
 * @see SecurityLabelOnKeyspaceStatement
 * @see SecurityLabelOnTableStatement
 * @see SecurityLabelOnColumnStatement
 */
public final class SecurityLabelOnUserTypeStatement extends SchemaDescriptionStatement
{
    private final String typeName;
    private final String provider;

    public SecurityLabelOnUserTypeStatement(String keyspaceName, String typeName, String securityLabel, String provider)
    {
        super(keyspaceName, securityLabel, DescriptionType.SECURITY_LABEL);
        this.typeName = typeName;
        this.provider = provider;
    }

    @Override
    public boolean compatibleWith(ClusterMetadata metadata)
    {
        return metadata.directory.commonSerializationVersion.isAtLeast(Version.V8);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        UserType type = validateAndGetType(schema, typeName);
        providerWarning(provider);

        UserType newType = type.withSecurityLabel(effectiveDescription());
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        KeyspaceMetadata newKeyspace = keyspace.withSwapped(keyspace.types.withUpdatedUserType(newType));

        return schema.withAddedOrUpdated(newKeyspace);
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TYPE, keyspaceName, typeName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.SECURITY_LABEL_TYPE, keyspaceName, typeName);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s.%s, %s)", getClass().getSimpleName(), keyspaceName, typeName, description);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final UTName typeName;
        private final String securityLabel;
        private final String provider;

        public Raw(UTName typeName, String securityLabel, String provider)
        {
            this.typeName = typeName;
            this.securityLabel = securityLabel;
            this.provider = provider;
        }

        @Override
        public SecurityLabelOnUserTypeStatement prepare(ClientState state)
        {
            String keyspaceName = typeName.hasKeyspace() ? typeName.getKeyspace() : state.getKeyspace();
            return new SecurityLabelOnUserTypeStatement(keyspaceName,
                                                        typeName.getStringTypeName(),
                                                        securityLabel,
                                                        provider);
        }
    }
}
