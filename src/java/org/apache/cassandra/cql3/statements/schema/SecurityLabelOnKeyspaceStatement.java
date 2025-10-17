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
import org.apache.cassandra.cql3.statements.SchemaDescriptionsUtil;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
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
 * Syntax: {@code SECURITY LABEL ON KEYSPACE keyspace_name IS 'label';}
 * </p>
 * <p>
 * Security labels can be used to mark data sensitivity levels or compliance requirements.
 * Labels can be removed by setting them to an empty string or null.
 * </p>
 *
 * @see SecurityLabelOnTableStatement
 * @see SecurityLabelOnColumnStatement
 * @see SecurityLabelOnTypeStatement
 */
public final class SecurityLabelOnKeyspaceStatement extends AlterSchemaStatement
{
    private final String securityLabel;
    private final String provider;

    public SecurityLabelOnKeyspaceStatement(String keyspaceName, String securityLabel, String provider)
    {
        super(keyspaceName);
        this.securityLabel = securityLabel;
        this.provider = provider;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);
        SchemaDescriptionsUtil.validateSecurityLabel(securityLabel);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        if (provider != null)
            ClientWarn.instance.warn("Provider is not yet implemented but will proceed with adding the security label");

        KeyspaceParams newParams = keyspace.params.withSecurityLabel(securityLabel);
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
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, securityLabel);
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
