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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

public final class DropKeyspaceStatement extends AlterSchemaStatement
{
    private final boolean ifExists;

    public DropKeyspaceStatement(String keyspaceName, boolean ifExists)
    {
        super(keyspaceName);
        this.ifExists = ifExists;
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Guardrails.dropKeyspaceEnabled.ensureEnabled(state);

        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (keyspace != null)
        {
            // check that no accord tables in the keyspace are currently in the process of being dropped
            List<TableMetadata> pendingDrop = keyspace.tables.stream()
                                                             .filter(t -> t.params.pendingDrop)
                                                             .collect(Collectors.toList());
            if (!pendingDrop.isEmpty())
                throw ire("Cannot drop keyspace '%s' as it contains accord tables which are currently being dropped. " +
                          "Please wait for those operations to complete before dropping the keyspace. (%s)",
                          keyspaceName, pendingDrop.stream()
                                                   .map(Object::toString)
                                                   .collect(Collectors.joining(",")));

            List<TableMetadata> accordTables = keyspace.tables.stream()
                                               .filter(TableMetadata::requiresAccordSupport)
                                               .collect(Collectors.toList());
            if (!accordTables.isEmpty())
                throw ire("Cannot drop keyspace '%s' as it contains accord tables. (%s)",
                          keyspaceName, accordTables.stream()
                                                   .map(Object::toString)
                                                   .collect(Collectors.joining(",")));
            return schema.without(keyspaceName);
        }

        if (ifExists)
            return schema;

        throw ire("Keyspace '%s' doesn't exist", keyspaceName);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.DROPPED, keyspaceName);
    }

    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.DROP);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_KEYSPACE, keyspaceName);
    }

    public String toString()
    {
        return String.format("%s (%s)", getClass().getSimpleName(), keyspaceName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final String keyspaceName;
        private final boolean ifExists;

        public Raw(String keyspaceName, boolean ifExists)
        {
            this.keyspaceName = keyspaceName;
            this.ifExists = ifExists;
        }

        public DropKeyspaceStatement prepare(ClientState state)
        {
            return new DropKeyspaceStatement(keyspaceName, ifExists);
        }
    }
}
