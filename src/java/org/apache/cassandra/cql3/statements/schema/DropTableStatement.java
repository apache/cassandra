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
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.lang.String.join;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;

public final class DropTableStatement extends AlterSchemaStatement
{
    private final String tableName;
    private final boolean ifExists;

    public DropTableStatement(String keyspaceName, String tableName, boolean ifExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.getTableOrViewNullable(tableName);

        if (null == table)
        {
            if (ifExists)
                return schema;

            throw ire("Table '%s.%s' doesn't exist", keyspaceName, tableName);
        }

        if (table.isView())
            throw ire("Cannot use DROP TABLE on a materialized view. Please use DROP MATERIALIZED VIEW instead.");

        Iterable<ViewMetadata> views = keyspace.views.forTable(table.id);
        if (!isEmpty(views))
        {
            throw ire("Cannot drop a table when materialized views still depend on it (%s)",
                      keyspaceName,
                      join(", ", transform(views, ViewMetadata::name)));
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.without(table)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.DROPPED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.DROP);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_TABLE, keyspaceName, tableName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, tableName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName name;
        private final boolean ifExists;

        public Raw(QualifiedName name, boolean ifExists)
        {
            this.name = name;
            this.ifExists = ifExists;
        }

        public DropTableStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new DropTableStatement(keyspaceName, name.getName(), ifExists);
        }
    }
}
