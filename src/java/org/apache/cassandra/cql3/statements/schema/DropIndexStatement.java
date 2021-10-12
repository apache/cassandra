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
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class DropIndexStatement extends AlterSchemaStatement
{
    private final String indexName;
    private final boolean ifExists;

    public DropIndexStatement(String keyspaceName, String indexName, boolean ifExists)
    {
        super(keyspaceName);
        this.indexName = indexName;
        this.ifExists = ifExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.findIndexedTable(indexName).orElse(null);

        if (null == table)
        {
            if (ifExists)
                return schema;

            throw ire("Index '%s.%s' doesn't exist'", keyspaceName, indexName);
        }

        TableMetadata newTable = table.withSwapped(table.indexes.without(indexName));
        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.withSwapped(newTable)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        assert diff.altered.size() == 1;
        KeyspaceDiff ksDiff = diff.altered.get(0);

        assert ksDiff.tables.altered.size() == 1;
        Diff.Altered<TableMetadata> tableDiff = ksDiff.tables.altered.iterator().next();

        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableDiff.after.name);
    }

    public void authorize(ClientState client)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (null == keyspace)
            return;

        keyspace.findIndexedTable(indexName)
                .ifPresent(t -> client.ensureTablePermission(keyspaceName, t.name, Permission.ALTER));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_INDEX, keyspaceName, indexName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, indexName);
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

        public DropIndexStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new DropIndexStatement(keyspaceName, name.getName(), ifExists);
        }
    }
}
