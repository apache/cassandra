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

import java.util.function.UnaryOperator;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.RawKeyspaceAwareStatement;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class DropTriggerStatement extends AlterSchemaStatement
{
    private final String tableName;
    private final String triggerName;
    private final boolean ifExists;

    public DropTriggerStatement(String queryString, String keyspaceName, String tableName,
                                String triggerName, boolean ifExists)
    {
        super(queryString, keyspaceName);
        this.tableName = tableName;
        this.triggerName = triggerName;
        this.ifExists = ifExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.tables.getNullable(tableName);

        TriggerMetadata trigger = null == table
                                ? null
                                : table.triggers.get(triggerName).orElse(null);

        if (null == trigger)
        {
            if (ifExists)
                return schema;

            throw ire("Trigger '%s' on '%s.%s' doesn't exist", triggerName, keyspaceName, tableName);
        }

        TableMetadata newTable = table.withSwapped(table.triggers.without(triggerName));
        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.withSwapped(newTable)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perfrom DROP TRIGGER queries");
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_TRIGGER, keyspaceName, triggerName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, triggerName);
    }

    public static final class Raw extends RawKeyspaceAwareStatement<DropTriggerStatement>
    {
        private final QualifiedName tableName;
        private final String triggerName;
        private final boolean ifExists;

        public Raw(QualifiedName tableName, String triggerName, boolean ifExists)
        {
            this.tableName = tableName;
            this.triggerName = triggerName;
            this.ifExists = ifExists;
        }

        @Override
        public DropTriggerStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            String keyspaceName = keyspaceMapper.apply(tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace());
            return new DropTriggerStatement(rawCQLStatement, keyspaceName, tableName.getName(),
                                            triggerName, ifExists);
        }
    }
}
