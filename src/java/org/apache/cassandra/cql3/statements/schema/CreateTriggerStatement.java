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
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class CreateTriggerStatement extends AlterSchemaStatement
{
    private final String tableName;
    private final String triggerName;
    private final String triggerClass;
    private final boolean ifNotExists;

    public CreateTriggerStatement(String keyspaceName, String tableName, String triggerName, String triggerClass, boolean ifNotExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.triggerName = triggerName;
        this.triggerClass = triggerClass;
        this.ifNotExists = ifNotExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        TableMetadata table = keyspace.getTableOrViewNullable(tableName);
        if (null == table)
            throw ire("Table '%s' doesn't exist", tableName);

        if (table.isView())
            throw ire("Cannot CREATE TRIGGER for a materialized view");

        TriggerMetadata existingTrigger = table.triggers.get(triggerName).orElse(null);
        if (null != existingTrigger)
        {
            if (ifNotExists)
                return schema;

            throw ire("Trigger '%s' already exists", triggerName);
        }

        try
        {
            TriggerExecutor.instance.loadTriggerInstance(triggerClass);
        }
        catch (Exception e)
        {
            throw ire("Trigger class '%s' couldn't be loaded", triggerClass);
        }

        TableMetadata newTable = table.withSwapped(table.triggers.with(TriggerMetadata.create(triggerName, triggerClass)));
        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.withSwapped(newTable)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureIsSuperuser("Only superusers are allowed to perform CREATE TRIGGER queries");
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TRIGGER, keyspaceName, triggerName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, triggerName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName tableName;
        private final String triggerName;
        private final String triggerClass;
        private final boolean ifNotExists;

        public Raw(QualifiedName tableName, String triggerName, String triggerClass, boolean ifNotExists)
        {
            this.tableName = tableName;
            this.triggerName = triggerName;
            this.triggerClass = triggerClass;
            this.ifNotExists = ifNotExists;
        }

        public CreateTriggerStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace() ? tableName.getKeyspace() : state.getKeyspace();
            return new CreateTriggerStatement(keyspaceName, tableName.getName(), triggerName, triggerClass, ifNotExists);
        }
    }
}
