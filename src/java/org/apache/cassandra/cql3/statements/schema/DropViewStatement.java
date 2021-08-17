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
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class DropViewStatement extends AlterSchemaStatement
{
    private final String viewName;
    private final boolean ifExists;

    public DropViewStatement(String keyspaceName, String viewName, boolean ifExists)
    {
        super(keyspaceName);
        this.viewName = viewName;
        this.ifExists = ifExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        ViewMetadata view = null == keyspace
                          ? null
                          : keyspace.views.getNullable(viewName);

        if (null == view)
        {
            if (ifExists)
                return schema;

            throw ire("Materialized view '%s.%s' doesn't exist", keyspaceName, viewName);
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.views.without(viewName)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.DROPPED, Target.TABLE, keyspaceName, viewName);
    }

    public void authorize(ClientState client)
    {
        ViewMetadata view = Schema.instance.getView(keyspaceName, viewName);
        if (null != view)
            client.ensureTablePermission(keyspaceName, view.baseTableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_VIEW, keyspaceName, viewName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, viewName);
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

        public DropViewStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new DropViewStatement(keyspaceName, name.getName(), ifExists);
        }
    }
}
