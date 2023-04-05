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

public final class AlterViewStatement extends AlterSchemaStatement
{
    private final String viewName;
    private final TableAttributes attrs;
    private ClientState state;
    private final boolean ifExists;

    public AlterViewStatement(String keyspaceName, String viewName, TableAttributes attrs, boolean ifExists)
    {
        super(keyspaceName);
        this.viewName = viewName;
        this.attrs = attrs;
        this.ifExists = ifExists;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        // save the query state to use it for guardrails validation in #apply
        this.state = state;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        ViewMetadata view = null == keyspace
                          ? null
                          : keyspace.views.getNullable(viewName);

        if (null == view)
        {
            if (ifExists) return schema;
            throw ire("Materialized view '%s.%s' doesn't exist", keyspaceName, viewName);
        }

        attrs.validate();

        // Guardrails on table properties
        Guardrails.tableProperties.guard(attrs.updatedProperties(), attrs::removeProperty, state);

        TableParams params = attrs.asAlteredTableParams(view.metadata.params);

        if (params.gcGraceSeconds == 0)
        {
            throw ire("Cannot alter gc_grace_seconds of a materialized view to 0, since this " +
                      "value is used to TTL undelivered updates. Setting gc_grace_seconds too " +
                      "low might cause undelivered updates to expire before being replayed.");
        }

        if (params.defaultTimeToLive > 0)
        {
            throw ire("Forbidden default_time_to_live detected for a materialized view. " +
                      "Data in a materialized view always expire at the same time than " +
                      "the corresponding data in the parent table. default_time_to_live " +
                      "must be set to zero, see CASSANDRA-12868 for more information");
        }

        ViewMetadata newView = view.copy(view.metadata.withSwapped(params));
        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.views.withSwapped(newView)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, viewName);
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
        return new AuditLogContext(AuditLogEntryType.ALTER_VIEW, keyspaceName, viewName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, viewName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName name;
        private final TableAttributes attrs;
        private final boolean ifExists;

        public Raw(QualifiedName name, TableAttributes attrs, boolean ifExists)
        {
            this.name = name;
            this.attrs = attrs;
            this.ifExists = ifExists;
        }

        public AlterViewStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new AlterViewStatement(keyspaceName, name.getName(), attrs, ifExists);
        }
    }
}
