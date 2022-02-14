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
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.RawKeyspaceAwareStatement;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class AlterViewStatement extends AlterSchemaStatement
{
    private final String viewName;
    private final TableAttributes attrs;
    private QueryState state;

    public AlterViewStatement(String queryString, String keyspaceName, String viewName,
            TableAttributes attrs)
    {
        super(queryString, keyspaceName);
        this.viewName = viewName;
        this.attrs = attrs;
    }

    public void validate(QueryState state)
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
            throw ire("Materialized view '%s.%s' doesn't exist", keyspaceName, viewName);

        attrs.validate();

        Guardrails.disallowedTableProperties.ensureAllowed(attrs.updatedProperties(), state);
        Guardrails.ignoredTableProperties.maybeIgnoreAndWarn(attrs.updatedProperties(), attrs::removeProperty, state);

        TableParams params = attrs.asAlteredTableParams(view.metadata.params);

        if (params.gcGraceSeconds == 0)
        {
            throw ire("Cannot alter gc_grace_seconds of a materialized view to 0, since this " +
                      "value is used to TTL undelivered updates. Setting gc_grace_seconds too " +
                      "low might cause undelivered updates to expire before being replayed.");
        }

        if (params.defaultTimeToLive > 0)
        {
            throw ire("Cannot set or alter default_time_to_live for a materialized view. " +
                      "Data in a materialized view always expires at the same time as " +
                      "the corresponding data in the parent table. default_time_to_live " +
                      "must be set to zero, see CASSANDRA-12868 for more information.");
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
        ViewMetadata view = SchemaManager.instance.getView(keyspaceName, viewName);
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

    public static final class Raw extends RawKeyspaceAwareStatement<AlterViewStatement>
    {
        private final QualifiedName name;
        private final TableAttributes attrs;

        public Raw(QualifiedName name, TableAttributes attrs)
        {
            this.name = name;
            this.attrs = attrs;
        }

        @Override
        public AlterViewStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            String keyspaceName = keyspaceMapper.apply(name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace());
            return new AlterViewStatement(rawCQLStatement, keyspaceName, name.getName(), attrs);
        }
    }
}
