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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

public final class AlterKeyspaceStatement extends AlterSchemaStatement
{
    private final KeyspaceAttributes attrs;

    public AlterKeyspaceStatement(String keyspaceName, KeyspaceAttributes attrs)
    {
        super(keyspaceName);
        this.attrs = attrs;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        attrs.validate();

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        KeyspaceMetadata newKeyspace = keyspace.withSwapped(attrs.asAlteredKeyspaceParams(keyspace.params));

        if (newKeyspace.params.replication.klass.equals(LocalStrategy.class))
            throw ire("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        newKeyspace.params.validate(keyspaceName);

        return schema.withAddedOrUpdated(newKeyspace);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, keyspaceName);
    }

    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    @Override
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        if (diff.isEmpty())
            return ImmutableSet.of();

        KeyspaceDiff keyspaceDiff = diff.altered.get(0);

        AbstractReplicationStrategy before = keyspaceDiff.before.createReplicationStrategy();
        AbstractReplicationStrategy after = keyspaceDiff.after.createReplicationStrategy();

        return before.getReplicationFactor() < after.getReplicationFactor()
             ? ImmutableSet.of("When increasing replication factor you need to run a full (-full) repair to distribute the data.")
             : ImmutableSet.of();
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_KEYSPACE, keyspaceName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final String keyspaceName;
        private final KeyspaceAttributes attrs;

        public Raw(String keyspaceName, KeyspaceAttributes attrs)
        {
            this.keyspaceName = keyspaceName;
            this.attrs = attrs;
        }

        public AlterKeyspaceStatement prepare(ClientState state)
        {
            return new AlterKeyspaceStatement(keyspaceName, attrs);
        }
    }
}
