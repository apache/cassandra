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

import java.nio.ByteBuffer;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;
import org.apache.cassandra.transport.Event.SchemaChange;

import static java.lang.String.join;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public final class DropTypeStatement extends AlterSchemaStatement
{
    private final String typeName;
    private final boolean ifExists;

    public DropTypeStatement(String keyspaceName, String typeName, boolean ifExists)
    {
        super(keyspaceName);
        this.typeName = typeName;
        this.ifExists = ifExists;
    }

    // TODO: expand types into tuples in all dropped columns of all tables
    public Keyspaces apply(Keyspaces schema)
    {
        ByteBuffer name = bytes(typeName);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        UserType type = null == keyspace
                      ? null
                      : keyspace.types.getNullable(name);

        if (null == type)
        {
            if (ifExists)
                return schema;

            throw ire("Type '%s.%s' doesn't exist", keyspaceName, typeName);
        }

        /*
         * We don't want to drop a type unless it's not used anymore (mainly because
         * if someone drops a type and recreates one with the same name but different
         * definition with the previous name still in use, things can get messy).
         * We have three places to check:
         * 1) UDFs and UDAs using the type
         * 2) other user type that can nest the one we drop and
         * 3) existing tables referencing the type (maybe in a nested way).
         */
        Iterable<UserFunction> functions = keyspace.userFunctions.referencingUserType(name);
        if (!isEmpty(functions))
        {
            throw ire("Cannot drop user type '%s.%s' as it is still used by functions %s",
                      keyspaceName,
                      typeName,
                      join(", ", transform(functions, f -> f.name().toString())));
        }

        Iterable<UserType> types = keyspace.types.referencingUserType(name);
        if (!isEmpty(types))
        {
            throw ire("Cannot drop user type '%s.%s' as it is still used by user types %s",
                      keyspaceName,
                      typeName,
                      join(", ", transform(types, UserType::getNameAsString)));

        }

        Iterable<TableMetadata> tables = keyspace.tables.referencingUserType(name);
        if (!isEmpty(tables))
        {
            throw ire("Cannot drop user type '%s.%s' as it is still used by tables %s",
                      keyspaceName,
                      typeName,
                      join(", ", transform(tables, t -> t.name)));
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.types.without(type)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.DROPPED, Target.TYPE, keyspaceName, typeName);
    }

    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.DROP);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_TYPE, keyspaceName, typeName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, typeName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final UTName name;
        private final boolean ifExists;

        public Raw(UTName name, boolean ifExists)
        {
            this.name = name;
            this.ifExists = ifExists;
        }

        public DropTypeStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new DropTypeStatement(keyspaceName, name.getStringTypeName(), ifExists);
        }
    }
}
