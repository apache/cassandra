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
package org.apache.cassandra.cql3.statements;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

public class DropTypeStatement extends SchemaAlteringStatement
{
    private final UTName name;
    private final boolean ifExists;

    public DropTypeStatement(UTName name, boolean ifExists)
    {
        this.name = name;
        this.ifExists = ifExists;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!name.hasKeyspace())
            name.setKeyspace(state.getKeyspace());
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(name.getKeyspace());
        if (ksm == null)
        {
            if (ifExists)
                return;
            else
                throw new InvalidRequestException(String.format("Cannot drop type in unknown keyspace %s", name.getKeyspace()));
        }

        if (!ksm.types.get(name.getUserTypeName()).isPresent())
        {
            if (ifExists)
                return;
            else
                throw new InvalidRequestException(String.format("No user type named %s exists.", name));
        }

        // We don't want to drop a type unless it's not used anymore (mainly because
        // if someone drops a type and recreates one with the same name but different
        // definition with the previous name still in use, things can get messy).
        // We have two places to check: 1) other user type that can nest the one
        // we drop and 2) existing tables referencing the type (maybe in a nested
        // way).

        for (Function function : ksm.functions)
        {
            if (function.returnType().referencesUserType(name.getStringTypeName()))
                throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", name, function));

            for (AbstractType<?> argType : function.argTypes())
                if (argType.referencesUserType(name.getStringTypeName()))
                    throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by function %s", name, function));
        }

        for (UserType ut : ksm.types)
            if (!ut.name.equals(name.getUserTypeName()) && ut.referencesUserType(name.getStringTypeName()))
                throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by user type %s", name, ut.getNameAsString()));

        for (TableMetadata table : ksm.tablesAndViews())
            for (ColumnMetadata def : table.columns())
                if (def.type.referencesUserType(name.getStringTypeName()))
                    throw new InvalidRequestException(String.format("Cannot drop user type %s as it is still used by table %s", name, table.toString()));
    }

    @Override
    public String keyspace()
    {
        return name.getKeyspace();
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(name.getKeyspace());
        if (ksm == null)
            return null; // do not assert (otherwise IF EXISTS case fails)

        UserType toDrop = ksm.types.getNullable(name.getUserTypeName());
        // Can be null with ifExists
        if (toDrop == null)
            return null;

        MigrationManager.announceTypeDrop(toDrop, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, Event.SchemaChange.Target.TYPE, keyspace(), name.getStringTypeName());
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
