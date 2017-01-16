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

import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class CreateTypeStatement extends SchemaAlteringStatement
{
    private final UTName name;
    private final List<FieldIdentifier> columnNames = new ArrayList<>();
    private final List<CQL3Type.Raw> columnTypes = new ArrayList<>();
    private final boolean ifNotExists;

    public CreateTypeStatement(UTName name, boolean ifNotExists)
    {
        super();
        this.name = name;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!name.hasKeyspace())
            name.setKeyspace(state.getKeyspace());
    }

    public void addDefinition(FieldIdentifier name, CQL3Type.Raw type)
    {
        columnNames.add(name);
        columnTypes.add(type);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.CREATE);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(name.getKeyspace());
        if (ksm == null)
            throw new InvalidRequestException(String.format("Cannot add type in unknown keyspace %s", name.getKeyspace()));

        if (ksm.types.get(name.getUserTypeName()).isPresent() && !ifNotExists)
            throw new InvalidRequestException(String.format("A user type of name %s already exists", name));

        for (CQL3Type.Raw type : columnTypes)
        {
            if (type.isCounter())
                throw new InvalidRequestException("A user type cannot contain counters");
            if (type.isUDT() && !type.isFrozen())
                throw new InvalidRequestException("A user type cannot contain non-frozen UDTs");
        }
    }

    public static void checkForDuplicateNames(UserType type) throws InvalidRequestException
    {
        for (int i = 0; i < type.size() - 1; i++)
        {
            FieldIdentifier fieldName = type.fieldName(i);
            for (int j = i+1; j < type.size(); j++)
            {
                if (fieldName.equals(type.fieldName(j)))
                    throw new InvalidRequestException(String.format("Duplicate field name %s in type %s", fieldName, type.name));
            }
        }
    }

    public void addToRawBuilder(Types.RawBuilder builder) throws InvalidRequestException
    {
        builder.add(name.getStringTypeName(),
                    columnNames.stream().map(FieldIdentifier::toString).collect(Collectors.toList()),
                    columnTypes.stream().map(CQL3Type.Raw::toString).collect(Collectors.toList()));
    }

    @Override
    public String keyspace()
    {
        return name.getKeyspace();
    }

    public UserType createType() throws InvalidRequestException
    {
        List<AbstractType<?>> types = new ArrayList<>(columnTypes.size());
        for (CQL3Type.Raw type : columnTypes)
            types.add(type.prepare(keyspace()).getType());

        return new UserType(name.getKeyspace(), name.getUserTypeName(), columnNames, types, true);
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(name.getKeyspace());
        assert ksm != null; // should haven't validate otherwise

        // Can happen with ifNotExists
        if (ksm.types.get(name.getUserTypeName()).isPresent())
            return null;

        UserType type = createType();
        checkForDuplicateNames(type);
        MigrationManager.announceNewType(type, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TYPE, keyspace(), name.getStringTypeName());
    }
}
