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

import java.util.*;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UTName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import static java.util.stream.Collectors.toList;

public final class CreateTypeStatement extends AlterSchemaStatement
{
    private final String typeName;
    private final List<FieldIdentifier> fieldNames;
    private final List<CQL3Type.Raw> rawFieldTypes;
    private final boolean ifNotExists;

    public CreateTypeStatement(String keyspaceName,
                               String typeName,
                               List<FieldIdentifier> fieldNames,
                               List<CQL3Type.Raw> rawFieldTypes,
                               boolean ifNotExists)
    {
        super(keyspaceName);
        this.typeName = typeName;
        this.fieldNames = fieldNames;
        this.rawFieldTypes = rawFieldTypes;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        Guardrails.fieldsPerUDT.guard(fieldNames.size(), typeName, false, state);

        for (int i = 0; i < rawFieldTypes.size(); i++)
        {
            rawFieldTypes.get(i).validate(state, "Field " + fieldNames.get(i));
        }
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        UserType existingType = keyspace.types.getNullable(bytes(typeName));
        if (null != existingType)
        {
            if (ifNotExists)
                return schema;

            throw ire("A user type with name '%s' already exists", typeName);
        }

        Set<FieldIdentifier> usedNames = new HashSet<>();
        for (FieldIdentifier name : fieldNames)
            if (!usedNames.add(name))
                throw ire("Duplicate field name '%s' in type '%s'", name, typeName);

        for (CQL3Type.Raw type : rawFieldTypes)
        {
            if (type.isCounter())
                throw ire("A user type cannot contain counters");

            if (type.isUDT() && !type.isFrozen())
                throw ire("A user type cannot contain non-frozen UDTs");
        }

        List<AbstractType<?>> fieldTypes =
            rawFieldTypes.stream()
                         .map(t -> t.prepare(keyspaceName, keyspace.types).getType())
                         .collect(toList());

        UserType udt = new UserType(keyspaceName, bytes(typeName), fieldNames, fieldTypes, true);
        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.types.with(udt)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.CREATED, Target.TYPE, keyspaceName, typeName);
    }

    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.CREATE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TYPE, keyspaceName, typeName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, typeName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final UTName name;
        private final boolean ifNotExists;

        private final List<FieldIdentifier> fieldNames = new ArrayList<>();
        private final List<CQL3Type.Raw> rawFieldTypes = new ArrayList<>();

        public Raw(UTName name, boolean ifNotExists)
        {
            this.name = name;
            this.ifNotExists = ifNotExists;
        }

        public CreateTypeStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            return new CreateTypeStatement(keyspaceName, name.getStringTypeName(), fieldNames, rawFieldTypes, ifNotExists);
        }

        public void addField(FieldIdentifier name, CQL3Type.Raw type)
        {
            fieldNames.add(name);
            rawFieldTypes.add(type);
        }

        public void addToRawBuilder(Types.RawBuilder builder)
        {
            builder.add(name.getStringTypeName(),
                        fieldNames.stream().map(FieldIdentifier::toString).collect(toList()),
                        rawFieldTypes.stream().map(CQL3Type.Raw::toString).collect(toList()));
        }
    }
}
