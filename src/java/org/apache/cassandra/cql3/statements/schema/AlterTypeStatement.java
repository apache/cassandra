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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.lang.String.join;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public abstract class AlterTypeStatement extends AlterSchemaStatement
{
    protected final String typeName;

    public AlterTypeStatement(String keyspaceName, String typeName)
    {
        super(keyspaceName);
        this.typeName = typeName;
    }

    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.ALTER);
    }

    SchemaChange schemaChangeEvent(Keyspaces.KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TYPE, keyspaceName, typeName);
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        UserType type = null == keyspace
                      ? null
                      : keyspace.types.getNullable(bytes(typeName));

        if (null == type)
            throw ire("Type %s.%s doesn't exist", keyspaceName, typeName);

        return schema.withAddedOrUpdated(keyspace.withUpdatedUserType(apply(keyspace, type)));
    }

    abstract UserType apply(KeyspaceMetadata keyspace, UserType type);

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_TYPE, keyspaceName, typeName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, typeName);
    }

    private static final class AddField extends AlterTypeStatement
    {
        private final FieldIdentifier fieldName;
        private final CQL3Type.Raw type;

        private AddField(String keyspaceName, String typeName, FieldIdentifier fieldName, CQL3Type.Raw type)
        {
            super(keyspaceName, typeName);
            this.fieldName = fieldName;
            this.type = type;
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            if (userType.fieldPosition(fieldName) >= 0)
                throw ire("Cannot add field %s to type %s: a field with name %s already exists", fieldName, userType.toCQLString(), fieldName);

            AbstractType<?> fieldType = type.prepare(keyspaceName, keyspace.types).getType();
            if (fieldType.referencesUserType(userType.name))
                throw ire("Cannot add new field %s of type %s to user type %s as it would create a circular reference", fieldName, type, userType.toCQLString());

            List<FieldIdentifier> fieldNames = new ArrayList<>(userType.fieldNames()); fieldNames.add(fieldName);
            List<AbstractType<?>> fieldTypes = new ArrayList<>(userType.fieldTypes()); fieldTypes.add(fieldType);

            return new UserType(keyspaceName, userType.name, fieldNames, fieldTypes, true);
        }
    }

    private static final class RenameFields extends AlterTypeStatement
    {
        private final Map<FieldIdentifier, FieldIdentifier> renamedFields;

        private RenameFields(String keyspaceName, String typeName, Map<FieldIdentifier, FieldIdentifier> renamedFields)
        {
            super(keyspaceName, typeName);
            this.renamedFields = renamedFields;
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            List<String> dependentAggregates =
                keyspace.functions
                        .udas()
                        .filter(uda -> null != uda.initialCondition() && uda.stateType().referencesUserType(userType.name))
                        .map(uda -> uda.name().toString())
                        .collect(toList());

            if (!dependentAggregates.isEmpty())
            {
                throw ire("Cannot alter user type %s as it is still used in INITCOND by aggregates %s",
                          userType.toCQLString(),
                          join(", ", dependentAggregates));
            }

            List<FieldIdentifier> fieldNames = new ArrayList<>(userType.fieldNames());

            renamedFields.forEach((oldName, newName) ->
            {
                int idx = userType.fieldPosition(oldName);
                if (idx < 0)
                    throw ire("Unkown field %s in user type %s", oldName, keyspaceName, userType.toCQLString());
                fieldNames.set(idx, newName);
            });

            fieldNames.forEach(name ->
            {
                if (fieldNames.stream().filter(isEqual(name)).count() > 1)
                    throw ire("Duplicate field name %s in type %s", name, keyspaceName, userType.toCQLString());
            });

            return new UserType(keyspaceName, userType.name, fieldNames, userType.fieldTypes(), true);
        }
    }

    private static final class AlterField extends AlterTypeStatement
    {
        private AlterField(String keyspaceName, String typeName)
        {
            super(keyspaceName, typeName);
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            throw ire("Alterting field types is no longer supported");
        }
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private enum Kind
        {
            ADD_FIELD, RENAME_FIELDS, ALTER_FIELD
        }

        private final UTName name;

        private Kind kind;

        // ADD
        private FieldIdentifier newFieldName;
        private CQL3Type.Raw newFieldType;

        // RENAME
        private final Map<FieldIdentifier, FieldIdentifier> renamedFields = new HashMap<>();

        public Raw(UTName name)
        {
            this.name = name;
        }

        public AlterTypeStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            String typeName = name.getStringTypeName();

            switch (kind)
            {
                case     ADD_FIELD: return new AddField(keyspaceName, typeName, newFieldName, newFieldType);
                case RENAME_FIELDS: return new RenameFields(keyspaceName, typeName, renamedFields);
                case   ALTER_FIELD: return new AlterField(keyspaceName, typeName);
            }

            throw new AssertionError();
        }

        public void add(FieldIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ADD_FIELD;
            newFieldName = name;
            newFieldType = type;
        }

        public void rename(FieldIdentifier from, FieldIdentifier to)
        {
            kind = Kind.RENAME_FIELDS;
            renamedFields.put(from, to);
        }

        public void alter(FieldIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ALTER_FIELD;
        }
    }
}
