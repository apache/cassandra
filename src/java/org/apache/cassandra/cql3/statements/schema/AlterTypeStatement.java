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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.join;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public abstract class AlterTypeStatement extends AlterSchemaStatement
{
    protected final String typeName;
    protected final boolean ifExists;

    public AlterTypeStatement(String keyspaceName, String typeName, boolean ifExists)
    {
        super(keyspaceName);
        this.ifExists = ifExists;
        this.typeName = typeName;
    }

    public void authorize(ClientState client)
    {
        client.ensureAllTablesPermission(keyspaceName, Permission.ALTER);
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
        {
            if (!ifExists)
                throw ire("Type %s.%s doesn't exist", keyspaceName, typeName);
            return schema;
        }

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
        private final boolean ifFieldNotExists;

        private ClientState state;

        private AddField(String keyspaceName, String typeName, FieldIdentifier fieldName, CQL3Type.Raw type, boolean ifExists, boolean ifFieldNotExists)
        {
            super(keyspaceName, typeName, ifExists);
            this.fieldName = fieldName;
            this.ifFieldNotExists = ifFieldNotExists;
            this.type = type;
        }

        @Override
        public void validate(ClientState state)
        {
            super.validate(state);

            // save the query state to use it for guardrails validation in #apply
            this.state = state;
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            if (type.isCounter())
                throw ire("A user type cannot contain counters");

            if (type.isUDT() && !type.isFrozen())
                throw ire("A user type cannot contain non-frozen UDTs");

            if (userType.fieldPosition(fieldName) >= 0)
            {
                if (!ifFieldNotExists)
                    throw ire("Cannot add field %s to type %s: a field with name %s already exists", fieldName, userType.getCqlTypeName(), fieldName);
                return userType;
            }

            AbstractType<?> fieldType = type.prepare(keyspaceName, keyspace.types).getType();
            if (fieldType.referencesUserType(userType.name))
                throw ire("Cannot add new field %s of type %s to user type %s as it would create a circular reference", fieldName, type, userType.getCqlTypeName());

            Collection<TableMetadata> tablesWithTypeInPartitionKey = findTablesReferencingTypeInPartitionKey(keyspace, userType);
            if (!tablesWithTypeInPartitionKey.isEmpty())
            {
                throw ire("Cannot add new field %s of type %s to user type %s as the type is being used in partition key by the following tables: %s",
                          fieldName, type, userType.getCqlTypeName(),
                          String.join(", ", transform(tablesWithTypeInPartitionKey, TableMetadata::toString)));
            }

            Guardrails.fieldsPerUDT.guard(userType.size() + 1, userType.getNameAsString(), false, state);
            type.validate(state, "Field " + fieldName);

            List<FieldIdentifier> fieldNames = new ArrayList<>(userType.fieldNames()); fieldNames.add(fieldName);
            List<AbstractType<?>> fieldTypes = new ArrayList<>(userType.fieldTypes()); fieldTypes.add(fieldType);

            return new UserType(keyspaceName, userType.name, fieldNames, fieldTypes, true);
        }

        private static Collection<TableMetadata> findTablesReferencingTypeInPartitionKey(KeyspaceMetadata keyspace, UserType userType)
        {
            Collection<TableMetadata> tables = new ArrayList<>();
            filter(keyspace.tablesAndViews(),
                   table -> any(table.partitionKeyColumns(), column -> column.type.referencesUserType(userType.name)))
                  .forEach(tables::add);
            return tables;
        }
    }

    private static final class RenameFields extends AlterTypeStatement
    {
        private final Map<FieldIdentifier, FieldIdentifier> renamedFields;
        private final boolean ifFieldExists;

        private RenameFields(String keyspaceName, String typeName, Map<FieldIdentifier, FieldIdentifier> renamedFields, boolean ifExists, boolean ifFieldExists)
        {
            super(keyspaceName, typeName, ifExists);
            this.ifFieldExists = ifFieldExists;
            this.renamedFields = renamedFields;
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            List<String> dependentAggregates =
                keyspace.userFunctions
                        .udas()
                        .filter(uda -> null != uda.initialCondition() && uda.stateType().referencesUserType(userType.name))
                        .map(uda -> uda.name().toString())
                        .collect(toList());

            if (!dependentAggregates.isEmpty())
            {
                throw ire("Cannot alter user type %s as it is still used in INITCOND by aggregates %s",
                          userType.getCqlTypeName(),
                          join(", ", dependentAggregates));
            }

            List<FieldIdentifier> fieldNames = new ArrayList<>(userType.fieldNames());

            renamedFields.forEach((oldName, newName) ->
            {
                int idx = userType.fieldPosition(oldName);
                if (idx < 0)
                {
                    if (!ifFieldExists)
                        throw ire("Unkown field %s in user type %s", oldName, userType.getCqlTypeName());
                    return;
                }
                fieldNames.set(idx, newName);
            });

            fieldNames.forEach(name ->
            {
                if (fieldNames.stream().filter(isEqual(name)).count() > 1)
                    throw ire("Duplicate field name %s in type %s", name, keyspaceName, userType.getCqlTypeName());
            });

            return new UserType(keyspaceName, userType.name, fieldNames, userType.fieldTypes(), true);
        }
    }

    private static final class AlterField extends AlterTypeStatement
    {
        private AlterField(String keyspaceName, String typeName, boolean ifExists)
        {
            super(keyspaceName, typeName, ifExists);
        }

        UserType apply(KeyspaceMetadata keyspace, UserType userType)
        {
            throw ire("Altering field types is no longer supported");
        }
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private enum Kind
        {
            ADD_FIELD, RENAME_FIELDS, ALTER_FIELD
        }

        private final UTName name;
        private final boolean ifExists;
        private boolean ifFieldExists;
        private boolean ifFieldNotExists;

        private Kind kind;

        // ADD
        private FieldIdentifier newFieldName;
        private CQL3Type.Raw newFieldType;

        // RENAME
        private final Map<FieldIdentifier, FieldIdentifier> renamedFields = new HashMap<>();

        public Raw(UTName name, boolean ifExists)
        {
            this.ifExists = ifExists;
            this.name = name;
        }

        public AlterTypeStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            String typeName = name.getStringTypeName();

            switch (kind)
            {
                case     ADD_FIELD: return new AddField(keyspaceName, typeName, newFieldName, newFieldType, ifExists, ifFieldNotExists);
                case RENAME_FIELDS: return new RenameFields(keyspaceName, typeName, renamedFields, ifExists, ifFieldExists);
                case   ALTER_FIELD: return new AlterField(keyspaceName, typeName, ifExists);
            }

            throw new AssertionError();
        }

        public void add(FieldIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ADD_FIELD;
            newFieldName = name;
            newFieldType = type;
        }

        public void ifFieldNotExists(boolean ifNotExists)
        {
            this.ifFieldNotExists = ifNotExists;
        }

        public void rename(FieldIdentifier from, FieldIdentifier to)
        {
            kind = Kind.RENAME_FIELDS;
            renamedFields.put(from, to);
        }

        public void ifFieldExists(boolean ifExists)
        {
            this.ifFieldExists = ifExists;
        }

        public void alter(FieldIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ALTER_FIELD;
        }
    }
}
