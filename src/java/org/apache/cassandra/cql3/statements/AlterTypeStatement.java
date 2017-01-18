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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public abstract class AlterTypeStatement extends SchemaAlteringStatement
{
    protected final UTName name;

    protected AlterTypeStatement(UTName name)
    {
        this.name = name;
    }

    @Override
    public void prepareKeyspace(ClientState state) throws InvalidRequestException
    {
        if (!name.hasKeyspace())
            name.setKeyspace(state.getKeyspace());

        if (name.getKeyspace() == null)
            throw new InvalidRequestException("You need to be logged in a keyspace or use a fully qualified user type name");
    }

    protected abstract UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException;

    public static AlterTypeStatement addition(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
    {
        return new Add(name, fieldName, type);
    }

    public static AlterTypeStatement alter(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
    {
        throw new InvalidRequestException("Altering of types is not allowed");
    }

    public static AlterTypeStatement renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames)
    {
        return new Renames(name, renames);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // Validation is left to announceMigration as it's easier to do it while constructing the updated type.
        // It doesn't really change anything anyway.
    }

    @Override
    public String keyspace()
    {
        return name.getKeyspace();
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws InvalidRequestException, ConfigurationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(name.getKeyspace());
        if (ksm == null)
            throw new InvalidRequestException(String.format("Cannot alter type in unknown keyspace %s", name.getKeyspace()));

        UserType toUpdate =
            ksm.types.get(name.getUserTypeName())
                     .orElseThrow(() -> new InvalidRequestException(String.format("No user type named %s exists.", name)));

        UserType updated = makeUpdatedType(toUpdate, ksm);

        // Now, we need to announce the type update to basically change it for new tables using this type,
        // but we also need to find all existing user types and CF using it and change them.
        MigrationManager.announceTypeUpdate(updated, isLocalOnly);

        for (CFMetaData cfm : ksm.tables)
        {
            CFMetaData copy = cfm.copy();
            boolean modified = false;
            for (ColumnDefinition def : copy.allColumns())
                modified |= updateDefinition(copy, def, toUpdate.keyspace, toUpdate.name, updated);
            if (modified)
                MigrationManager.announceColumnFamilyUpdate(copy, isLocalOnly);
        }

        for (ViewDefinition view : ksm.views)
        {
            ViewDefinition copy = view.copy();
            boolean modified = false;
            for (ColumnDefinition def : copy.metadata.allColumns())
                modified |= updateDefinition(copy.metadata, def, toUpdate.keyspace, toUpdate.name, updated);
            if (modified)
                MigrationManager.announceViewUpdate(copy, isLocalOnly);
        }

        // Other user types potentially using the updated type
        for (UserType ut : ksm.types)
        {
            // Re-updating the type we've just updated would be harmless but useless so we avoid it.
            // Besides, we use the occasion to drop the old version of the type if it's a type rename
            if (ut.keyspace.equals(toUpdate.keyspace) && ut.name.equals(toUpdate.name))
            {
                if (!ut.keyspace.equals(updated.keyspace) || !ut.name.equals(updated.name))
                    MigrationManager.announceTypeDrop(ut);
                continue;
            }
            AbstractType<?> upd = updateWith(ut, toUpdate.keyspace, toUpdate.name, updated);
            if (upd != null)
                MigrationManager.announceTypeUpdate((UserType) upd, isLocalOnly);
        }
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TYPE, keyspace(), name.getStringTypeName());
    }

    private boolean updateDefinition(CFMetaData cfm, ColumnDefinition def, String keyspace, ByteBuffer toReplace, UserType updated)
    {
        AbstractType<?> t = updateWith(def.type, keyspace, toReplace, updated);
        if (t == null)
            return false;

        // We need to update this validator ...
        cfm.addOrReplaceColumnDefinition(def.withNewType(t));
        return true;
    }

    // Update the provided type were all instance of a given userType is replaced by a new version
    // Note that this methods reaches inside other UserType, CompositeType and CollectionType.
    private static AbstractType<?> updateWith(AbstractType<?> type, String keyspace, ByteBuffer toReplace, UserType updated)
    {
        if (type instanceof UserType)
        {
            UserType ut = (UserType)type;

            // If it's directly the type we've updated, then just use the new one.
            if (keyspace.equals(ut.keyspace) && toReplace.equals(ut.name))
                return type.isMultiCell() ? updated : updated.freeze();

            // Otherwise, check for nesting
            List<AbstractType<?>> updatedTypes = updateTypes(ut.fieldTypes(), keyspace, toReplace, updated);
            return updatedTypes == null ? null : new UserType(ut.keyspace, ut.name, new ArrayList<>(ut.fieldNames()), updatedTypes, type.isMultiCell());
        }
        else if (type instanceof TupleType)
        {
            TupleType tt = (TupleType)type;
            List<AbstractType<?>> updatedTypes = updateTypes(tt.allTypes(), keyspace, toReplace, updated);
            return updatedTypes == null ? null : new TupleType(updatedTypes);
        }
        else if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)type;
            List<AbstractType<?>> updatedTypes = updateTypes(ct.types, keyspace, toReplace, updated);
            return updatedTypes == null ? null : CompositeType.getInstance(updatedTypes);
        }
        else if (type instanceof CollectionType)
        {
            if (type instanceof ListType)
            {
                AbstractType<?> t = updateWith(((ListType)type).getElementsType(), keyspace, toReplace, updated);
                if (t == null)
                    return null;
                return ListType.getInstance(t, type.isMultiCell());
            }
            else if (type instanceof SetType)
            {
                AbstractType<?> t = updateWith(((SetType)type).getElementsType(), keyspace, toReplace, updated);
                if (t == null)
                    return null;
                return SetType.getInstance(t, type.isMultiCell());
            }
            else
            {
                assert type instanceof MapType;
                MapType mt = (MapType)type;
                AbstractType<?> k = updateWith(mt.getKeysType(), keyspace, toReplace, updated);
                AbstractType<?> v = updateWith(mt.getValuesType(), keyspace, toReplace, updated);
                if (k == null && v == null)
                    return null;
                return MapType.getInstance(k == null ? mt.getKeysType() : k, v == null ? mt.getValuesType() : v, type.isMultiCell());
            }
        }
        else
        {
            return null;
        }
    }

    private static List<AbstractType<?>> updateTypes(List<AbstractType<?>> toUpdate, String keyspace, ByteBuffer toReplace, UserType updated)
    {
        // But this can also be nested.
        List<AbstractType<?>> updatedTypes = null;
        for (int i = 0; i < toUpdate.size(); i++)
        {
            AbstractType<?> t = updateWith(toUpdate.get(i), keyspace, toReplace, updated);
            if (t == null)
                continue;

            if (updatedTypes == null)
                updatedTypes = new ArrayList<>(toUpdate);

            updatedTypes.set(i, t);
        }
        return updatedTypes;
    }

    protected void checkTypeNotUsedByAggregate(KeyspaceMetadata ksm)
    {
        ksm.functions.udas().filter(aggregate -> aggregate.initialCondition() != null && aggregate.stateType().referencesUserType(name.getStringTypeName()))
                     .findAny()
                     .ifPresent((aggregate) -> {
                         throw new InvalidRequestException(String.format("Cannot alter user type %s as it is still used as an INITCOND by aggregate %s", name, aggregate));
                     });
    }

    private static class Add extends AlterTypeStatement
    {
        private final FieldIdentifier fieldName;
        private final CQL3Type.Raw type;

        public Add(UTName name, FieldIdentifier fieldName, CQL3Type.Raw type)
        {
            super(name);
            this.fieldName = fieldName;
            this.type = type;
        }

        protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException
        {
            if (toUpdate.fieldPosition(fieldName) >= 0)
                throw new InvalidRequestException(String.format("Cannot add new field %s to type %s: a field of the same name already exists", fieldName, name));

            List<FieldIdentifier> newNames = new ArrayList<>(toUpdate.size() + 1);
            newNames.addAll(toUpdate.fieldNames());
            newNames.add(fieldName);

            AbstractType<?> addType = type.prepare(keyspace()).getType();
            if (addType.referencesUserType(toUpdate.getNameAsString()))
                throw new InvalidRequestException(String.format("Cannot add new field %s of type %s to type %s as this would create a circular reference", fieldName, type, name));

            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.size() + 1);
            newTypes.addAll(toUpdate.fieldTypes());
            newTypes.add(addType);

            return new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
        }
    }

    private static class Renames extends AlterTypeStatement
    {
        private final Map<FieldIdentifier, FieldIdentifier> renames;

        public Renames(UTName name, Map<FieldIdentifier, FieldIdentifier> renames)
        {
            super(name);
            this.renames = renames;
        }

        protected UserType makeUpdatedType(UserType toUpdate, KeyspaceMetadata ksm) throws InvalidRequestException
        {
            checkTypeNotUsedByAggregate(ksm);

            List<FieldIdentifier> newNames = new ArrayList<>(toUpdate.fieldNames());
            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.fieldTypes());

            for (Map.Entry<FieldIdentifier, FieldIdentifier> entry : renames.entrySet())
            {
                FieldIdentifier from = entry.getKey();
                FieldIdentifier to = entry.getValue();
                int idx = toUpdate.fieldPosition(from);
                if (idx < 0)
                    throw new InvalidRequestException(String.format("Unknown field %s in type %s", from, name));
                newNames.set(idx, to);
            }

            UserType updated = new UserType(toUpdate.keyspace, toUpdate.name, newNames, newTypes, toUpdate.isMultiCell());
            CreateTypeStatement.checkForDuplicateNames(updated);
            return updated;
        }

    }
}
