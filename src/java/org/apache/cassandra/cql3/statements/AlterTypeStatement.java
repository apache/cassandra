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
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AlterTypeStatement extends SchemaAlteringStatement
{
    protected final ColumnIdentifier name;

    protected AlterTypeStatement(ColumnIdentifier name)
    {
        super();
        this.name = name;
    }

    protected abstract UserType makeUpdatedType(UserType toUpdate) throws InvalidRequestException;

    public static AlterTypeStatement addition(ColumnIdentifier name, ColumnIdentifier fieldName, CQL3Type type)
    {
        return new AddOrAlter(name, true, fieldName, type);
    }

    public static AlterTypeStatement alter(ColumnIdentifier name, ColumnIdentifier fieldName, CQL3Type type)
    {
        return new AddOrAlter(name, false, fieldName, type);
    }

    public static AlterTypeStatement renames(ColumnIdentifier name, Map<ColumnIdentifier, ColumnIdentifier> renames)
    {
        return new Renames(name, renames);
    }

    public static AlterTypeStatement typeRename(ColumnIdentifier name, ColumnIdentifier newName)
    {
        return new TypeRename(name, newName);
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        // We may want a slightly different permission?
        state.hasAllKeyspacesAccess(Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // Validation is left to announceMigration as it's easier to do it while constructing the updated type.
        // It doesn't really change anything anyway.
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }

    @Override
    public String keyspace()
    {
        // Kind of ugly, but SchemaAlteringStatement uses that for notifying change, and an empty keyspace
        // there kind of make sense
        return "";
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        UserType toUpdate = Schema.instance.userTypes.getType(name);
        // Shouldn't happen, unless we race with a drop
        if (toUpdate == null)
            throw new InvalidRequestException(String.format("No user type named %s exists.", name));

        UserType updated = makeUpdatedType(toUpdate);

        // Now, we need to announce the type update to basically change it for new tables using this type,
        // but we also need to find all existing user types and CF using it and change them.
        MigrationManager.announceTypeUpdate(updated);

        for (KSMetaData ksm : Schema.instance.getKeyspaceDefinitions())
        {
            for (CFMetaData cfm : ksm.cfMetaData().values())
            {
                CFMetaData copy = cfm.clone();
                boolean modified = false;
                for (ColumnDefinition def : copy.allColumns())
                    modified |= updateDefinition(copy, def, toUpdate.name, updated);
                if (modified)
                    MigrationManager.announceColumnFamilyUpdate(copy, false);
            }
        }

        // Other user types potentially using the updated type
        for (UserType ut : Schema.instance.userTypes.getAllTypes().values())
        {
            // Re-updating the type we've just updated would be harmless but useless so we avoid it.
            // Besides, we use the occasion to drop the old version of the type if it's a type rename
            if (ut.name.equals(toUpdate.name))
            {
                if (!ut.name.equals(updated.name))
                    MigrationManager.announceTypeDrop(ut);
                continue;
            }
            AbstractType<?> upd = updateWith(ut, toUpdate.name, updated);
            if (upd != null)
                MigrationManager.announceTypeUpdate((UserType)upd);
        }
    }

    private static int getIdxOfField(UserType type, ColumnIdentifier field)
    {
        for (int i = 0; i < type.types.size(); i++)
            if (field.bytes.equals(type.columnNames.get(i)))
                return i;
        return -1;
    }

    private boolean updateDefinition(CFMetaData cfm, ColumnDefinition def, ByteBuffer toReplace, UserType updated)
    {
        AbstractType<?> t = updateWith(def.type, toReplace, updated);
        if (t == null)
            return false;

        // We need to update this validator ...
        cfm.addOrReplaceColumnDefinition(def.withNewType(t));

        // ... but if it's part of the comparator or key validator, we need to go update those too.
        switch (def.kind)
        {
            case PARTITION_KEY:
                cfm.keyValidator(updateWith(cfm.getKeyValidator(), toReplace, updated));
                break;
            case CLUSTERING_COLUMN:
                cfm.comparator = updateWith(cfm.comparator, toReplace, updated);
                break;
        }
        return true;
    }

    // Update the provided type were all instance of a given userType is replaced by a new version
    // Note that this methods reaches inside other UserType, CompositeType and CollectionType.
    private static AbstractType<?> updateWith(AbstractType<?> type, ByteBuffer toReplace, UserType updated)
    {
        if (type instanceof UserType)
        {
            UserType ut = (UserType)type;

            // If it's directly the type we've updated, then just use the new one.
            if (toReplace.equals(ut.name))
                return updated;

            // Otherwise, check for nesting
            List<AbstractType<?>> updatedTypes = updateTypes(ut.types, toReplace, updated);
            return updatedTypes == null ? null : new UserType(ut.name, new ArrayList<>(ut.columnNames), updatedTypes);
        }
        else if (type instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)type;
            List<AbstractType<?>> updatedTypes = updateTypes(ct.types, toReplace, updated);
            return updatedTypes == null ? null : CompositeType.getInstance(updatedTypes);
        }
        else if (type instanceof ColumnToCollectionType)
        {
            ColumnToCollectionType ctct = (ColumnToCollectionType)type;
            Map<ByteBuffer, CollectionType> updatedTypes = null;
            for (Map.Entry<ByteBuffer, CollectionType> entry : ctct.defined.entrySet())
            {
                AbstractType<?> t = updateWith(entry.getValue(), toReplace, updated);
                if (t == null)
                    continue;

                if (updatedTypes == null)
                    updatedTypes = new HashMap<>(ctct.defined);

                updatedTypes.put(entry.getKey(), (CollectionType)t);
            }
            return updatedTypes == null ? null : ColumnToCollectionType.getInstance(updatedTypes);
        }
        else if (type instanceof CollectionType)
        {
            if (type instanceof ListType)
            {
                AbstractType<?> t = updateWith(((ListType)type).elements, toReplace, updated);
                return t == null ? null : ListType.getInstance(t);
            }
            else if (type instanceof SetType)
            {
                AbstractType<?> t = updateWith(((SetType)type).elements, toReplace, updated);
                return t == null ? null : SetType.getInstance(t);
            }
            else
            {
                assert type instanceof MapType;
                MapType mt = (MapType)type;
                AbstractType<?> k = updateWith(mt.keys, toReplace, updated);
                AbstractType<?> v = updateWith(mt.values, toReplace, updated);
                if (k == null && v == null)
                    return null;
                return MapType.getInstance(k == null ? mt.keys : k, v == null ? mt.values : v);
            }
        }
        else
        {
            return null;
        }
    }

    private static List<AbstractType<?>> updateTypes(List<AbstractType<?>> toUpdate, ByteBuffer toReplace, UserType updated)
    {
        // But this can also be nested.
        List<AbstractType<?>> updatedTypes = null;
        for (int i = 0; i < toUpdate.size(); i++)
        {
            AbstractType<?> t = updateWith(toUpdate.get(i), toReplace, updated);
            if (t == null)
                continue;

            if (updatedTypes == null)
                updatedTypes = new ArrayList<>(toUpdate);

            updatedTypes.set(i, t);
        }
        return updatedTypes;
    }

    private static class AddOrAlter extends AlterTypeStatement
    {
        private final boolean isAdd;
        private final ColumnIdentifier fieldName;
        private final CQL3Type type;

        public AddOrAlter(ColumnIdentifier name, boolean isAdd, ColumnIdentifier fieldName, CQL3Type type)
        {
            super(name);
            this.isAdd = isAdd;
            this.fieldName = fieldName;
            this.type = type;
        }

        private UserType doAdd(UserType toUpdate) throws InvalidRequestException
        {
            if (getIdxOfField(toUpdate, fieldName) >= 0)
                throw new InvalidRequestException(String.format("Cannot add new field %s to type %s: a field of the same name already exists", fieldName, name));

            List<ByteBuffer> newNames = new ArrayList<>(toUpdate.columnNames.size() + 1);
            newNames.addAll(toUpdate.columnNames);
            newNames.add(fieldName.bytes);

            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.types.size() + 1);
            newTypes.addAll(toUpdate.types);
            newTypes.add(type.getType());

            return new UserType(toUpdate.name, newNames, newTypes);
        }

        private UserType doAlter(UserType toUpdate) throws InvalidRequestException
        {
            int idx = getIdxOfField(toUpdate, fieldName);
            if (idx < 0)
                throw new InvalidRequestException(String.format("Unknown field %s in type %s", fieldName, name));

            AbstractType<?> previous = toUpdate.types.get(idx);
            if (!type.getType().isCompatibleWith(previous))
                throw new InvalidRequestException(String.format("Type %s is incompatible with previous type %s of field %s in user type %s", type, previous.asCQL3Type(), fieldName, name));

            List<ByteBuffer> newNames = new ArrayList<>(toUpdate.columnNames);
            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.types);
            newTypes.set(idx, type.getType());

            return new UserType(toUpdate.name, newNames, newTypes);
        }

        protected UserType makeUpdatedType(UserType toUpdate) throws InvalidRequestException
        {
            return isAdd ? doAdd(toUpdate) : doAlter(toUpdate);
        }
    }

    private static class Renames extends AlterTypeStatement
    {
        private final Map<ColumnIdentifier, ColumnIdentifier> renames;

        public Renames(ColumnIdentifier name, Map<ColumnIdentifier, ColumnIdentifier> renames)
        {
            super(name);
            this.renames = renames;
        }

        protected UserType makeUpdatedType(UserType toUpdate) throws InvalidRequestException
        {
            List<ByteBuffer> newNames = new ArrayList<>(toUpdate.columnNames);
            List<AbstractType<?>> newTypes = new ArrayList<>(toUpdate.types);

            for (Map.Entry<ColumnIdentifier, ColumnIdentifier> entry : renames.entrySet())
            {
                ColumnIdentifier from = entry.getKey();
                ColumnIdentifier to = entry.getValue();
                int idx = getIdxOfField(toUpdate, from);
                if (idx < 0)
                    throw new InvalidRequestException(String.format("Unknown field %s in type %s", from, name));
                newNames.set(idx, to.bytes);
            }

            UserType updated = new UserType(toUpdate.name, newNames, newTypes);
            CreateTypeStatement.checkForDuplicateNames(updated);
            return updated;
        }

    }

    private static class TypeRename extends AlterTypeStatement
    {
        private final ColumnIdentifier newName;

        public TypeRename(ColumnIdentifier name, ColumnIdentifier newName)
        {
            super(name);
            this.newName = newName;
        }

        protected UserType makeUpdatedType(UserType toUpdate) throws InvalidRequestException
        {
            UserType previous = Schema.instance.userTypes.getType(newName.bytes);
            if (previous != null)
                throw new InvalidRequestException(String.format("Cannot rename user type %s to %s as another type of that name exists", name, newName));

            return new UserType(newName.bytes, new ArrayList<>(toUpdate.columnNames), new ArrayList<>(toUpdate.types));
        }
    }
}
