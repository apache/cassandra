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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public static enum Type
    {
        ADD, ALTER, DROP, OPTS, RENAME
    }

    public final Type oType;
    public final CQL3Type validator;
    public final ColumnIdentifier columnName;
    private final CFPropDefs cfProps;
    private final Map<ColumnIdentifier, ColumnIdentifier> renames;

    public AlterTableStatement(CFName name, Type type, ColumnIdentifier columnName, CQL3Type validator, CFPropDefs cfProps, Map<ColumnIdentifier, ColumnIdentifier> renames)
    {
        super(name);
        this.oType = type;
        this.columnName = columnName;
        this.validator = validator; // used only for ADD/ALTER commands
        this.cfProps = cfProps;
        this.renames = renames;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public void announceMigration() throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.clone();

        CFDefinition cfDef = meta.getCfDef();
        CFDefinition.Name name = columnName == null ? null : cfDef.get(columnName);
        switch (oType)
        {
            case ADD:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot add new column to a compact CF");
                if (name != null)
                {
                    switch (name.kind)
                    {
                        case KEY_ALIAS:
                        case COLUMN_ALIAS:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        case COLUMN_METADATA:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }

                AbstractType<?> type = validator.getType();
                if (type instanceof CollectionType)
                {
                    if (!cfDef.isComposite)
                        throw new InvalidRequestException("Cannot use collection types with non-composite PRIMARY KEY");
                    if (cfDef.cfm.isSuper())
                        throw new InvalidRequestException("Cannot use collection types with Super column family");

                    Map<ByteBuffer, CollectionType> collections = cfDef.hasCollections
                                                                ? new HashMap<ByteBuffer, CollectionType>(cfDef.getCollectionType().defined)
                                                                : new HashMap<ByteBuffer, CollectionType>();

                    collections.put(columnName.key, (CollectionType)type);
                    ColumnToCollectionType newColType = ColumnToCollectionType.getInstance(collections);
                    List<AbstractType<?>> ctypes = new ArrayList<AbstractType<?>>(((CompositeType)cfm.comparator).types);
                    if (cfDef.hasCollections)
                        ctypes.set(ctypes.size() - 1, newColType);
                    else
                        ctypes.add(newColType);
                    cfm.comparator = CompositeType.getInstance(ctypes);
                }

                Integer componentIndex = cfDef.isComposite
                                       ? ((CompositeType)meta.comparator).types.size() - (cfDef.hasCollections ? 2 : 1)
                                       : null;
                cfm.addColumnDefinition(ColumnDefinition.regularDef(columnName.key, type, componentIndex));
                break;

            case ALTER:
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                        AbstractType<?> newType = validator.getType();
                        if (newType instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));
                        if (cfDef.hasCompositeKey)
                        {
                            List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(((CompositeType) cfm.getKeyValidator()).types);
                            newTypes.set(name.position, newType);
                            cfm.keyValidator(CompositeType.getInstance(newTypes));
                        }
                        else
                        {
                            cfm.keyValidator(newType);
                        }
                        break;
                    case COLUMN_ALIAS:
                        assert cfDef.isComposite;
                        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(((CompositeType) cfm.comparator).types);
                        newTypes.set(name.position, validator.getType());
                        cfm.comparator = CompositeType.getInstance(newTypes);
                        break;
                    case VALUE_ALIAS:
                        // See below
                        if (!validator.getType().isCompatibleWith(cfm.getDefaultValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           cfm.getDefaultValidator().asCQL3Type(),
                                                                           validator));
                        cfm.defaultValidator(validator.getType());
                        break;
                    case COLUMN_METADATA:
                        ColumnDefinition column = cfm.getColumnDefinition(columnName.key);
                        // Thrift allows to change a column validator so CFMetaData.validateCompatility will let it slide
                        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                        // allow it for CQL3 (see #5882) so validating it explicitly here
                        if (!validator.getType().isCompatibleWith(column.getValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           column.getValidator().asCQL3Type(),
                                                                           validator));

                        column.setValidator(validator.getType());
                        break;
                }
                break;

            case DROP:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot drop columns from a compact CF");
                if (!cfDef.isComposite)
                    throw new InvalidRequestException("Cannot drop columns from a non-CQL3 CF");
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case COLUMN_METADATA:
                        ColumnDefinition toDelete = null;
                        for (ColumnDefinition columnDef : cfm.regularColumns())
                        {
                            if (columnDef.name.equals(columnName.key))
                                toDelete = columnDef;
                        }
                        assert toDelete != null;
                        cfm.removeColumnDefinition(toDelete);
                        cfm.recordColumnDrop(toDelete);
                        break;
                }
                break;
            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

                cfProps.validate();
                cfProps.applyToCFMetadata(cfm);
                break;
            case RENAME:
                for (Map.Entry<ColumnIdentifier, ColumnIdentifier> entry : renames.entrySet())
                {
                    ColumnIdentifier from = entry.getKey();
                    ColumnIdentifier to = entry.getValue();
                    cfm.renameColumn(from.key, from.toString(), to.key, to.toString());
                }
                break;
        }

        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s, column=%s, validator=%s)",
                             cfName,
                             oType,
                             columnName,
                             validator);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
