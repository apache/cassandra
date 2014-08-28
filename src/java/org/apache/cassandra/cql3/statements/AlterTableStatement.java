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
    private final boolean isStatic; // Only for ALTER ADD

    public AlterTableStatement(CFName name,
                               Type type,
                               ColumnIdentifier columnName,
                               CQL3Type validator,
                               CFPropDefs cfProps,
                               Map<ColumnIdentifier, ColumnIdentifier> renames,
                               boolean isStatic)
    {
        super(name);
        this.oType = type;
        this.columnName = columnName;
        this.validator = validator; // used only for ADD/ALTER commands
        this.cfProps = cfProps;
        this.renames = renames;
        this.isStatic = isStatic;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public boolean announceMigration() throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.clone();

        CFDefinition cfDef = meta.getCfDef();
        CFDefinition.Name name = columnName == null ? null : cfDef.get(columnName);
        switch (oType)
        {
            case ADD:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");

                if (isStatic)
                {
                    if (!cfDef.isComposite)
                        throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                    if (cfDef.clusteringColumns().isEmpty())
                        throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                }

                if (name != null)
                {
                    switch (name.kind)
                    {
                        case KEY_ALIAS:
                        case COLUMN_ALIAS:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        default:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }

                // Cannot re-add a dropped counter column. See #7831.
                if (meta.getDefaultValidator().isCommutative() && meta.getDroppedColumns().containsKey(columnName.key))
                    throw new InvalidRequestException(String.format("Cannot re-add previously dropped counter column %s", columnName));

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

                    // If there used to be a collection column with the same name (that has been dropped), it will
                    // still be appear in the ColumnToCollectionType because or reasons explained on #6276. The same
                    // reason mean that we can't allow adding a new collection with that name (see the ticket for details).
                    CollectionType previous = collections.get(columnName.key);
                    if (previous != null && !type.isCompatibleWith(previous))
                        throw new InvalidRequestException(String.format("Cannot add a collection with the name %s " +
                                    "because a collection with the same name and a different type has already been used in the past", columnName));

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
                cfm.addColumnDefinition(isStatic
                                        ? ColumnDefinition.staticDef(columnName.key, type, componentIndex)
                                        : ColumnDefinition.regularDef(columnName.key, type, componentIndex));
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
                            List<AbstractType<?>> oldTypes = ((CompositeType) cfm.getKeyValidator()).types;
                            if (!newType.isValueCompatibleWith(oldTypes.get(name.position)))
                                throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                               columnName,
                                                                               oldTypes.get(name.position).asCQL3Type(),
                                                                               validator));

                            List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(oldTypes);
                            newTypes.set(name.position, newType);
                            cfm.keyValidator(CompositeType.getInstance(newTypes));
                        }
                        else
                        {
                            if (!newType.isValueCompatibleWith(cfm.getKeyValidator()))
                                throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                               columnName,
                                                                               cfm.getKeyValidator().asCQL3Type(),
                                                                               validator));
                            cfm.keyValidator(newType);
                        }
                        break;
                    case COLUMN_ALIAS:
                        assert cfDef.isComposite;
                        List<AbstractType<?>> oldTypes = ((CompositeType) cfm.comparator).types;
                        // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
                        // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
                        // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
                        if (!validator.getType().isCompatibleWith(oldTypes.get(name.position)))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are not order-compatible.",
                                                                           columnName,
                                                                           oldTypes.get(name.position).asCQL3Type(),
                                                                           validator));
                        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(oldTypes);
                        newTypes.set(name.position, validator.getType());
                        cfm.comparator = CompositeType.getInstance(newTypes);
                        break;
                    case VALUE_ALIAS:
                        // See below
                        if (!validator.getType().isValueCompatibleWith(cfm.getDefaultValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           cfm.getDefaultValidator().asCQL3Type(),
                                                                           validator));
                        cfm.defaultValidator(validator.getType());
                        break;
                    case COLUMN_METADATA:
                    case STATIC:
                        ColumnDefinition column = cfm.getColumnDefinition(columnName.key);
                        // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
                        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                        // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
                        // though since we won't compare values (except when there is an index, but that is validated by
                        // ColumnDefinition already).
                        if (!validator.getType().isValueCompatibleWith(column.getValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           column.getValidator().asCQL3Type(),
                                                                           validator));

                        column.setValidator(validator.getType());
                        break;
                }
                break;

            case DROP:
                if (cfDef.isCompact || !cfDef.isComposite)
                    throw new InvalidRequestException("Cannot drop columns from a COMPACT STORAGE table");
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case COLUMN_METADATA:
                    case STATIC:
                        ColumnDefinition toDelete = null;
                        for (ColumnDefinition columnDef : cfm.regularAndStaticColumns())
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
        return true;
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
