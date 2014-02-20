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

import java.util.ArrayList;
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
    public final CQL3Type.Raw validator;
    public final ColumnIdentifier columnName;
    private final CFPropDefs cfProps;
    private final Map<ColumnIdentifier, ColumnIdentifier> renames;
    private final boolean isStatic; // Only for ALTER ADD

    public AlterTableStatement(CFName name,
                               Type type,
                               ColumnIdentifier columnName,
                               CQL3Type.Raw validator,
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

    public void announceMigration() throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.clone();

        CQL3Type validator = this.validator == null ? null : this.validator.prepare(keyspace());

        ColumnDefinition def = columnName == null ? null : cfm.getColumnDefinition(columnName);
        switch (oType)
        {
            case ADD:
                if (cfm.comparator.isDense())
                    throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");
                if (isStatic && !cfm.comparator.isCompound())
                    throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                if (def != null)
                {
                    switch (def.kind)
                    {
                        case PARTITION_KEY:
                        case CLUSTERING_COLUMN:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        default:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }

                AbstractType<?> type = validator.getType();
                if (type instanceof CollectionType)
                {
                    if (!cfm.comparator.supportCollections())
                        throw new InvalidRequestException("Cannot use collection types with non-composite PRIMARY KEY");
                    if (cfm.isSuper())
                        throw new InvalidRequestException("Cannot use collection types with Super column family");

                    cfm.comparator = cfm.comparator.addCollection(columnName, (CollectionType)type);
                }

                Integer componentIndex = cfm.comparator.isCompound() ? cfm.comparator.clusteringPrefixSize() : null;
                cfm.addColumnDefinition(isStatic
                                        ? ColumnDefinition.staticDef(cfm, columnName.bytes, type, componentIndex)
                                        : ColumnDefinition.regularDef(cfm, columnName.bytes, type, componentIndex));
                break;

            case ALTER:
                if (def == null)
                    throw new InvalidRequestException(String.format("Cell %s was not found in table %s", columnName, columnFamily()));

                switch (def.kind)
                {
                    case PARTITION_KEY:
                        AbstractType<?> newType = validator.getType();
                        if (newType instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));
                        if (cfm.getKeyValidator() instanceof CompositeType)
                        {
                            List<AbstractType<?>> oldTypes = ((CompositeType) cfm.getKeyValidator()).types;
                            if (!newType.isValueCompatibleWith(oldTypes.get(def.position())))
                                throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                               columnName,
                                                                               oldTypes.get(def.position()).asCQL3Type(),
                                                                               validator));

                            List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(oldTypes);
                            newTypes.set(def.position(), newType);
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
                    case CLUSTERING_COLUMN:
                        AbstractType<?> oldType = cfm.comparator.subtype(def.position());
                        // Note that CFMetaData.validateCompatibility already validate the change we're about to do. However, the error message it
                        // sends is a bit cryptic for a CQL3 user, so validating here for a sake of returning a better error message
                        // Do note that we need isCompatibleWith here, not just isValueCompatibleWith.
                        if (!validator.getType().isCompatibleWith(oldType))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are not order-compatible.",
                                                                           columnName,
                                                                           oldType.asCQL3Type(),
                                                                           validator));

                        cfm.comparator = cfm.comparator.setSubtype(def.position(), validator.getType());
                        break;
                    case COMPACT_VALUE:
                        // See below
                        if (!validator.getType().isValueCompatibleWith(cfm.getDefaultValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           cfm.getDefaultValidator().asCQL3Type(),
                                                                           validator));
                        cfm.defaultValidator(validator.getType());
                        break;
                    case REGULAR:
                    case STATIC:
                        // Thrift allows to change a column validator so CFMetaData.validateCompatibility will let it slide
                        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                        // allow it for CQL3 (see #5882) so validating it explicitly here. We only care about value compatibility
                        // though since we won't compare values (except when there is an index, but that is validated by
                        // ColumnDefinition already).
                        if (!validator.getType().isValueCompatibleWith(def.type))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           def.type.asCQL3Type(),
                                                                           validator));

                        break;
                }
                // In any case, we update the column definition
                cfm.addOrReplaceColumnDefinition(def.withNewType(validator.getType()));
                break;

            case DROP:
                if (!cfm.isCQL3Table())
                    throw new InvalidRequestException("Cannot drop columns from a non-CQL3 table");
                if (def == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (def.kind)
                {
                    case PARTITION_KEY:
                    case CLUSTERING_COLUMN:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case REGULAR:
                    case STATIC:
                        ColumnDefinition toDelete = null;
                        for (ColumnDefinition columnDef : cfm.regularAndStaticColumns())
                        {
                            if (columnDef.name.equals(columnName))
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
                    cfm.renameColumn(entry.getKey(), entry.getValue());
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
