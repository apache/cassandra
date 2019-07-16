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

import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public enum Type
    {
        ADD, ALTER, DROP, DROP_COMPACT_STORAGE, OPTS, RENAME
    }

    public final Type oType;
    private final TableAttributes attrs;
    private final Map<ColumnDefinition.Raw, ColumnDefinition.Raw> renames;
    private final List<AlterTableStatementColumn> colNameList;
    private final Long deleteTimestamp;

    public AlterTableStatement(CFName name,
                               Type type,
                               List<AlterTableStatementColumn> colDataList,
                               TableAttributes attrs,
                               Map<ColumnDefinition.Raw, ColumnDefinition.Raw> renames,
                               Long deleteTimestamp)
    {
        super(name);
        this.oType = type;
        this.colNameList = colDataList;
        this.attrs = attrs;
        this.renames = renames;
        this.deleteTimestamp = deleteTimestamp;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        if (meta.isView())
            throw new InvalidRequestException("Cannot use ALTER TABLE on Materialized View");

        CFMetaData cfm;
        ColumnIdentifier columnName = null;
        ColumnDefinition def = null;
        CQL3Type.Raw dataType = null;
        boolean isStatic = false;
        CQL3Type validator = null;

        List<ViewDefinition> viewUpdates = null;
        Iterable<ViewDefinition> views = View.findAll(keyspace(), columnFamily());

        switch (oType)
        {
            case ALTER:
                throw new InvalidRequestException("Altering of types is not allowed");
            case ADD:
                if (meta.isDense())
                    throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");

                cfm = meta.copy();

                for (AlterTableStatementColumn colData : colNameList)
                {
                    columnName = colData.getColumnName().getIdentifier(cfm);
                    def = cfm.getColumnDefinition(columnName);
                    dataType = colData.getColumnType();
                    assert dataType != null;
                    isStatic = colData.getStaticType();
                    validator = dataType.prepare(keyspace());


                    if (isStatic)
                    {
                        if (!cfm.isCompound())
                            throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                        if (cfm.clusteringColumns().isEmpty())
                            throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                    }

                    if (def != null)
                    {
                        switch (def.kind)
                        {
                            case PARTITION_KEY:
                            case CLUSTERING:
                                throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                            default:
                                throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                        }
                    }

                    AbstractType<?> type = validator.getType();
                    if (type.isCollection() && type.isMultiCell())
                    {
                        if (!cfm.isCompound())
                            throw new InvalidRequestException("Cannot use non-frozen collections in COMPACT STORAGE tables");
                        if (cfm.isSuper())
                            throw new InvalidRequestException("Cannot use non-frozen collections with super column families");
                    }

                    ColumnDefinition toAdd = isStatic
                                           ? ColumnDefinition.staticDef(cfm, columnName.bytes, type)
                                           : ColumnDefinition.regularDef(cfm, columnName.bytes, type);

                    CFMetaData.DroppedColumn droppedColumn = meta.getDroppedColumns().get(columnName.bytes);
                    if (null != droppedColumn)
                    {
                        if (droppedColumn.kind != toAdd.kind)
                        {
                            String message =
                                String.format("Cannot re-add previously dropped column '%s' of kind %s, incompatible with previous kind %s",
                                              columnName,
                                              toAdd.kind,
                                              droppedColumn.kind == null ? "UNKNOWN" : droppedColumn.kind);
                            throw new InvalidRequestException(message);
                        }
                        // After #8099, not safe to re-add columns of incompatible types - until *maybe* deser logic with dropped
                        // columns is pushed deeper down the line. The latter would still be problematic in cases of schema races.
                        if (!type.isValueCompatibleWith(droppedColumn.type))
                        {
                            String message =
                                String.format("Cannot re-add previously dropped column '%s' of type %s, incompatible with previous type %s",
                                              columnName,
                                              type.asCQL3Type(),
                                              droppedColumn.type.asCQL3Type());
                            throw new InvalidRequestException(message);
                        }

                        // Cannot re-add a dropped counter column. See #7831.
                        if (meta.isCounter())
                            throw new InvalidRequestException(String.format("Cannot re-add previously dropped counter column %s", columnName));
                    }

                    cfm.addColumnDefinition(toAdd);

                    // Adding a column to a table which has an include all view requires the column to be added to the view as well
                    if (!isStatic)
                    {
                        for (ViewDefinition view : views)
                        {
                            if (view.includeAllColumns)
                            {
                                ViewDefinition viewCopy = view.copy();
                                viewCopy.metadata.addColumnDefinition(ColumnDefinition.regularDef(viewCopy.metadata, columnName.bytes, type));
                                if (viewUpdates == null)
                                    viewUpdates = new ArrayList<>();
                                viewUpdates.add(viewCopy);
                            }
                        }
                    }
                }
                break;

            case DROP:
                if (!meta.isCQLTable())
                    throw new InvalidRequestException("Cannot drop columns from a non-CQL3 table");

                cfm = meta.copy();

                for (AlterTableStatementColumn colData : colNameList)
                {
                    columnName = colData.getColumnName().getIdentifier(cfm);
                    def = cfm.getColumnDefinition(columnName);

                    if (def == null)
                        throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                    switch (def.kind)
                    {
                         case PARTITION_KEY:
                         case CLUSTERING:
                              throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                         case REGULAR:
                         case STATIC:
                              ColumnDefinition toDelete = null;
                              for (ColumnDefinition columnDef : cfm.partitionColumns())
                              {
                                   if (columnDef.name.equals(columnName))
                                   {
                                       toDelete = columnDef;
                                       break;
                                   }
                               }
                             assert toDelete != null;
                             cfm.removeColumnDefinition(toDelete);
                             cfm.recordColumnDrop(toDelete, deleteTimestamp  == null ? queryState.getTimestamp() : deleteTimestamp);
                             break;
                    }

                    // If the dropped column is required by any secondary indexes
                    // we reject the operation, as the indexes must be dropped first
                    Indexes allIndexes = cfm.getIndexes();
                    if (!allIndexes.isEmpty())
                    {
                        ColumnFamilyStore store = Keyspace.openAndGetStore(cfm);
                        Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                        if (!dependentIndexes.isEmpty())
                            throw new InvalidRequestException(String.format("Cannot drop column %s because it has " +
                                                                            "dependent secondary indexes (%s)",
                                                                            def,
                                                                            dependentIndexes.stream()
                                                                                            .map(i -> i.name)
                                                                                            .collect(Collectors.joining(","))));
                    }

                    if (!Iterables.isEmpty(views))
                    throw new InvalidRequestException(String.format("Cannot drop column %s on base table %s with materialized views.",
                                                                        columnName.toString(),
                                                                        columnFamily()));
                }
                break;
            case DROP_COMPACT_STORAGE:
                if (!meta.isCompactTable())
                    throw new InvalidRequestException("Cannot DROP COMPACT STORAGE on table without COMPACT STORAGE");

                cfm = meta.asNonCompact();
                break;
            case OPTS:
                if (attrs == null)
                    throw new InvalidRequestException("ALTER TABLE WITH invoked, but no parameters found");
                attrs.validate();

                cfm = meta.copy();

                TableParams params = attrs.asAlteredTableParams(cfm.params);

                if (!Iterables.isEmpty(views) && params.gcGraceSeconds == 0)
                {
                    throw new InvalidRequestException("Cannot alter gc_grace_seconds of the base table of a " +
                                                      "materialized view to 0, since this value is used to TTL " +
                                                      "undelivered updates. Setting gc_grace_seconds too low might " +
                                                      "cause undelivered updates to expire " +
                                                      "before being replayed.");
                }

                if (meta.isCounter() && params.defaultTimeToLive > 0)
                    throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");

                cfm.params(params);

                break;
            case RENAME:
                cfm = meta.copy();

                for (Map.Entry<ColumnDefinition.Raw, ColumnDefinition.Raw> entry : renames.entrySet())
                {
                    ColumnIdentifier from = entry.getKey().getIdentifier(cfm);
                    ColumnIdentifier to = entry.getValue().getIdentifier(cfm);
                    cfm.renameColumn(from, to);

                    // If the view includes a renamed column, it must be renamed in the view table and the definition.
                    for (ViewDefinition view : views)
                    {
                        if (!view.includes(from)) continue;

                        ViewDefinition viewCopy = view.copy();
                        ColumnIdentifier viewFrom = entry.getKey().getIdentifier(viewCopy.metadata);
                        ColumnIdentifier viewTo = entry.getValue().getIdentifier(viewCopy.metadata);
                        viewCopy.renameColumn(viewFrom, viewTo);

                        if (viewUpdates == null)
                            viewUpdates = new ArrayList<>();
                        viewUpdates.add(viewCopy);
                    }
                }
                break;
            default:
                throw new InvalidRequestException("Can not alter table: unknown option type " + oType);
        }

        MigrationManager.announceColumnFamilyUpdate(cfm, viewUpdates, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s)",
                             cfName,
                             oType);
    }
}
