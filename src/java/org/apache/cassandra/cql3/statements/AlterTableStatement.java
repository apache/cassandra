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
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public enum Type
    {
        ADD, ALTER, DROP, OPTS, RENAME
    }

    public final Type oType;
    private final TableAttributes attrs;
    private final Map<ColumnMetadata.Raw, ColumnMetadata.Raw> renames;
    private final List<AlterTableStatementColumn> colNameList;
    private final Long deleteTimestamp;

    public AlterTableStatement(CFName name,
                               Type type,
                               List<AlterTableStatementColumn> colDataList,
                               TableAttributes attrs,
                               Map<ColumnMetadata.Raw, ColumnMetadata.Raw> renames,
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
        TableMetadata current = Schema.instance.validateTable(keyspace(), columnFamily());
        if (current.isView())
            throw new InvalidRequestException("Cannot use ALTER TABLE on Materialized View");

        TableMetadata.Builder builder = current.unbuild();

        ColumnIdentifier columnName = null;
        ColumnMetadata def = null;
        CQL3Type.Raw dataType = null;
        boolean isStatic = false;
        CQL3Type validator = null;

        List<ViewMetadata> viewUpdates = new ArrayList<>();
        Iterable<ViewMetadata> views = View.findAll(keyspace(), columnFamily());

        switch (oType)
        {
            case ALTER:
                throw new InvalidRequestException("Altering of types is not allowed");
            case ADD:
                if (current.isDense())
                    throw new InvalidRequestException("Cannot add new column to a COMPACT STORAGE table");

                for (AlterTableStatementColumn colData : colNameList)
                {
                    columnName = colData.getColumnName().getIdentifier(current);
                    def = builder.getColumn(columnName);
                    dataType = colData.getColumnType();
                    assert dataType != null;
                    isStatic = colData.getStaticType();
                    validator = dataType.prepare(keyspace());


                    if (isStatic)
                    {
                        if (!current.isCompound())
                            throw new InvalidRequestException("Static columns are not allowed in COMPACT STORAGE tables");
                        if (current.clusteringColumns().isEmpty())
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

                    // Cannot re-add a dropped counter column. See #7831.
                    if (current.isCounter() && current.getDroppedColumn(columnName.bytes) != null)
                        throw new InvalidRequestException(String.format("Cannot re-add previously dropped counter column %s", columnName));

                    AbstractType<?> type = validator.getType();
                    if (type.isCollection() && type.isMultiCell())
                    {
                        if (!current.isCompound())
                            throw new InvalidRequestException("Cannot use non-frozen collections in COMPACT STORAGE tables");
                        if (current.isSuper())
                            throw new InvalidRequestException("Cannot use non-frozen collections with super column families");

                        // If there used to be a non-frozen collection column with the same name (that has been dropped),
                        // we could still have some data using the old type, and so we can't allow adding a collection
                        // with the same name unless the types are compatible (see #6276).
                        DroppedColumn dropped = current.droppedColumns.get(columnName.bytes);
                        if (dropped != null && dropped.column.type instanceof CollectionType
                            && dropped.column.type.isMultiCell() && !type.isCompatibleWith(dropped.column.type))
                        {
                            String message =
                                String.format("Cannot add a collection with the name %s because a collection with the same name"
                                              + " and a different type (%s) has already been used in the past",
                                              columnName,
                                              dropped.column.type.asCQL3Type());
                            throw new InvalidRequestException(message);
                        }
                    }

                    builder.addColumn(isStatic
                                    ? ColumnMetadata.staticColumn(current, columnName.bytes, type)
                                    : ColumnMetadata.regularColumn(current, columnName.bytes, type));

                    // Adding a column to a table which has an include all view requires the column to be added to the view
                    // as well
                    if (!isStatic)
                        for (ViewMetadata view : views)
                            if (view.includeAllColumns)
                                viewUpdates.add(view.withAddedRegularColumn(ColumnMetadata.regularColumn(view.metadata, columnName.bytes, type)));

                }
                break;
            case DROP:
                if (!current.isCQLTable())
                    throw new InvalidRequestException("Cannot drop columns from a non-CQL3 table");

                for (AlterTableStatementColumn colData : colNameList)
                {
                    columnName = colData.getColumnName().getIdentifier(current);
                    def = builder.getColumn(columnName);

                    if (def == null)
                        throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                    switch (def.kind)
                    {
                         case PARTITION_KEY:
                         case CLUSTERING:
                              throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                         case REGULAR:
                         case STATIC:
                             builder.removeRegularOrStaticColumn(def.name);
                             builder.recordColumnDrop(def, deleteTimestamp  == null ? queryState.getTimestamp() : deleteTimestamp);
                             break;
                    }

                    // If the dropped column is required by any secondary indexes
                    // we reject the operation, as the indexes must be dropped first
                    Indexes allIndexes = current.indexes;
                    if (!allIndexes.isEmpty())
                    {
                        ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                        Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                        if (!dependentIndexes.isEmpty())
                        {
                            throw new InvalidRequestException(String.format("Cannot drop column %s because it has " +
                                                                            "dependent secondary indexes (%s)",
                                                                            def,
                                                                            dependentIndexes.stream()
                                                                                            .map(i -> i.name)
                                                                                            .collect(Collectors.joining(","))));
                        }
                    }


                    if (!Iterables.isEmpty(views))
                        throw new InvalidRequestException(String.format("Cannot drop column %s on base table with materialized views.",
                                                                        columnName.toString(),
                                                                        keyspace()));
                }
                break;
            case OPTS:
                if (attrs == null)
                    throw new InvalidRequestException("ALTER TABLE WITH invoked, but no parameters found");
                attrs.validate();

                TableParams params = attrs.asAlteredTableParams(current.params);

                if (!Iterables.isEmpty(views) && params.gcGraceSeconds == 0)
                {
                    throw new InvalidRequestException("Cannot alter gc_grace_seconds of the base table of a " +
                                                      "materialized view to 0, since this value is used to TTL " +
                                                      "undelivered updates. Setting gc_grace_seconds too low might " +
                                                      "cause undelivered updates to expire " +
                                                      "before being replayed.");
                }

                if (current.isCounter() && params.defaultTimeToLive > 0)
                    throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");

                builder.params(params);

                break;
            case RENAME:
                for (Map.Entry<ColumnMetadata.Raw, ColumnMetadata.Raw> entry : renames.entrySet())
                {
                    ColumnIdentifier from = entry.getKey().getIdentifier(current);
                    ColumnIdentifier to = entry.getValue().getIdentifier(current);

                    def = current.getColumn(from);
                    if (def == null)
                        throw new InvalidRequestException(String.format("Cannot rename unknown column %s in table %s", from, current.name));

                    if (current.getColumn(to) != null)
                        throw new InvalidRequestException(String.format("Cannot rename column %s to %s in table %s; another column of that name already exist", from, to, current.name));

                    if (!def.isPrimaryKeyColumn())
                        throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));

                    if (!current.indexes.isEmpty())
                    {
                        ColumnFamilyStore store = Keyspace.openAndGetStore(current);
                        Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
                        if (!dependentIndexes.isEmpty())
                            throw new InvalidRequestException(String.format("Cannot rename column %s because it has " +
                                                                            "dependent secondary indexes (%s)",
                                                                            from,
                                                                            dependentIndexes.stream()
                                                                                            .map(i -> i.name)
                                                                                            .collect(Collectors.joining(","))));
                    }

                    builder.renamePrimaryKeyColumn(from, to);

                    // If the view includes a renamed column, it must be renamed in the view table and the definition.
                    for (ViewMetadata view : views)
                    {
                        if (!view.includes(from))
                            continue;

                        ColumnIdentifier viewFrom = entry.getKey().getIdentifier(view.metadata);
                        ColumnIdentifier viewTo = entry.getValue().getIdentifier(view.metadata);
                        viewUpdates.add(view.renamePrimaryKeyColumn(viewFrom, viewTo));
                    }
                }
                break;
        }

        MigrationManager.announceTableUpdate(builder.build(), viewUpdates, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    @Override
    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s)",
                             cfName,
                             oType);
    }
}
