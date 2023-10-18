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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.String.format;
import static java.lang.String.join;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;

import static org.apache.cassandra.schema.TableMetadata.Flag;

public abstract class AlterTableStatement extends AlterSchemaStatement
{
    protected final String tableName;
    private final boolean ifExists;
    protected ClientState state;

    public AlterTableStatement(String keyspaceName, String tableName, boolean ifExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        // save the query state to use it for guardrails validation in #apply
        this.state = state;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.getTableOrViewNullable(tableName);

        if (null == table)
        {
            if (!ifExists)
                throw ire("Table '%s.%s' doesn't exist", keyspaceName, tableName);
            return schema;
        }

        if (table.isView())
            throw ire("Cannot use ALTER TABLE on a materialized view; use ALTER MATERIALIZED VIEW instead");

        return schema.withAddedOrUpdated(apply(keyspace, table));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_TABLE, keyspaceName, tableName);
    }

    public String toString()
    {
        return format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, tableName);
    }

    abstract KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table);

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> ALTER <column> TYPE <newtype>;}
     *
     * No longer supported.
     */
    public static class AlterColumn extends AlterTableStatement
    {
        AlterColumn(String keyspaceName, String tableName, boolean ifTableExists)
        {
            super(keyspaceName, tableName, ifTableExists);
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            throw ire("Altering column types is no longer supported");
        }
    }

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> ALTER [IF EXISTS] <column> ( MASKED WITH <newMask> | DROP MASKED )}
     */
    public static class MaskColumn extends AlterTableStatement
    {
        private final ColumnIdentifier columnName;
        @Nullable
        private final ColumnMask.Raw rawMask;
        private final boolean ifColumnExists;

        MaskColumn(String keyspaceName,
                   String tableName,
                   ColumnIdentifier columnName,
                   @Nullable ColumnMask.Raw rawMask,
                   boolean ifTableExists,
                   boolean ifColumnExists)
        {
            super(keyspaceName, tableName, ifTableExists);
            this.columnName = columnName;
            this.rawMask = rawMask;
            this.ifColumnExists = ifColumnExists;
        }

        @Override
        public void validate(ClientState state)
        {
            super.validate(state);

            // we don't allow creating masks if they are disabled, but we still allow dropping them
            if (rawMask != null)
                ColumnMask.ensureEnabled();
        }

        @Override
        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            ColumnMetadata column = table.getColumn(columnName);

            if (column == null)
            {
                if (!ifColumnExists)
                    throw ire("Column with name '%s' doesn't exist on table '%s'", columnName, tableName);

                return keyspace;
            }

            ColumnMask oldMask = table.getColumn(columnName).getMask();
            ColumnMask newMask = rawMask == null ? null : rawMask.prepare(keyspace.name, table.name, columnName, column.type);

            if (Objects.equals(oldMask, newMask))
                return keyspace;

            TableMetadata.Builder tableBuilder = table.unbuild();
            tableBuilder.alterColumnMask(columnName, newMask);
            TableMetadata newTable = tableBuilder.build();
            newTable.validate();

            // Update any reference on materialized views, so the mask is consistent among the base table and its views.
            Views.Builder viewsBuilder = keyspace.views.unbuild();
            for (ViewMetadata view : keyspace.views.forTable(table.id))
            {
                if (view.includes(columnName))
                {
                    viewsBuilder.put(viewsBuilder.get(view.name()).withNewColumnMask(columnName, newMask));
                }
            }

            return keyspace.withSwapped(keyspace.tables.withSwapped(newTable))
                           .withSwapped(viewsBuilder.build());
        }
    }

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] <column> <newtype>}
     * {@code ALTER TABLE [IF EXISTS] <table> ADD [IF NOT EXISTS] (<column> <newtype>, <column1> <newtype1>, ... <columnn> <newtypen>)}
     */
    private static class AddColumns extends AlterTableStatement
    {
        private static class Column
        {
            private final ColumnIdentifier name;
            private final CQL3Type.Raw type;
            private final boolean isStatic;
            @Nullable
            private final ColumnMask.Raw mask;

            Column(ColumnIdentifier name, CQL3Type.Raw type, boolean isStatic, @Nullable ColumnMask.Raw mask)
            {
                this.name = name;
                this.type = type;
                this.isStatic = isStatic;
                this.mask = mask;
            }
        }

        private final Collection<Column> newColumns;
        private final boolean ifColumnNotExists;

        private AddColumns(String keyspaceName, String tableName, Collection<Column> newColumns, boolean ifTableExists, boolean ifColumnNotExists)
        {
            super(keyspaceName, tableName, ifTableExists);
            this.newColumns = newColumns;
            this.ifColumnNotExists = ifColumnNotExists;
        }

        @Override
        public void validate(ClientState state)
        {
            super.validate(state);
            newColumns.forEach(c -> c.type.validate(state, "Column " + c.name));
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            Guardrails.alterTableEnabled.ensureEnabled("ALTER TABLE changing columns", state);
            TableMetadata.Builder tableBuilder = table.unbuild();
            Views.Builder viewsBuilder = keyspace.views.unbuild();
            newColumns.forEach(c -> addColumn(keyspace, table, c, ifColumnNotExists, tableBuilder, viewsBuilder));

            Guardrails.columnsPerTable.guard(tableBuilder.numColumns(), tableName, false, state);

            TableMetadata tableMetadata = tableBuilder.build();
            tableMetadata.validate();

            return keyspace.withSwapped(keyspace.tables.withSwapped(tableMetadata))
                           .withSwapped(viewsBuilder.build());
        }

        private void addColumn(KeyspaceMetadata keyspace,
                               TableMetadata table,
                               Column column,
                               boolean ifColumnNotExists,
                               TableMetadata.Builder tableBuilder,
                               Views.Builder viewsBuilder)
        {
            ColumnIdentifier name = column.name;
            AbstractType<?> type = column.type.prepare(keyspaceName, keyspace.types).getType();
            boolean isStatic = column.isStatic;
            ColumnMask mask = column.mask == null ? null : column.mask.prepare(keyspaceName, tableName, name, type);

            if (null != tableBuilder.getColumn(name)) {
                if (!ifColumnNotExists)
                    throw ire("Column with name '%s' already exists", name);
                return;
            }

            if (table.isCompactTable())
                throw ire("Cannot add new column to a COMPACT STORAGE table");

            if (isStatic && table.clusteringColumns().isEmpty())
                throw ire("Static columns are only useful (and thus allowed) if the table has at least one clustering column");

            ColumnMetadata droppedColumn = table.getDroppedColumn(name.bytes);
            if (null != droppedColumn)
            {
                // After #8099, not safe to re-add columns of incompatible types - until *maybe* deser logic with dropped
                // columns is pushed deeper down the line. The latter would still be problematic in cases of schema races.
                if (!type.isSerializationCompatibleWith(droppedColumn.type))
                {
                    throw ire("Cannot re-add previously dropped column '%s' of type %s, incompatible with previous type %s",
                              name,
                              type.asCQL3Type(),
                              droppedColumn.type.asCQL3Type());
                }

                if (droppedColumn.isStatic() != isStatic)
                {
                    throw ire("Cannot re-add previously dropped column '%s' of kind %s, incompatible with previous kind %s",
                              name,
                              isStatic ? ColumnMetadata.Kind.STATIC : ColumnMetadata.Kind.REGULAR,
                              droppedColumn.kind);
                }

                // Cannot re-add a dropped counter column. See #7831.
                if (table.isCounter())
                    throw ire("Cannot re-add previously dropped counter column %s", name);
            }

            if (isStatic)
                tableBuilder.addStaticColumn(name, type, mask);
            else
                tableBuilder.addRegularColumn(name, type, mask);

            if (!isStatic)
            {
                for (ViewMetadata view : keyspace.views.forTable(table.id))
                {
                    if (view.includeAllColumns)
                    {
                        ColumnMetadata viewColumn = ColumnMetadata.regularColumn(view.metadata, name.bytes, type)
                                                                  .withNewMask(mask);
                        viewsBuilder.put(viewsBuilder.get(view.name()).withAddedRegularColumn(viewColumn));
                    }
                }
            }
        }
    }

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] <column>}
     * {@code ALTER TABLE [IF EXISTS] <table> DROP [IF EXISTS] ( <column>, <column1>, ... <columnn>)}
     */
    // TODO: swap UDT refs with expanded tuples on drop
    private static class DropColumns extends AlterTableStatement
    {
        private final Set<ColumnIdentifier> removedColumns;
        private final boolean ifColumnExists;
        private final Long timestamp;

        private DropColumns(String keyspaceName, String tableName, Set<ColumnIdentifier> removedColumns, boolean ifTableExists, boolean ifColumnExists, Long timestamp)
        {
            super(keyspaceName, tableName, ifTableExists);
            this.removedColumns = removedColumns;
            this.ifColumnExists = ifColumnExists;
            this.timestamp = timestamp;
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            Guardrails.alterTableEnabled.ensureEnabled("ALTER TABLE changing columns", state);
            TableMetadata.Builder builder = table.unbuild();
            removedColumns.forEach(c -> dropColumn(keyspace, table, c, ifColumnExists, builder));
            return keyspace.withSwapped(keyspace.tables.withSwapped(builder.build()));
        }

        private void dropColumn(KeyspaceMetadata keyspace, TableMetadata table, ColumnIdentifier column, boolean ifExists, TableMetadata.Builder builder)
        {
            ColumnMetadata currentColumn = table.getColumn(column);
            if (null == currentColumn) {
                if (!ifExists)
                    throw ire("Column %s was not found in table '%s'", column, table);
                return;
            }

            if (currentColumn.isPrimaryKeyColumn())
                throw ire("Cannot drop PRIMARY KEY column %s", column);

            /*
             * Cannot allow dropping top-level columns of user defined types that aren't frozen because we cannot convert
             * the type into an equivalent tuple: we only support frozen tuples currently. And as such we cannot persist
             * the correct type in system_schema.dropped_columns.
             */
            if (currentColumn.type.isUDT() && currentColumn.type.isMultiCell())
                throw ire("Cannot drop non-frozen column %s of user type %s", column, currentColumn.type.asCQL3Type());

            // TODO: some day try and find a way to not rely on Keyspace/IndexManager/Index to find dependent indexes
            Set<IndexMetadata> dependentIndexes = Keyspace.openAndGetStore(table).indexManager.getDependentIndexes(currentColumn);
            if (!dependentIndexes.isEmpty())
            {
                throw ire("Cannot drop column %s because it has dependent secondary indexes (%s)",
                          currentColumn,
                          join(", ", transform(dependentIndexes, i -> i.name)));
            }

            if (!isEmpty(keyspace.views.forTable(table.id)))
                throw ire("Cannot drop column %s on base table %s with materialized views", currentColumn, table.name);

            builder.removeRegularOrStaticColumn(column);
            builder.recordColumnDrop(currentColumn, getTimestamp());
        }

        /**
         * @return timestamp from query, otherwise return current time in micros
         */
        private long getTimestamp()
        {
            return timestamp == null ? ClientState.getTimestamp() : timestamp;
        }
    }

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> RENAME [IF EXISTS] <column> TO <column>;}
     */
    private static class RenameColumns extends AlterTableStatement
    {
        private final Map<ColumnIdentifier, ColumnIdentifier> renamedColumns;
        private final boolean ifColumnsExists;

        private RenameColumns(String keyspaceName, String tableName, Map<ColumnIdentifier, ColumnIdentifier> renamedColumns, boolean ifTableExists, boolean ifColumnsExists)
        {
            super(keyspaceName, tableName, ifTableExists);
            this.renamedColumns = renamedColumns;
            this.ifColumnsExists = ifColumnsExists;
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            Guardrails.alterTableEnabled.ensureEnabled("ALTER TABLE changing columns", state);
            TableMetadata.Builder tableBuilder = table.unbuild();
            Views.Builder viewsBuilder = keyspace.views.unbuild();
            renamedColumns.forEach((o, n) -> renameColumn(keyspace, table, o, n, ifColumnsExists, tableBuilder, viewsBuilder));

            return keyspace.withSwapped(keyspace.tables.withSwapped(tableBuilder.build()))
                           .withSwapped(viewsBuilder.build());
        }

        private void renameColumn(KeyspaceMetadata keyspace,
                                  TableMetadata table,
                                  ColumnIdentifier oldName,
                                  ColumnIdentifier newName,
                                  boolean ifColumnsExists,
                                  TableMetadata.Builder tableBuilder,
                                  Views.Builder viewsBuilder)
        {
            ColumnMetadata column = table.getExistingColumn(oldName);
            if (null == column)
            {
                if (!ifColumnsExists)
                    throw ire("Column %s was not found in table %s", oldName, table);
                return;
            }

            if (!column.isPrimaryKeyColumn())
                throw ire("Cannot rename non PRIMARY KEY column %s", oldName);

            if (null != table.getColumn(newName))
            {
                throw ire("Cannot rename column %s to %s in table '%s'; another column with that name already exists",
                          oldName,
                          newName,
                          table);
            }

            // TODO: some day try and find a way to not rely on Keyspace/IndexManager/Index to find dependent indexes
            Set<IndexMetadata> dependentIndexes = Keyspace.openAndGetStore(table).indexManager.getDependentIndexes(column);
            if (!dependentIndexes.isEmpty())
            {
                throw ire("Can't rename column %s because it has dependent secondary indexes (%s)",
                          oldName,
                          join(", ", transform(dependentIndexes, i -> i.name)));
            }

            for (ViewMetadata view : keyspace.views.forTable(table.id))
            {
                if (view.includes(oldName))
                {
                    viewsBuilder.put(viewsBuilder.get(view.name()).withRenamedPrimaryKeyColumn(oldName, newName));
                }
            }

            tableBuilder.renamePrimaryKeyColumn(oldName, newName);
        }
    }

    /**
     * {@code ALTER TABLE [IF EXISTS] <table> WITH <property> = <value>}
     */
    private static class AlterOptions extends AlterTableStatement
    {
        private final TableAttributes attrs;

        private AlterOptions(String keyspaceName, String tableName, TableAttributes attrs, boolean ifTableExists)
        {
            super(keyspaceName, tableName, ifTableExists);
            this.attrs = attrs;
        }

        @Override
        public void validate(ClientState state)
        {
            super.validate(state);

            Guardrails.tableProperties.guard(attrs.updatedProperties(), attrs::removeProperty, state);

            validateDefaultTimeToLive(attrs.asNewTableParams());
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            attrs.validate();

            TableParams params = attrs.asAlteredTableParams(table.params);

            if (table.isCounter() && params.defaultTimeToLive > 0)
                throw ire("Cannot set default_time_to_live on a table with counters");

            if (!isEmpty(keyspace.views.forTable(table.id)) && params.gcGraceSeconds == 0)
            {
                throw ire("Cannot alter gc_grace_seconds of the base table of a " +
                          "materialized view to 0, since this value is used to TTL " +
                          "undelivered updates. Setting gc_grace_seconds too low might " +
                          "cause undelivered updates to expire " +
                          "before being replayed.");
            }

            if (keyspace.createReplicationStrategy().hasTransientReplicas()
                && params.readRepair != ReadRepairStrategy.NONE)
            {
                throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
            }

            if (!params.compression.isEnabled())
                Guardrails.uncompressedTablesEnabled.ensureEnabled(state);

            return keyspace.withSwapped(keyspace.tables.withSwapped(table.withSwapped(params)));
        }
    }


    /**
     * {@code ALTER TABLE [IF EXISTS] <table> DROP COMPACT STORAGE}
     */
    private static class DropCompactStorage extends AlterTableStatement
    {
        private static final Logger logger = LoggerFactory.getLogger(AlterTableStatement.class);
        private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
        private DropCompactStorage(String keyspaceName, String tableName, boolean ifTableExists)
        {
            super(keyspaceName, tableName, ifTableExists);
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            if (!DatabaseDescriptor.enableDropCompactStorage())
                throw new InvalidRequestException("DROP COMPACT STORAGE is disabled. Enable in cassandra.yaml to use.");

            if (!table.isCompactTable())
                throw AlterTableStatement.ire("Cannot DROP COMPACT STORAGE on table without COMPACT STORAGE");

            validateCanDropCompactStorage();

            Set<Flag> flags = table.isCounter()
                            ? ImmutableSet.of(Flag.COMPOUND, Flag.COUNTER)
                            : ImmutableSet.of(Flag.COMPOUND);

            return keyspace.withSwapped(keyspace.tables.withSwapped(table.withSwapped(flags)));
        }

        /**
         * Throws if DROP COMPACT STORAGE cannot be used (yet) because the cluster is not sufficiently upgraded. To be able
         * to use DROP COMPACT STORAGE, we need to ensure that no pre-3.0 sstables exists in the cluster, as we won't be
         * able to read them anymore once COMPACT STORAGE is dropped (see CASSANDRA-15897). In practice, this method checks
         * 3 things:
         *   1) that all nodes are on 3.0+. We need this because 2.x nodes don't advertise their sstable versions.
         *   2) for 3.0+, we use the new (CASSANDRA-15897) sstables versions set gossiped by all nodes to ensure all
         *      sstables have been upgraded cluster-wise.
         *   3) if the cluster still has some 3.0 nodes that predate CASSANDRA-15897, we will not have the sstable versions
         *      for them. In that case, we also refuse DROP COMPACT (even though it may well be safe at this point) and ask
         *      the user to upgrade all nodes.
         */
        private void validateCanDropCompactStorage()
        {
            Set<InetAddressAndPort> before4 = new HashSet<>();
            Set<InetAddressAndPort> preC15897nodes = new HashSet<>();
            Set<InetAddressAndPort> with2xSStables = new HashSet<>();
            Splitter onComma = Splitter.on(',').omitEmptyStrings().trimResults();
            for (InetAddressAndPort node : StorageService.instance.getTokenMetadata().getAllEndpoints())
            {
                if (MessagingService.instance().versions.knows(node) &&
                    MessagingService.instance().versions.getRaw(node) < MessagingService.VERSION_40)
                {
                    before4.add(node);
                    continue;
                }

                String sstableVersionsString = Gossiper.instance.getApplicationState(node, ApplicationState.SSTABLE_VERSIONS);
                if (sstableVersionsString == null)
                {
                    preC15897nodes.add(node);
                    continue;
                }

                try
                {
                    boolean has2xSStables = onComma.splitToList(sstableVersionsString)
                                                   .stream()
                                                   .anyMatch(v -> v.compareTo("big-ma")<=0);
                    if (has2xSStables)
                        with2xSStables.add(node);
                }
                catch (IllegalArgumentException e)
                {
                    // Means VersionType::fromString didn't parse a version correctly. Which shouldn't happen, we shouldn't
                    // have garbage in Gossip. But crashing the request is not ideal, so we log the error but ignore the
                    // node otherwise.
                    noSpamLogger.error("Unexpected error parsing sstable versions from gossip for {} (gossiped value " +
                                       "is '{}'). This is a bug and should be reported. Cannot ensure that {} has no " +
                                       "non-upgraded 2.x sstables anymore. If after this DROP COMPACT STORAGE some old " +
                                       "sstables cannot be read anymore, please use `upgradesstables` with the " +
                                       "`--force-compact-storage-on` option.", node, sstableVersionsString, node);
                }
            }

            if (!before4.isEmpty())
                throw new InvalidRequestException(format("Cannot DROP COMPACT STORAGE as some nodes in the cluster (%s) " +
                                                         "are not on 4.0+ yet. Please upgrade those nodes and run " +
                                                         "`upgradesstables` before retrying.", before4));
            if (!preC15897nodes.isEmpty())
                throw new InvalidRequestException(format("Cannot guarantee that DROP COMPACT STORAGE is safe as some nodes " +
                                                         "in the cluster (%s) do not have https://issues.apache.org/jira/browse/CASSANDRA-15897. " +
                                                         "Please upgrade those nodes and retry.", preC15897nodes));
            if (!with2xSStables.isEmpty())
                throw new InvalidRequestException(format("Cannot DROP COMPACT STORAGE as some nodes in the cluster (%s) " +
                                                         "has some non-upgraded 2.x sstables. Please run `upgradesstables` " +
                                                         "on those nodes before retrying", with2xSStables));
        }
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private enum Kind
        {
            ALTER_COLUMN,
            MASK_COLUMN,
            ADD_COLUMNS,
            DROP_COLUMNS,
            RENAME_COLUMNS,
            ALTER_OPTIONS,
            DROP_COMPACT_STORAGE
        }

        private final QualifiedName name;
        private final boolean ifTableExists;
        private boolean ifColumnExists;
        private boolean ifColumnNotExists;

        private Kind kind;

        // ADD
        private final List<AddColumns.Column> addedColumns = new ArrayList<>();

        // ALTER MASK
        private ColumnIdentifier maskedColumn = null;
        private ColumnMask.Raw rawMask = null;

        // DROP
        private final Set<ColumnIdentifier> droppedColumns = new HashSet<>();
        private Long timestamp = null; // will use execution timestamp if not provided by query

        // RENAME
        private final Map<ColumnIdentifier, ColumnIdentifier> renamedColumns = new HashMap<>();

        // OPTIONS
        public final TableAttributes attrs = new TableAttributes();

        public Raw(QualifiedName name, boolean ifTableExists)
        {
            this.name = name;
            this.ifTableExists = ifTableExists;
        }

        public AlterTableStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();
            String tableName = name.getName();

            switch (kind)
            {
                case          ALTER_COLUMN: return new AlterColumn(keyspaceName, tableName, ifTableExists);
                case           MASK_COLUMN: return new MaskColumn(keyspaceName, tableName, maskedColumn, rawMask, ifTableExists, ifColumnExists);
                case           ADD_COLUMNS: return new AddColumns(keyspaceName, tableName, addedColumns, ifTableExists, ifColumnNotExists);
                case          DROP_COLUMNS: return new DropColumns(keyspaceName, tableName, droppedColumns, ifTableExists, ifColumnExists, timestamp);
                case        RENAME_COLUMNS: return new RenameColumns(keyspaceName, tableName, renamedColumns, ifTableExists, ifColumnExists);
                case         ALTER_OPTIONS: return new AlterOptions(keyspaceName, tableName, attrs, ifTableExists);
                case  DROP_COMPACT_STORAGE: return new DropCompactStorage(keyspaceName, tableName, ifTableExists);
            }

            throw new AssertionError();
        }

        public void alter(ColumnIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ALTER_COLUMN;
        }

        public void mask(ColumnIdentifier name, ColumnMask.Raw mask)
        {
            kind = Kind.MASK_COLUMN;
            maskedColumn = name;
            rawMask = mask;
        }

        public void add(ColumnIdentifier name, CQL3Type.Raw type, boolean isStatic, @Nullable ColumnMask.Raw mask)
        {
            kind = Kind.ADD_COLUMNS;
            addedColumns.add(new AddColumns.Column(name, type, isStatic, mask));
        }

        public void drop(ColumnIdentifier name)
        {
            kind = Kind.DROP_COLUMNS;
            droppedColumns.add(name);
        }

        public void ifColumnNotExists(boolean ifNotExists)
        {
            ifColumnNotExists = ifNotExists;
        }

        public void ifColumnExists(boolean ifExists)
        {
            ifColumnExists = ifExists;
        }

        public void dropCompactStorage()
        {
            kind = Kind.DROP_COMPACT_STORAGE;
        }

        public void timestamp(long timestamp)
        {
            this.timestamp = timestamp;
        }

        public void rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            kind = Kind.RENAME_COLUMNS;
            renamedColumns.put(from, to);
        }

        public void attrs()
        {
            this.kind = Kind.ALTER_OPTIONS;
        }
    }
}
