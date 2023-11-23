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

import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.lang.String.join;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.config.CassandraRelevantProperties.MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE;

public final class CreateViewStatement extends AlterSchemaStatement
{
    private final String tableName;
    private final String viewName;

    private final List<RawSelector> rawColumns;
    private final List<ColumnIdentifier> partitionKeyColumns;
    private final List<ColumnIdentifier> clusteringColumns;

    private final WhereClause whereClause;

    private final LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder;
    private final TableAttributes attrs;

    private final boolean ifNotExists;

    private ClientState state;

    public CreateViewStatement(String keyspaceName,
                               String tableName,
                               String viewName,

                               List<RawSelector> rawColumns,
                               List<ColumnIdentifier> partitionKeyColumns,
                               List<ColumnIdentifier> clusteringColumns,

                               WhereClause whereClause,

                               LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder,
                               TableAttributes attrs,

                               boolean ifNotExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.viewName = viewName;

        this.rawColumns = rawColumns;
        this.partitionKeyColumns = partitionKeyColumns;
        this.clusteringColumns = clusteringColumns;

        this.whereClause = whereClause;

        this.clusteringOrder = clusteringOrder;
        this.attrs = attrs;

        this.ifNotExists = ifNotExists;
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
        if (!DatabaseDescriptor.getMaterializedViewsEnabled())
            throw ire("Materialized views are disabled. Enable in cassandra.yaml to use.");

        /*
         * Basic dependency validations
         */

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        if (keyspace.createReplicationStrategy().hasTransientReplicas())
            throw new InvalidRequestException("Materialized views are not supported on transiently replicated keyspaces");

        TableMetadata table = keyspace.tables.getNullable(tableName);
        if (null == table)
            throw ire("Base table '%s' doesn't exist", tableName);

        if (keyspace.hasTable(viewName))
            throw ire("Cannot create materialized view '%s' - a table with the same name already exists", viewName);

        if (keyspace.hasView(viewName))
        {
            if (ifNotExists)
                return schema;

            throw new AlreadyExistsException(keyspaceName, viewName);
        }

        /*
         * Base table validation
         */

        if (table.isCounter())
            throw ire("Materialized views are not supported on counter tables");

        if (table.isView())
            throw ire("Materialized views cannot be created against other materialized views");

        // Guardrails on table properties
        Guardrails.tableProperties.guard(attrs.updatedProperties(), attrs::removeProperty, state);

        // Guardrail to limit number of mvs per table
        Iterable<ViewMetadata> tableViews = keyspace.views.forTable(table.id);
        Guardrails.materializedViewsPerTable.guard(Iterables.size(tableViews) + 1,
                                                   String.format("%s on table %s", viewName, table.name),
                                                   false,
                                                   state);

        if (table.params.gcGraceSeconds == 0)
        {
            throw ire("Cannot create materialized view '%s' for base table " +
                      "'%s' with gc_grace_seconds of 0, since this value is " +
                      "used to TTL undelivered updates. Setting gc_grace_seconds" +
                      " too low might cause undelivered updates to expire " +
                      "before being replayed.",
                      viewName, tableName);
        }

        /*
         * Process SELECT clause
         */

        Set<ColumnIdentifier> selectedColumns = new HashSet<>();

        if (rawColumns.isEmpty()) // SELECT *
            table.columns().forEach(c -> selectedColumns.add(c.name));

        rawColumns.forEach(selector ->
        {
            if (null != selector.alias)
                throw ire("Cannot use aliases when defining a materialized view (got %s)", selector);

            if (!(selector.selectable instanceof Selectable.RawIdentifier))
                throw ire("Can only select columns by name when defining a materialized view (got %s)", selector.selectable);

            // will throw IRE if the column doesn't exist in the base table
            Selectable.RawIdentifier rawIdentifier = (Selectable.RawIdentifier) selector.selectable;
            ColumnMetadata column = rawIdentifier.columnMetadata(table);

            selectedColumns.add(column.name);
        });

        selectedColumns.stream()
                       .map(table::getColumn)
                       .filter(ColumnMetadata::isStatic)
                       .findAny()
                       .ifPresent(c -> { throw ire("Cannot include static column '%s' in materialized view '%s'", c, viewName); });

        /*
         * Process PRIMARY KEY columns and CLUSTERING ORDER BY clause
         */

        if (partitionKeyColumns.isEmpty())
            throw ire("Must provide at least one partition key column for materialized view '%s'", viewName);

        HashSet<ColumnIdentifier> primaryKeyColumns = new HashSet<>();

        concat(partitionKeyColumns, clusteringColumns).forEach(name ->
        {
            ColumnMetadata column = table.getColumn(name);
            if (null == column || !selectedColumns.contains(name))
                throw ire("Unknown column '%s' referenced in PRIMARY KEY for materialized view '%s'", name, viewName);

            if (!primaryKeyColumns.add(name))
                throw ire("Duplicate column '%s' in PRIMARY KEY clause for materialized view '%s'", name, viewName);

            AbstractType<?> type = column.type;

            if (type.isMultiCell())
            {
                if (type.isCollection())
                    throw ire("Invalid non-frozen collection type '%s' for PRIMARY KEY column '%s'", type, name);
                else
                    throw ire("Invalid non-frozen user-defined type '%s' for PRIMARY KEY column '%s'", type, name);
            }

            if (type.isCounter())
                throw ire("counter type is not supported for PRIMARY KEY column '%s'", name);

            if (type.referencesDuration())
                throw ire("duration type is not supported for PRIMARY KEY column '%s'", name);
        });

        // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
        if (!clusteringOrder.isEmpty() && !clusteringColumns.equals(new ArrayList<>(clusteringOrder.keySet())))
            throw ire("Clustering key columns must exactly match columns in CLUSTERING ORDER BY directive");

        /*
         * We need to include all of the primary key columns from the base table in order to make sure that we do not
         * overwrite values in the view. We cannot support "collapsing" the base table into a smaller number of rows in
         * the view because if we need to generate a tombstone, we have no way of knowing which value is currently being
         * used in the view and whether or not to generate a tombstone. In order to not surprise our users, we require
         * that they include all of the columns. We provide them with a list of all of the columns left to include.
         */
        List<ColumnIdentifier> missingPrimaryKeyColumns =
            Lists.newArrayList(filter(transform(table.primaryKeyColumns(), c -> c.name), c -> !primaryKeyColumns.contains(c)));

        if (!missingPrimaryKeyColumns.isEmpty())
        {
            throw ire("Cannot create materialized view '%s' without primary key columns %s from base table '%s'",
                      viewName, join(", ", transform(missingPrimaryKeyColumns, ColumnIdentifier::toString)), tableName);
        }

        Set<ColumnIdentifier> regularBaseTableColumnsInViewPrimaryKey = new HashSet<>(primaryKeyColumns);
        transform(table.primaryKeyColumns(), c -> c.name).forEach(regularBaseTableColumnsInViewPrimaryKey::remove);
        if (regularBaseTableColumnsInViewPrimaryKey.size() > 1)
        {
            throw ire("Cannot include more than one non-primary key column in materialized view primary key (got %s)",
                      join(", ", transform(regularBaseTableColumnsInViewPrimaryKey, ColumnIdentifier::toString)));
        }

        /*
         * Process WHERE clause
         */
        if (whereClause.containsTokenRelations())
            throw new InvalidRequestException("Cannot use token relation when defining a materialized view");

        if (whereClause.containsCustomExpressions())
            throw ire("WHERE clause for materialized view '%s' cannot contain custom index expressions", viewName);

        StatementRestrictions restrictions =
            new StatementRestrictions(state,
                                      StatementType.SELECT,
                                      table,
                                      whereClause,
                                      VariableSpecifications.empty(),
                                      Collections.emptyList(),
                                      false,
                                      false,
                                      true,
                                      true);

        List<ColumnIdentifier> nonRestrictedPrimaryKeyColumns =
            Lists.newArrayList(filter(primaryKeyColumns, name -> !restrictions.isRestricted(table.getColumn(name))));

        if (!nonRestrictedPrimaryKeyColumns.isEmpty())
        {
            throw ire("Primary key columns %s must be restricted with 'IS NOT NULL' or otherwise",
                      join(", ", transform(nonRestrictedPrimaryKeyColumns, ColumnIdentifier::toString)));
        }

        // See CASSANDRA-13798
        Set<ColumnMetadata> restrictedNonPrimaryKeyColumns = restrictions.nonPKRestrictedColumns(false);
        if (!restrictedNonPrimaryKeyColumns.isEmpty() && !MV_ALLOW_FILTERING_NONKEY_COLUMNS_UNSAFE.getBoolean())
        {
            throw ire("Non-primary key columns can only be restricted with 'IS NOT NULL' (got: %s restricted illegally)",
                      join(",", transform(restrictedNonPrimaryKeyColumns, ColumnMetadata::toString)));
        }

        /*
         * Validate WITH params
         */

        attrs.validate();

        if (attrs.hasOption(TableParams.Option.DEFAULT_TIME_TO_LIVE)
            && attrs.getInt(TableParams.Option.DEFAULT_TIME_TO_LIVE.toString(), 0) != 0)
        {
            throw ire("Cannot set default_time_to_live for a materialized view. " +
                      "Data in a materialized view always expire at the same time than " +
                      "the corresponding data in the parent table.");
        }

        /*
         * Build the thing
         */

        TableMetadata.Builder builder = TableMetadata.builder(keyspaceName, viewName);

        if (attrs.hasProperty(TableAttributes.ID))
            builder.id(attrs.getId());

        builder.params(attrs.asNewTableParams())
               .kind(TableMetadata.Kind.VIEW);

        partitionKeyColumns.stream()
                           .map(table::getColumn)
                           .forEach(column -> builder.addPartitionKeyColumn(column.name, getType(column), column.getMask()));

        clusteringColumns.stream()
                         .map(table::getColumn)
                         .forEach(column -> builder.addClusteringColumn(column.name, getType(column), column.getMask()));

        selectedColumns.stream()
                       .filter(name -> !primaryKeyColumns.contains(name))
                       .map(table::getColumn)
                       .forEach(column -> builder.addRegularColumn(column.name, getType(column), column.getMask()));

        ViewMetadata view = new ViewMetadata(table.id, table.name, rawColumns.isEmpty(), whereClause, builder.build());
        view.metadata.validate();

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.views.with(view)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.CREATED, Target.TABLE, keyspaceName, viewName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    private AbstractType<?> getType(ColumnMetadata column)
    {
        AbstractType<?> type = column.type;
        if (clusteringOrder.containsKey(column.name))
        {
            boolean reverse = !clusteringOrder.get(column.name);

            if (type.isReversed() && !reverse)
                return ((ReversedType<?>) type).baseType;

            if (!type.isReversed() && reverse)
                return ReversedType.getInstance(type);
        }
        return type;
    }

    @Override
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        return ImmutableSet.of(View.USAGE_WARNING);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_VIEW, keyspaceName, viewName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, viewName);
    }

    public final static class Raw extends CQLStatement.Raw
    {
        private final QualifiedName tableName;
        private final QualifiedName viewName;
        private final boolean ifNotExists;

        private final List<RawSelector> rawColumns;
        private final List<ColumnIdentifier> clusteringColumns = new ArrayList<>();
        private List<ColumnIdentifier> partitionKeyColumns;

        private final WhereClause whereClause;

        private final LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder = new LinkedHashMap<>();
        public final TableAttributes attrs = new TableAttributes();

        public Raw(QualifiedName tableName, QualifiedName viewName, List<RawSelector> rawColumns, WhereClause whereClause, boolean ifNotExists)
        {
            this.tableName = tableName;
            this.viewName = viewName;
            this.rawColumns = rawColumns;
            this.whereClause = whereClause;
            this.ifNotExists = ifNotExists;
        }

        public CreateViewStatement prepare(ClientState state)
        {
            String keyspaceName = viewName.hasKeyspace() ? viewName.getKeyspace() : state.getKeyspace();

            if (tableName.hasKeyspace() && !keyspaceName.equals(tableName.getKeyspace()))
                throw ire("Cannot create a materialized view on a table in a different keyspace");

            if (!bindVariables.isEmpty())
                throw ire("Bind variables are not allowed in CREATE MATERIALIZED VIEW statements");

            if (null == partitionKeyColumns)
                throw ire("No PRIMARY KEY specifed for view '%s' (exactly one required)", viewName);

            return new CreateViewStatement(keyspaceName,
                                           tableName.getName(),
                                           viewName.getName(),

                                           rawColumns,
                                           partitionKeyColumns,
                                           clusteringColumns,

                                           whereClause,

                                           clusteringOrder,
                                           attrs,

                                           ifNotExists);
        }

        public void setPartitionKeyColumns(List<ColumnIdentifier> columns)
        {
            partitionKeyColumns = columns;
        }

        public void markClusteringColumn(ColumnIdentifier column)
        {
            clusteringColumns.add(column);
        }

        public void extendClusteringOrder(ColumnIdentifier column, boolean ascending)
        {
            if (null != clusteringOrder.put(column, ascending))
                throw ire("Duplicate column '%s' in CLUSTERING ORDER BY clause for view '%s'", column, viewName);
        }
    }
}
