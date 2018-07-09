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
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CreateViewStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateViewStatement.class);

    private final CFName baseName;
    private final List<RawSelector> selectClause;
    private final WhereClause whereClause;
    private final List<ColumnMetadata.Raw> partitionKeys;
    private final List<ColumnMetadata.Raw> clusteringKeys;
    public final CFProperties properties = new CFProperties();
    private final boolean ifNotExists;

    public CreateViewStatement(CFName viewName,
                               CFName baseName,
                               List<RawSelector> selectClause,
                               WhereClause whereClause,
                               List<ColumnMetadata.Raw> partitionKeys,
                               List<ColumnMetadata.Raw> clusteringKeys,
                               boolean ifNotExists)
    {
        super(viewName);
        this.baseName = baseName;
        this.selectClause = selectClause;
        this.whereClause = whereClause;
        this.partitionKeys = partitionKeys;
        this.clusteringKeys = clusteringKeys;
        this.ifNotExists = ifNotExists;
    }


    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (!baseName.hasKeyspace())
            baseName.setKeyspace(keyspace(), true);
        state.hasColumnFamilyAccess(keyspace(), baseName.getColumnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // We do validation in announceMigration to reduce doubling up of work
    }

    private interface AddColumn
    {
        void add(ColumnIdentifier identifier, AbstractType<?> type);
    }

    private void add(TableMetadata baseCfm, Iterable<ColumnIdentifier> columns, AddColumn adder)
    {
        for (ColumnIdentifier column : columns)
        {
            AbstractType<?> type = baseCfm.getColumn(column).type;
            if (properties.definedOrdering.containsKey(column))
            {
                boolean desc = properties.definedOrdering.get(column);
                if (!desc && type.isReversed())
                {
                    type = ((ReversedType)type).baseType;
                }
                else if (desc && !type.isReversed())
                {
                    type = ReversedType.getInstance(type);
                }
            }
            adder.add(column, type);
        }
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException
    {
        if (!DatabaseDescriptor.enableMaterializedViews())
        {
            throw new InvalidRequestException("Materialized views are disabled. Enable in cassandra.yaml to use.");
        }

        // We need to make sure that:
        //  - primary key includes all columns in base table's primary key
        //  - make sure that the select statement does not have anything other than columns
        //    and their names match the base table's names
        //  - make sure that primary key does not include any collections
        //  - make sure there is no where clause in the select statement
        //  - make sure there is not currently a table or view
        //  - make sure baseTable gcGraceSeconds > 0

        properties.validate();

        if (properties.useCompactStorage)
            throw new InvalidRequestException("Cannot use 'COMPACT STORAGE' when defining a materialized view");

        // We enforce the keyspace because if the RF is different, the logic to wait for a
        // specific replica would break
        if (!baseName.getKeyspace().equals(keyspace()))
            throw new InvalidRequestException("Cannot create a materialized view on a table in a separate keyspace");

        TableMetadata metadata = Schema.instance.validateTable(baseName.getKeyspace(), baseName.getColumnFamily());

        if (metadata.isVirtual())
            throw new InvalidRequestException("Materialized views are not supported on virtual tables");
        if (metadata.isCounter())
            throw new InvalidRequestException("Materialized views are not supported on counter tables");
        if (metadata.isView())
            throw new InvalidRequestException("Materialized views cannot be created against other materialized views");

        if (metadata.params.gcGraceSeconds == 0)
        {
            throw new InvalidRequestException(String.format("Cannot create materialized view '%s' for base table " +
                                                            "'%s' with gc_grace_seconds of 0, since this value is " +
                                                            "used to TTL undelivered updates. Setting gc_grace_seconds" +
                                                            " too low might cause undelivered updates to expire " +
                                                            "before being replayed.", cfName.getColumnFamily(),
                                                            baseName.getColumnFamily()));
        }

        Set<ColumnIdentifier> included = Sets.newHashSetWithExpectedSize(selectClause.size());
        for (RawSelector selector : selectClause)
        {
            Selectable.Raw selectable = selector.selectable;
            if (selectable instanceof Selectable.WithFieldSelection.Raw)
                throw new InvalidRequestException("Cannot select out a part of type when defining a materialized view");
            if (selectable instanceof Selectable.WithFunction.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            if (selectable instanceof Selectable.WritetimeOrTTL.Raw)
                throw new InvalidRequestException("Cannot use function when defining a materialized view");
            if (selectable instanceof Selectable.WithElementSelection.Raw)
                throw new InvalidRequestException("Cannot use collection element selection when defining a materialized view");
            if (selectable instanceof Selectable.WithSliceSelection.Raw)
                throw new InvalidRequestException("Cannot use collection slice selection when defining a materialized view");
            if (selector.alias != null)
                throw new InvalidRequestException("Cannot use alias when defining a materialized view");

            Selectable s = selectable.prepare(metadata);
            if (s instanceof Term.Raw)
                throw new InvalidRequestException("Cannot use terms in selection when defining a materialized view");

            ColumnMetadata cdef = (ColumnMetadata)s;
            included.add(cdef.name);
        }

        Set<ColumnMetadata.Raw> targetPrimaryKeys = new HashSet<>();
        for (ColumnMetadata.Raw identifier : Iterables.concat(partitionKeys, clusteringKeys))
        {
            if (!targetPrimaryKeys.add(identifier))
                throw new InvalidRequestException("Duplicate entry found in PRIMARY KEY: "+identifier);

            ColumnMetadata cdef = identifier.prepare(metadata);

            if (cdef.type.isMultiCell())
                throw new InvalidRequestException(String.format("Cannot use MultiCell column '%s' in PRIMARY KEY of materialized view", identifier));

            if (cdef.isStatic())
                throw new InvalidRequestException(String.format("Cannot use Static column '%s' in PRIMARY KEY of materialized view", identifier));

            if (cdef.type instanceof DurationType)
                throw new InvalidRequestException(String.format("Cannot use Duration column '%s' in PRIMARY KEY of materialized view", identifier));
        }

        // build the select statement
        Map<ColumnMetadata.Raw, Boolean> orderings = Collections.emptyMap();
        List<ColumnMetadata.Raw> groups = Collections.emptyList();
        SelectStatement.Parameters parameters = new SelectStatement.Parameters(orderings, groups, false, true, false);

        SelectStatement.RawStatement rawSelect = new SelectStatement.RawStatement(baseName, parameters, selectClause, whereClause, null, null);

        ClientState state = ClientState.forInternalCalls();
        state.setKeyspace(keyspace());

        rawSelect.prepareKeyspace(state);
        rawSelect.setBoundVariables(getBoundVariables());

        ParsedStatement.Prepared prepared = rawSelect.prepare(true);
        SelectStatement select = (SelectStatement) prepared.statement;
        StatementRestrictions restrictions = select.getRestrictions();

        if (!prepared.boundNames.isEmpty())
            throw new InvalidRequestException("Cannot use query parameters in CREATE MATERIALIZED VIEW statements");

        // SEE CASSANDRA-13798, use it if the use case is append-only.
        final boolean allowFilteringNonKeyColumns = Boolean.parseBoolean(System.getProperty("cassandra.mv.allow_filtering_nonkey_columns_unsafe",
                                                                                            "false"));
        if (!restrictions.nonPKRestrictedColumns(false).isEmpty() && !allowFilteringNonKeyColumns)
        {
            throw new InvalidRequestException(
                                              String.format("Non-primary key columns cannot be restricted in the SELECT statement used"
                                                      + " for materialized view creation (got restrictions on: %s)",
                                                            restrictions.nonPKRestrictedColumns(false)
                                                                        .stream()
                                                                        .map(def -> def.name.toString())
                                                                        .collect(Collectors.joining(", "))));
        }

        String whereClauseText = View.relationsToWhereClause(whereClause.relations);

        Set<ColumnIdentifier> basePrimaryKeyCols = new HashSet<>();
        for (ColumnMetadata definition : Iterables.concat(metadata.partitionKeyColumns(), metadata.clusteringColumns()))
            basePrimaryKeyCols.add(definition.name);

        List<ColumnIdentifier> targetClusteringColumns = new ArrayList<>();
        List<ColumnIdentifier> targetPartitionKeys = new ArrayList<>();

        // This is only used as an intermediate state; this is to catch whether multiple non-PK columns are used
        boolean hasNonPKColumn = false;
        for (ColumnMetadata.Raw raw : partitionKeys)
            hasNonPKColumn |= getColumnIdentifier(metadata, basePrimaryKeyCols, hasNonPKColumn, raw, targetPartitionKeys, restrictions);

        for (ColumnMetadata.Raw raw : clusteringKeys)
            hasNonPKColumn |= getColumnIdentifier(metadata, basePrimaryKeyCols, hasNonPKColumn, raw, targetClusteringColumns, restrictions);

        // We need to include all of the primary key columns from the base table in order to make sure that we do not
        // overwrite values in the view. We cannot support "collapsing" the base table into a smaller number of rows in
        // the view because if we need to generate a tombstone, we have no way of knowing which value is currently being
        // used in the view and whether or not to generate a tombstone. In order to not surprise our users, we require
        // that they include all of the columns. We provide them with a list of all of the columns left to include.
        boolean missingClusteringColumns = false;
        StringBuilder columnNames = new StringBuilder();
        List<ColumnIdentifier> includedColumns = new ArrayList<>();
        for (ColumnMetadata def : metadata.columns())
        {
            ColumnIdentifier identifier = def.name;
            boolean includeDef = included.isEmpty() || included.contains(identifier);

            if (includeDef && def.isStatic())
            {
                throw new InvalidRequestException(String.format("Unable to include static column '%s' which would be included by Materialized View SELECT * statement", identifier));
            }

            boolean defInTargetPrimaryKey = targetClusteringColumns.contains(identifier)
                                            || targetPartitionKeys.contains(identifier);

            if (includeDef && !defInTargetPrimaryKey)
            {
                includedColumns.add(identifier);
            }
            if (!def.isPrimaryKeyColumn()) continue;

            if (!defInTargetPrimaryKey)
            {
                if (missingClusteringColumns)
                    columnNames.append(',');
                else
                    missingClusteringColumns = true;
                columnNames.append(identifier);
            }
        }
        if (missingClusteringColumns)
            throw new InvalidRequestException(String.format("Cannot create Materialized View %s without primary key columns from base %s (%s)",
                                                            columnFamily(), baseName.getColumnFamily(), columnNames.toString()));

        if (targetPartitionKeys.isEmpty())
            throw new InvalidRequestException("Must select at least a column for a Materialized View");

        if (targetClusteringColumns.isEmpty())
            throw new InvalidRequestException("No columns are defined for Materialized View other than primary key");

        TableParams params = properties.properties.asNewTableParams();

        if (params.defaultTimeToLive > 0)
        {
            throw new InvalidRequestException("Cannot set default_time_to_live for a materialized view. " +
                                              "Data in a materialized view always expire at the same time than " +
                                              "the corresponding data in the parent table.");
        }

        TableMetadata.Builder builder =
            TableMetadata.builder(keyspace(), columnFamily(), properties.properties.getId())
                         .kind(TableMetadata.Kind.VIEW)
                         .params(params);

        add(metadata, targetPartitionKeys, builder::addPartitionKeyColumn);
        add(metadata, targetClusteringColumns, builder::addClusteringColumn);
        add(metadata, includedColumns, builder::addRegularColumn);

        ViewMetadata definition = new ViewMetadata(keyspace(),
                                                   columnFamily(),
                                                   metadata.id,
                                                   metadata.name,
                                                   included.isEmpty(),
                                                   rawSelect,
                                                   whereClauseText,
                                                   builder.build());

        logger.warn("Creating materialized view {} for {}.{}. " +
                    "Materialized views are experimental and are not recommended for production use.",
                    definition.name, metadata.keyspace, metadata.name);

        try
        {
            ClientWarn.instance.warn("Materialized views are experimental and are not recommended for production use.");
            MigrationManager.announceNewView(definition, isLocalOnly);
            return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
        }
        catch (AlreadyExistsException e)
        {
            if (ifNotExists)
                return null;
            throw e;
        }
    }

    private static boolean getColumnIdentifier(TableMetadata cfm,
                                               Set<ColumnIdentifier> basePK,
                                               boolean hasNonPKColumn,
                                               ColumnMetadata.Raw raw,
                                               List<ColumnIdentifier> columns,
                                               StatementRestrictions restrictions)
    {
        ColumnMetadata def = raw.prepare(cfm);

        boolean isPk = basePK.contains(def.name);
        if (!isPk && hasNonPKColumn)
            throw new InvalidRequestException(String.format("Cannot include more than one non-primary key column '%s' in materialized view primary key", def.name));

        // We don't need to include the "IS NOT NULL" filter on a non-composite partition key
        // because we will never allow a single partition key to be NULL
        boolean isSinglePartitionKey = def.isPartitionKey()
                                       && cfm.partitionKeyColumns().size() == 1;
        if (!isSinglePartitionKey && !restrictions.isRestricted(def))
            throw new InvalidRequestException(String.format("Primary key column '%s' is required to be filtered by 'IS NOT NULL'", def.name));

        columns.add(def.name);
        return !isPk;
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_VIEW, keyspace(), cfName.getColumnFamily());
    }
}
