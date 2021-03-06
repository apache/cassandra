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

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static java.util.Comparator.comparing;

import static com.google.common.collect.Iterables.concat;

public final class CreateTableStatement extends AlterSchemaStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateTableStatement.class);
    private final String tableName;

    private final Map<ColumnIdentifier, CQL3Type.Raw> rawColumns;
    private final Set<ColumnIdentifier> staticColumns;
    private final List<ColumnIdentifier> partitionKeyColumns;
    private final List<ColumnIdentifier> clusteringColumns;

    private final LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder;
    private final TableAttributes attrs;

    private final boolean ifNotExists;
    private final boolean useCompactStorage;

    public CreateTableStatement(String keyspaceName,
                                String tableName,

                                Map<ColumnIdentifier, CQL3Type.Raw> rawColumns,
                                Set<ColumnIdentifier> staticColumns,
                                List<ColumnIdentifier> partitionKeyColumns,
                                List<ColumnIdentifier> clusteringColumns,

                                LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder,
                                TableAttributes attrs,

                                boolean ifNotExists,
                                boolean useCompactStorage)
    {
        super(keyspaceName);
        this.tableName = tableName;

        this.rawColumns = rawColumns;
        this.staticColumns = staticColumns;
        this.partitionKeyColumns = partitionKeyColumns;
        this.clusteringColumns = clusteringColumns;

        this.clusteringOrder = clusteringOrder;
        this.attrs = attrs;

        this.ifNotExists = ifNotExists;
        this.useCompactStorage = useCompactStorage;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        if (keyspace.hasTable(tableName))
        {
            if (ifNotExists)
                return schema;

            throw new AlreadyExistsException(keyspaceName, tableName);
        }

        TableMetadata table = builder(keyspace.types).build();
        table.validate();

        if (keyspace.createReplicationStrategy().hasTransientReplicas()
            && table.params.readRepair != ReadRepairStrategy.NONE)
        {
            throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.with(table)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.CREATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureKeyspacePermission(keyspaceName, Permission.CREATE);
    }

    @Override
    Set<IResource> createdResources(KeyspacesDiff diff)
    {
        return ImmutableSet.of(DataResource.table(keyspaceName, tableName));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TABLE, keyspaceName, tableName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, tableName);
    }

    public TableMetadata.Builder builder(Types types)
    {
        attrs.validate();
        TableParams params = attrs.asNewTableParams();

        // use a TreeMap to preserve ordering across JDK versions (see CASSANDRA-9492) - important for stable unit tests
        Map<ColumnIdentifier, CQL3Type> columns = new TreeMap<>(comparing(o -> o.bytes));
        rawColumns.forEach((column, type) -> columns.put(column, type.prepare(keyspaceName, types)));

        // check for nested non-frozen UDTs or collections in a non-frozen UDT
        columns.forEach((column, type) ->
        {
            if (type.isUDT() && type.getType().isMultiCell())
            {
                ((UserType) type.getType()).fieldTypes().forEach(field ->
                {
                    if (field.isMultiCell())
                        throw ire("Non-frozen UDTs with nested non-frozen collections are not supported");
                });
            }
        });

        /*
         * Deal with PRIMARY KEY columns
         */

        HashSet<ColumnIdentifier> primaryKeyColumns = new HashSet<>();
        concat(partitionKeyColumns, clusteringColumns).forEach(column ->
        {
            CQL3Type type = columns.get(column);
            if (null == type)
                throw ire("Unknown column '%s' referenced in PRIMARY KEY for table '%s'", column, tableName);

            if (!primaryKeyColumns.add(column))
                throw ire("Duplicate column '%s' in PRIMARY KEY clause for table '%s'", column, tableName);

            if (type.getType().isMultiCell())
            {
                if (type.isCollection())
                    throw ire("Invalid non-frozen collection type %s for PRIMARY KEY column '%s'", type, column);
                else
                    throw ire("Invalid non-frozen user-defined type %s for PRIMARY KEY column '%s'", type, column);
            }

            if (type.getType().isCounter())
                throw ire("counter type is not supported for PRIMARY KEY column '%s'", column);

            if (type.getType().referencesDuration())
                throw ire("duration type is not supported for PRIMARY KEY column '%s'", column);

            if (staticColumns.contains(column))
                throw ire("Static column '%s' cannot be part of the PRIMARY KEY", column);
        });

        List<AbstractType<?>> partitionKeyTypes = new ArrayList<>();
        List<AbstractType<?>> clusteringTypes = new ArrayList<>();

        partitionKeyColumns.forEach(column ->
        {
            CQL3Type type = columns.remove(column);
            partitionKeyTypes.add(type.getType());
        });

        clusteringColumns.forEach(column ->
        {
            CQL3Type type = columns.remove(column);
            boolean reverse = !clusteringOrder.getOrDefault(column, true);
            clusteringTypes.add(reverse ? ReversedType.getInstance(type.getType()) : type.getType());
        });

        if (clusteringOrder.size() > clusteringColumns.size())
            throw ire("Only clustering columns can be defined in CLUSTERING ORDER directive");

        int n = 0;
        for (ColumnIdentifier id : clusteringOrder.keySet())
        {
            ColumnIdentifier c = clusteringColumns.get(n);
            if (!id.equals(c))
            {
                if (clusteringOrder.containsKey(c))
                    throw ire("The order of columns in the CLUSTERING ORDER directive must match that of the clustering columns (%s must appear before %s)", c, id);
                else
                    throw ire("Missing CLUSTERING ORDER for column %s", c);
            }
            ++n;
        }

        // For COMPACT STORAGE, we reject any "feature" that we wouldn't be able to translate back to thrift.
        if (useCompactStorage)
        {
            validateCompactTable(clusteringTypes, columns);
        }
        else
        {
            // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
            if (clusteringColumns.isEmpty() && !staticColumns.isEmpty())
                throw ire("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
        }

        /*
         * Counter table validation
         */

        boolean hasCounters = rawColumns.values().stream().anyMatch(CQL3Type.Raw::isCounter);
        if (hasCounters)
        {
            // We've handled anything that is not a PRIMARY KEY so columns only contains NON-PK columns. So
            // if it's a counter table, make sure we don't have non-counter types
            if (columns.values().stream().anyMatch(t -> !t.getType().isCounter()))
                throw ire("Cannot mix counter and non counter columns in the same table");

            if (params.defaultTimeToLive > 0)
                throw ire("Cannot set %s on a table with counters", TableParams.Option.DEFAULT_TIME_TO_LIVE);
        }

        /*
         * Create the builder
         */

        TableMetadata.Builder builder = TableMetadata.builder(keyspaceName, tableName);

        if (attrs.hasProperty(TableAttributes.ID))
            builder.id(attrs.getId());

        builder.isCounter(hasCounters)
               .params(params);

        for (int i = 0; i < partitionKeyColumns.size(); i++)
            builder.addPartitionKeyColumn(partitionKeyColumns.get(i), partitionKeyTypes.get(i));

        for (int i = 0; i < clusteringColumns.size(); i++)
            builder.addClusteringColumn(clusteringColumns.get(i), clusteringTypes.get(i));

        if (useCompactStorage)
        {
            fixupCompactTable(clusteringTypes, columns, hasCounters, builder);
        }
        else
        {
            columns.forEach((column, type) -> {
                if (staticColumns.contains(column))
                    builder.addStaticColumn(column, type.getType());
                else
                    builder.addRegularColumn(column, type.getType());
            });
        }
        return builder;
    }

    private void validateCompactTable(List<AbstractType<?>> clusteringTypes,
                                      Map<ColumnIdentifier, CQL3Type> columns)
    {
        boolean isDense = !clusteringTypes.isEmpty();

        if (columns.values().stream().anyMatch(c -> c.getType().isMultiCell()))
            throw ire("Non-frozen collections and UDTs are not supported with COMPACT STORAGE");
        if (!staticColumns.isEmpty())
            throw ire("Static columns are not supported in COMPACT STORAGE tables");

        if (clusteringTypes.isEmpty())
        {
            // It's a thrift "static CF" so there should be some columns definition
            if (columns.isEmpty())
                throw ire("No definition found that is not part of the PRIMARY KEY");
        }

        if (isDense)
        {
            // We can have no columns (only the PK), but we can't have more than one.
            if (columns.size() > 1)
                throw ire(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(columns.keySet(), ", ")));
        }
        else
        {
            // we are in the "static" case, so we need at least one column defined. For non-compact however, having
            // just the PK is fine.
            if (columns.isEmpty())
                throw ire("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
        }
    }

    private void fixupCompactTable(List<AbstractType<?>> clusteringTypes,
                                   Map<ColumnIdentifier, CQL3Type> columns,
                                   boolean hasCounters,
                                   TableMetadata.Builder builder)
    {
        Set<TableMetadata.Flag> flags = EnumSet.noneOf(TableMetadata.Flag.class);
        boolean isDense = !clusteringTypes.isEmpty();
        boolean isCompound = clusteringTypes.size() > 1;

        if (isDense)
            flags.add(TableMetadata.Flag.DENSE);
        if (isCompound)
            flags.add(TableMetadata.Flag.COMPOUND);
        if (hasCounters)
            flags.add(TableMetadata.Flag.COUNTER);

        boolean isStaticCompact = !isDense && !isCompound;

        builder.flags(flags);

        columns.forEach((name, type) -> {
            // Note that for "static" no-clustering compact storage we use static for the defined columns
            if (staticColumns.contains(name) || isStaticCompact)
                builder.addStaticColumn(name, type.getType());
            else
                builder.addRegularColumn(name, type.getType());
        });

        DefaultNames names = new DefaultNames(builder.columnNames());
        // Compact tables always have a clustering and a single regular value.
        if (isStaticCompact)
        {
            builder.addClusteringColumn(names.defaultClusteringName(), UTF8Type.instance);
            builder.addRegularColumn(names.defaultCompactValueName(), hasCounters ? CounterColumnType.instance : BytesType.instance);
        }
        else if (!builder.hasRegularColumns())
        {
            // Even for dense, we might not have our regular column if it wasn't part of the declaration. If
            // that's the case, add it but with a specific EmptyType so we can recognize that case later
            builder.addRegularColumn(names.defaultCompactValueName(), EmptyType.instance);
        }
    }

    @Override
    public Set<String> clientWarnings(KeyspacesDiff diff)
    {
        int tableCount = Schema.instance.getNumberOfTables();
        if (tableCount > DatabaseDescriptor.tableCountWarnThreshold())
        {
            String msg = String.format("Cluster already contains %d tables in %d keyspaces. Having a large number of tables will significantly slow down schema dependent cluster operations.",
                                       tableCount,
                                       Schema.instance.getKeyspaces().size());
            logger.warn(msg);
            return ImmutableSet.of(msg);
        }
        return ImmutableSet.of();
    }

    private static class DefaultNames
    {
        private static final String DEFAULT_CLUSTERING_NAME = "column";
        private static final String DEFAULT_COMPACT_VALUE_NAME = "value";

        private final Set<String> usedNames;
        private int clusteringIndex = 1;
        private int compactIndex = 0;

        private DefaultNames(Set<String> usedNames)
        {
            this.usedNames = usedNames;
        }

        public String defaultClusteringName()
        {
            while (true)
            {
                String candidate = DEFAULT_CLUSTERING_NAME + clusteringIndex;
                ++clusteringIndex;
                if (usedNames.add(candidate))
                    return candidate;
            }
        }

        public String defaultCompactValueName()
        {
            while (true)
            {
                String candidate = compactIndex == 0 ? DEFAULT_COMPACT_VALUE_NAME : DEFAULT_COMPACT_VALUE_NAME + compactIndex;
                ++compactIndex;
                if (usedNames.add(candidate))
                    return candidate;
            }
        }
    }

    public static TableMetadata.Builder parse(String cql, String keyspace)
    {
        return CQLFragmentParser.parseAny(CqlParser::createTableStatement, cql, "CREATE TABLE")
                                .keyspace(keyspace)
                                .prepare(null) // works around a messy ClientState/QueryProcessor class init deadlock
                                .builder(Types.none());
    }

    public final static class Raw extends CQLStatement.Raw
    {
        private final QualifiedName name;
        private final boolean ifNotExists;

        private boolean useCompactStorage = false;
        private final Map<ColumnIdentifier, CQL3Type.Raw> rawColumns = new HashMap<>();
        private final Set<ColumnIdentifier> staticColumns = new HashSet<>();
        private final List<ColumnIdentifier> clusteringColumns = new ArrayList<>();

        private List<ColumnIdentifier> partitionKeyColumns;

        private final LinkedHashMap<ColumnIdentifier, Boolean> clusteringOrder = new LinkedHashMap<>();
        public final TableAttributes attrs = new TableAttributes();

        public Raw(QualifiedName name, boolean ifNotExists)
        {
            this.name = name;
            this.ifNotExists = ifNotExists;
        }

        public CreateTableStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace();

            if (null == partitionKeyColumns)
                throw ire("No PRIMARY KEY specifed for table '%s' (exactly one required)", name);

            return new CreateTableStatement(keyspaceName,
                                            name.getName(),

                                            rawColumns,
                                            staticColumns,
                                            partitionKeyColumns,
                                            clusteringColumns,

                                            clusteringOrder,
                                            attrs,

                                            ifNotExists,
                                            useCompactStorage);
        }

        public String keyspace()
        {
            return name.getKeyspace();
        }

        public Raw keyspace(String keyspace)
        {
            name.setKeyspace(keyspace, true);
            return this;
        }

        public String table()
        {
            return name.getName();
        }

        public void addColumn(ColumnIdentifier column, CQL3Type.Raw type, boolean isStatic)
        {
            if (null != rawColumns.put(column, type))
                throw ire("Duplicate column '%s' declaration for table '%s'", column, name);

            if (isStatic)
                staticColumns.add(column);
        }

        public void setCompactStorage()
        {
            useCompactStorage = true;
        }

        public void setPartitionKeyColumn(ColumnIdentifier column)
        {
            setPartitionKeyColumns(Collections.singletonList(column));
        }

        public void setPartitionKeyColumns(List<ColumnIdentifier> columns)
        {
            if (null != partitionKeyColumns)
                throw ire("Multiple PRIMARY KEY specified for table '%s' (exactly one required)", name);

            partitionKeyColumns = columns;
        }

        public void markClusteringColumn(ColumnIdentifier column)
        {
            clusteringColumns.add(column);
        }

        public void extendClusteringOrder(ColumnIdentifier column, boolean ascending)
        {
            if (null != clusteringOrder.put(column, ascending))
                throw ire("Duplicate column '%s' in CLUSTERING ORDER BY clause for table '%s'", column, name);
        }
    }
}
