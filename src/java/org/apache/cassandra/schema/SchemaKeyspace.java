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
package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.schema.ColumnMetadata.ClusteringOrder;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Simulate;

import static java.lang.String.format;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static org.apache.cassandra.config.CassandraRelevantProperties.IGNORE_CORRUPTED_SCHEMA_TABLES;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_FLUSH_LOCAL_SCHEMA_CHANGES;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.schema.SchemaKeyspaceTables.*;
import static org.apache.cassandra.utils.Simulate.With.GLOBAL_CLOCK;

/**
 * system_schema.* tables and methods for manipulating them.
 *
 * Please notice this class is _not_ thread safe and all methods which reads or updates the data in schema keyspace
 * should be accessed only from the implementation of {@link SchemaUpdateHandler} in synchronized blocks.
 */
@NotThreadSafe
public final class SchemaKeyspace
{
    private SchemaKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SchemaKeyspace.class);

    private static final boolean FLUSH_SCHEMA_TABLES = TEST_FLUSH_LOCAL_SCHEMA_CHANGES.getBoolean();
    private static final boolean IGNORE_CORRUPTED_SCHEMA_TABLES_PROPERTY_VALUE = IGNORE_CORRUPTED_SCHEMA_TABLES.getBoolean();

    /**
     * The tables to which we added the cdc column. This is used in {@link #makeUpdateForSchema} below to make sure we skip that
     * column is cdc is disabled as the columns breaks pre-cdc to post-cdc upgrades (typically, 3.0 -> 3.X).
     */
    private static final Set<String> TABLES_WITH_CDC_ADDED = ImmutableSet.of(SchemaKeyspaceTables.TABLES, SchemaKeyspaceTables.VIEWS);

    private static final TableMetadata Keyspaces =
        parse(KEYSPACES,
              "keyspace definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "durable_writes boolean,"
              + "replication frozen<map<text, text>>,"
              + "PRIMARY KEY ((keyspace_name)))");

    private static final TableMetadata Tables =
        parse(TABLES,
              "table definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "allow_auto_snapshot boolean,"
              + "bloom_filter_fp_chance double,"
              + "caching frozen<map<text, text>>,"
              + "comment text,"
              + "compaction frozen<map<text, text>>,"
              + "compression frozen<map<text, text>>,"
              + "memtable text,"
              + "crc_check_chance double,"
              + "dclocal_read_repair_chance double," // no longer used, left for drivers' sake
              + "default_time_to_live int,"
              + "extensions frozen<map<text, blob>>,"
              + "flags frozen<set<text>>," // SUPER, COUNTER, DENSE, COMPOUND
              + "gc_grace_seconds int,"
              + "incremental_backups boolean,"
              + "id uuid,"
              + "max_index_interval int,"
              + "memtable_flush_period_in_ms int,"
              + "min_index_interval int,"
              + "read_repair_chance double," // no longer used, left for drivers' sake
              + "speculative_retry text,"
              + "additional_write_policy text,"
              + "cdc boolean,"
              + "read_repair text,"
              + "PRIMARY KEY ((keyspace_name), table_name))");

    private static final TableMetadata Columns =
        parse(COLUMNS,
              "column definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "column_name text,"
              + "clustering_order text,"
              + "column_name_bytes blob,"
              + "kind text,"
              + "position int,"
              + "type text,"
              + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final TableMetadata ColumnMasks =
    parse(COLUMN_MASKS,
          "column dynamic data masks",
          "CREATE TABLE %s ("
          + "keyspace_name text,"
          + "table_name text,"
          + "column_name text,"
          + "function_keyspace text,"
          + "function_name text,"
          + "function_argument_types frozen<list<text>>,"
          + "function_argument_values frozen<list<text>>,"
          + "function_argument_nulls frozen<list<boolean>>," // arguments that are null
          + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final TableMetadata DroppedColumns =
        parse(DROPPED_COLUMNS,
              "dropped column registry",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "column_name text,"
              + "dropped_time timestamp,"
              + "kind text,"
              + "type text,"
              + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final TableMetadata Triggers =
        parse(TRIGGERS,
              "trigger definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "trigger_name text,"
              + "options frozen<map<text, text>>,"
              + "PRIMARY KEY ((keyspace_name), table_name, trigger_name))");

    private static final TableMetadata Views =
        parse(VIEWS,
              "view definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "view_name text,"
              + "base_table_id uuid,"
              + "base_table_name text,"
              + "where_clause text,"
              + "allow_auto_snapshot boolean,"
              + "bloom_filter_fp_chance double,"
              + "caching frozen<map<text, text>>,"
              + "comment text,"
              + "compaction frozen<map<text, text>>,"
              + "compression frozen<map<text, text>>,"
              + "memtable text,"
              + "crc_check_chance double,"
              + "dclocal_read_repair_chance double," // no longer used, left for drivers' sake
              + "default_time_to_live int,"
              + "extensions frozen<map<text, blob>>,"
              + "gc_grace_seconds int,"
              + "incremental_backups boolean,"
              + "id uuid,"
              + "include_all_columns boolean,"
              + "max_index_interval int,"
              + "memtable_flush_period_in_ms int,"
              + "min_index_interval int,"
              + "read_repair_chance double," // no longer used, left for drivers' sake
              + "speculative_retry text,"
              + "additional_write_policy text,"
              + "cdc boolean,"
              + "read_repair text,"
              + "PRIMARY KEY ((keyspace_name), view_name))");

    private static final TableMetadata Indexes =
        parse(INDEXES,
              "secondary index definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "table_name text,"
              + "index_name text,"
              + "kind text,"
              + "options frozen<map<text, text>>,"
              + "PRIMARY KEY ((keyspace_name), table_name, index_name))");

    private static final TableMetadata Types =
        parse(TYPES,
              "user defined type definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "type_name text,"
              + "field_names frozen<list<text>>,"
              + "field_types frozen<list<text>>,"
              + "PRIMARY KEY ((keyspace_name), type_name))");

    private static final TableMetadata Functions =
        parse(FUNCTIONS,
              "user defined function definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "function_name text,"
              + "argument_types frozen<list<text>>,"
              + "argument_names frozen<list<text>>,"
              + "body text,"
              + "language text,"
              + "return_type text,"
              + "called_on_null_input boolean,"
              + "PRIMARY KEY ((keyspace_name), function_name, argument_types))");

    private static final TableMetadata Aggregates =
        parse(AGGREGATES,
              "user defined aggregate definitions",
              "CREATE TABLE %s ("
              + "keyspace_name text,"
              + "aggregate_name text,"
              + "argument_types frozen<list<text>>,"
              + "final_func text,"
              + "initcond text,"
              + "return_type text,"
              + "state_func text,"
              + "state_type text,"
              + "PRIMARY KEY ((keyspace_name), aggregate_name, argument_types))");

    private static final List<TableMetadata> ALL_TABLE_METADATA = ImmutableList.of(Keyspaces,
                                                                                   Tables,
                                                                                   Columns,
                                                                                   ColumnMasks,
                                                                                   Triggers,
                                                                                   DroppedColumns,
                                                                                   Views,
                                                                                   Types,
                                                                                   Functions,
                                                                                   Aggregates,
                                                                                   Indexes);

    private static TableMetadata parse(String name, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, name), SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.SCHEMA_KEYSPACE_NAME, name))
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7))
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .comment(description)
                                   .build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, KeyspaceParams.local(), org.apache.cassandra.schema.Tables.of(ALL_TABLE_METADATA));
    }

    static Collection<Mutation> convertSchemaDiffToMutations(KeyspacesDiff diff, long timestamp)
    {
        Map<String, Mutation> mutations = new HashMap<>();

        diff.dropped.forEach(k -> mutations.put(k.name, makeDropKeyspaceMutation(k, timestamp).build()));
        diff.created.forEach(k -> mutations.put(k.name, makeCreateKeyspaceMutation(k, timestamp).build()));
        diff.altered.forEach(kd ->
        {
            KeyspaceMetadata ks = kd.after;

            Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(ks.name, ks.params, timestamp);

            kd.types.dropped.forEach(t -> addDropTypeToSchemaMutation(t, builder));
            kd.types.created.forEach(t -> addTypeToSchemaMutation(t, builder));
            kd.types.altered(Difference.SHALLOW).forEach(td -> addTypeToSchemaMutation(td.after, builder));

            kd.tables.dropped.forEach(t -> addDropTableToSchemaMutation(t, builder));
            kd.tables.created.forEach(t -> addTableToSchemaMutation(t, true, builder));
            kd.tables.altered(Difference.SHALLOW).forEach(td -> addAlterTableToSchemaMutation(td.before, td.after, builder));

            kd.views.dropped.forEach(v -> addDropViewToSchemaMutation(v, builder));
            kd.views.created.forEach(v -> addViewToSchemaMutation(v, true, builder));
            kd.views.altered(Difference.SHALLOW).forEach(vd -> addAlterViewToSchemaMutation(vd.before, vd.after, builder));

            kd.udfs.dropped.forEach(f -> addDropFunctionToSchemaMutation((UDFunction) f, builder));
            kd.udfs.created.forEach(f -> addFunctionToSchemaMutation((UDFunction) f, builder));
            kd.udfs.altered(Difference.SHALLOW).forEach(fd -> addFunctionToSchemaMutation(fd.after, builder));

            kd.udas.dropped.forEach(a -> addDropAggregateToSchemaMutation((UDAggregate) a, builder));
            kd.udas.created.forEach(a -> addAggregateToSchemaMutation((UDAggregate) a, builder));
            kd.udas.altered(Difference.SHALLOW).forEach(ad -> addAggregateToSchemaMutation(ad.after, builder));

            mutations.put(ks.name, builder.build());
        });

        return mutations.values();
    }

    /**
     * Add entries to system_schema.* for the hardcoded system keyspaces
     */
    @Simulate(with = GLOBAL_CLOCK)
    static void saveSystemKeyspacesSchema()
    {
        KeyspaceMetadata system = Schema.instance.getKeyspaceMetadata(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        KeyspaceMetadata schema = Schema.instance.getKeyspaceMetadata(SchemaConstants.SCHEMA_KEYSPACE_NAME);

        long timestamp = FBUtilities.timestampMicros();

        // delete old, possibly obsolete entries in schema tables
        for (String schemaTable : ALL)
        {
            String query = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, schemaTable);
            for (String systemKeyspace : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES)
                executeOnceInternal(query, timestamp, systemKeyspace);
        }

        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        makeCreateKeyspaceMutation(system, timestamp + 1).build().apply();
        makeCreateKeyspaceMutation(schema, timestamp + 1).build().apply();
    }

    static void truncate()
    {
        logger.debug("Truncating schema tables...");
        ALL.reverse().forEach(table -> getSchemaCFS(table).truncateBlocking());
    }

    private static void flush()
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            ALL.forEach(table -> FBUtilities.waitOnFuture(getSchemaCFS(table).forceFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED)));
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public static UUID calculateSchemaDigest()
    {
        Digest digest = Digest.forSchema();
        for (String table : ALL)
        {
            ReadCommand cmd = getReadCommandForTableSchema(table);
            try (ReadExecutionController executionController = cmd.executionController();
                 PartitionIterator schema = cmd.executeInternal(executionController))
            {
                while (schema.hasNext())
                {
                    try (RowIterator partition = schema.next())
                    {
                        if (!isSystemKeyspaceSchemaPartition(partition.partitionKey()))
                            RowIterators.digest(partition, digest);
                    }
                }
            }
        }
        return UUID.nameUUIDFromBytes(digest.digest());
    }

    /**
     * @param schemaTableName The name of the table responsible for part of the schema
     * @return CFS responsible to hold low-level serialized schema
     */
    private static ColumnFamilyStore getSchemaCFS(String schemaTableName)
    {
        return Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(schemaTableName);
    }

    /**
     * @param schemaTableName The name of the table responsible for part of the schema.
     * @return low-level schema representation
     */
    private static ReadCommand getReadCommandForTableSchema(String schemaTableName)
    {
        ColumnFamilyStore cfs = getSchemaCFS(schemaTableName);
        return PartitionRangeReadCommand.allDataRead(cfs.metadata(), FBUtilities.nowInSeconds());
    }

    static Collection<Mutation> convertSchemaToMutations()
    {
        Map<DecoratedKey, Mutation.PartitionUpdateCollector> mutationMap = new HashMap<>();

        for (String table : ALL)
            convertSchemaToMutations(mutationMap, table);

        return mutationMap.values().stream().map(Mutation.PartitionUpdateCollector::build).collect(Collectors.toList());
    }

    private static void convertSchemaToMutations(Map<DecoratedKey, Mutation.PartitionUpdateCollector> mutationMap, String schemaTableName)
    {
        ReadCommand cmd = getReadCommandForTableSchema(schemaTableName);
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iter = cmd.executeLocally(executionController))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    if (isSystemKeyspaceSchemaPartition(partition.partitionKey()))
                        continue;

                    DecoratedKey key = partition.partitionKey();
                    Mutation.PartitionUpdateCollector puCollector = mutationMap.computeIfAbsent(key, k -> new Mutation.PartitionUpdateCollector(SchemaConstants.SCHEMA_KEYSPACE_NAME, key));
                    puCollector.add(makeUpdateForSchema(partition, cmd.columnFilter()).withOnlyPresentColumns());
                }
            }
        }
    }

    /**
     * Creates a PartitionUpdate from a partition containing some schema table content.
     * This is mainly calling {@code PartitionUpdate.fromIterator} except for the fact that it deals with
     * the problem described in #12236.
     */
    private static PartitionUpdate makeUpdateForSchema(UnfilteredRowIterator partition, ColumnFilter filter)
    {
        // This method is used during schema migration tasks, and if cdc is disabled, we want to force excluding the
        // 'cdc' column from the TABLES/VIEWS schema table because it is problematic if received by older nodes (see #12236
        // and #12697). Otherwise though, we just simply "buffer" the content of the partition into a PartitionUpdate.
        if (DatabaseDescriptor.isCDCEnabled() || !TABLES_WITH_CDC_ADDED.contains(partition.metadata().name))
            return PartitionUpdate.fromIterator(partition, filter);

        // We want to skip the 'cdc' column. A simple solution for that is based on the fact that
        // 'PartitionUpdate.fromIterator()' will ignore any columns that are marked as 'fetched' but not 'queried'.
        ColumnFilter.Builder builder = ColumnFilter.allRegularColumnsBuilder(partition.metadata(), false);
        for (ColumnMetadata column : filter.fetchedColumns())
        {
            if (!column.name.toString().equals("cdc"))
                builder.add(column);
        }

        return PartitionUpdate.fromIterator(partition, builder.build());
    }

    private static boolean isSystemKeyspaceSchemaPartition(DecoratedKey partitionKey)
    {
        return SchemaConstants.isLocalSystemKeyspace(UTF8Type.instance.compose(partitionKey.getKey()));
    }

    /*
     * Schema entities to mutations
     */

    @SuppressWarnings("unchecked")
    private static DecoratedKey decorate(TableMetadata metadata, Object value)
    {
        return metadata.partitioner.decorateKey(metadata.partitionKeyType.decomposeUntyped(value));
    }

    private static Mutation.SimpleBuilder makeCreateKeyspaceMutation(String name, KeyspaceParams params, long timestamp)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(Keyspaces.keyspace, decorate(Keyspaces, name))
                                                 .timestamp(timestamp);

        builder.update(Keyspaces)
               .row()
               .add(KeyspaceParams.Option.DURABLE_WRITES.toString(), params.durableWrites)
               .add(KeyspaceParams.Option.REPLICATION.toString(), params.replication.asMap());

        return builder;
    }

    @VisibleForTesting
    static Mutation.SimpleBuilder makeCreateKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        keyspace.tables.forEach(table -> addTableToSchemaMutation(table, true, builder));
        keyspace.views.forEach(view -> addViewToSchemaMutation(view, true, builder));
        keyspace.types.forEach(type -> addTypeToSchemaMutation(type, builder));
        keyspace.userFunctions.udfs().forEach(udf -> addFunctionToSchemaMutation(udf, builder));
        keyspace.userFunctions.udas().forEach(uda -> addAggregateToSchemaMutation(uda, builder));

        return builder;
    }

    private static Mutation.SimpleBuilder makeDropKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(SchemaConstants.SCHEMA_KEYSPACE_NAME, decorate(Keyspaces, keyspace.name))
                                                 .timestamp(timestamp);

        for (TableMetadata schemaTable : ALL_TABLE_METADATA)
            builder.update(schemaTable).delete();

        return builder;
    }

    private static void addTypeToSchemaMutation(UserType type, Mutation.SimpleBuilder mutation)
    {
        mutation.update(Types)
                .row(type.getNameAsString())
                .add("field_names", type.fieldNames().stream().map(FieldIdentifier::toString).collect(toList()))
                .add("field_types", type.fieldTypes().stream().map(AbstractType::asCQL3Type).map(CQL3Type::toString).collect(toList()));
    }

    private static void addDropTypeToSchemaMutation(UserType type, Mutation.SimpleBuilder builder)
    {
        builder.update(Types).row(type.name).delete();
    }

    @VisibleForTesting
    static Mutation.SimpleBuilder makeCreateTableMutation(KeyspaceMetadata keyspace, TableMetadata table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addTableToSchemaMutation(table, true, builder);
        return builder;
    }

    private static void addTableToSchemaMutation(TableMetadata table, boolean withColumnsAndTriggers, Mutation.SimpleBuilder builder)
    {
        Row.SimpleBuilder rowBuilder = builder.update(Tables)
                                              .row(table.name)
                                              .deletePrevious()
                                              .add("id", table.id.asUUID())
                                              .add("flags", TableMetadata.Flag.toStringSet(table.flags));

        addTableParamsToRowBuilder(table.params, rowBuilder);

        if (withColumnsAndTriggers)
        {
            for (ColumnMetadata column : table.columns())
                addColumnToSchemaMutation(table, column, builder);

            for (DroppedColumn column : table.droppedColumns.values())
                addDroppedColumnToSchemaMutation(table, column, builder);

            for (TriggerMetadata trigger : table.triggers)
                addTriggerToSchemaMutation(table, trigger, builder);

            for (IndexMetadata index : table.indexes)
                addIndexToSchemaMutation(table, index, builder);
        }
    }

    private static void addTableParamsToRowBuilder(TableParams params, Row.SimpleBuilder builder)
    {
        builder.add("bloom_filter_fp_chance", params.bloomFilterFpChance)
               .add("comment", params.comment)
               .add("dclocal_read_repair_chance", 0.0) // no longer used, left for drivers' sake
               .add("default_time_to_live", params.defaultTimeToLive)
               .add("gc_grace_seconds", params.gcGraceSeconds)
               .add("max_index_interval", params.maxIndexInterval)
               .add("memtable_flush_period_in_ms", params.memtableFlushPeriodInMs)
               .add("min_index_interval", params.minIndexInterval)
               .add("read_repair_chance", 0.0) // no longer used, left for drivers' sake
               .add("speculative_retry", params.speculativeRetry.toString())
               .add("additional_write_policy", params.additionalWritePolicy.toString())
               .add("crc_check_chance", params.crcCheckChance)
               .add("caching", params.caching.asMap())
               .add("compaction", params.compaction.asMap())
               .add("compression", params.compression.asMap())
               .add("read_repair", params.readRepair.toString())
               .add("extensions", params.extensions);

        // Only add CDC-enabled flag to schema if it's enabled on the node. This is to work around RTE's post-8099 if a 3.8+
        // node sends table schema to a < 3.8 versioned node with an unknown column.
        if (DatabaseDescriptor.isCDCEnabled())
            builder.add("cdc", params.cdc);

        // As above, only add the memtable column if the table uses a non-default memtable configuration to avoid RTE
        // in mixed operation with pre-4.1 versioned node during upgrades.
        if (params.memtable != MemtableParams.DEFAULT)
            builder.add("memtable", params.memtable.configurationKey());

        // As above, only add the allow_auto_snapshot column if the value is not default (true) and
        // auto-snapshotting is enabled, to avoid RTE in pre-4.2 versioned node during upgrades
        if (!params.allowAutoSnapshot)
            builder.add("allow_auto_snapshot", false);

        // As above, only add the incremental_backups column if the value is not default (true) and
        // incremental_backups is enabled, to avoid RTE in pre-4.2 versioned node during upgrades
        if (!params.incrementalBackups)
            builder.add("incremental_backups", false);
    }

    private static void addAlterTableToSchemaMutation(TableMetadata oldTable, TableMetadata newTable, Mutation.SimpleBuilder builder)
    {
        addTableToSchemaMutation(newTable, false, builder);

        MapDifference<ByteBuffer, ColumnMetadata> columnDiff = Maps.difference(oldTable.columns, newTable.columns);

        // columns that are no longer needed
        for (ColumnMetadata column : columnDiff.entriesOnlyOnLeft().values())
            dropColumnFromSchemaMutation(oldTable, column, builder);

        // newly added columns
        for (ColumnMetadata column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, builder);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumn(name), builder);

        // dropped columns
        MapDifference<ByteBuffer, DroppedColumn> droppedColumnDiff =
            Maps.difference(oldTable.droppedColumns, newTable.droppedColumns);

        // newly dropped columns
        for (DroppedColumn column : droppedColumnDiff.entriesOnlyOnRight().values())
            addDroppedColumnToSchemaMutation(newTable, column, builder);

        // columns added then dropped again
        for (ByteBuffer name : droppedColumnDiff.entriesDiffering().keySet())
            addDroppedColumnToSchemaMutation(newTable, newTable.droppedColumns.get(name), builder);

        MapDifference<String, TriggerMetadata> triggerDiff = triggersDiff(oldTable.triggers, newTable.triggers);

        // dropped triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, builder);

        // newly created triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, builder);

        MapDifference<String, IndexMetadata> indexesDiff = indexesDiff(oldTable.indexes, newTable.indexes);

        // dropped indexes
        for (IndexMetadata index : indexesDiff.entriesOnlyOnLeft().values())
            dropIndexFromSchemaMutation(oldTable, index, builder);

        // newly created indexes
        for (IndexMetadata index : indexesDiff.entriesOnlyOnRight().values())
            addIndexToSchemaMutation(newTable, index, builder);

        // updated indexes need to be updated
        for (MapDifference.ValueDifference<IndexMetadata> diff : indexesDiff.entriesDiffering().values())
            addUpdatedIndexToSchemaMutation(newTable, diff.rightValue(), builder);
    }

    @VisibleForTesting
    static Mutation.SimpleBuilder makeUpdateTableMutation(KeyspaceMetadata keyspace,
                                                          TableMetadata oldTable,
                                                          TableMetadata newTable,
                                                          long timestamp)
    {
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addAlterTableToSchemaMutation(oldTable, newTable, builder);
        return builder;
    }

    private static MapDifference<String, IndexMetadata> indexesDiff(Indexes before, Indexes after)
    {
        Map<String, IndexMetadata> beforeMap = new HashMap<>();
        before.forEach(i -> beforeMap.put(i.name, i));

        Map<String, IndexMetadata> afterMap = new HashMap<>();
        after.forEach(i -> afterMap.put(i.name, i));

        return Maps.difference(beforeMap, afterMap);
    }

    private static MapDifference<String, TriggerMetadata> triggersDiff(Triggers before, Triggers after)
    {
        Map<String, TriggerMetadata> beforeMap = new HashMap<>();
        before.forEach(t -> beforeMap.put(t.name, t));

        Map<String, TriggerMetadata> afterMap = new HashMap<>();
        after.forEach(t -> afterMap.put(t.name, t));

        return Maps.difference(beforeMap, afterMap);
    }

    private static void addDropTableToSchemaMutation(TableMetadata table, Mutation.SimpleBuilder builder)
    {
        builder.update(Tables).row(table.name).delete();

        for (ColumnMetadata column : table.columns())
            dropColumnFromSchemaMutation(table, column, builder);

        for (TriggerMetadata trigger : table.triggers)
            dropTriggerFromSchemaMutation(table, trigger, builder);

        for (DroppedColumn column : table.droppedColumns.values())
            dropDroppedColumnFromSchemaMutation(table, column, builder);

        for (IndexMetadata index : table.indexes)
            dropIndexFromSchemaMutation(table, index, builder);
    }

    private static void addColumnToSchemaMutation(TableMetadata table, ColumnMetadata column, Mutation.SimpleBuilder builder)
    {
        AbstractType<?> type = column.type;
        if (type instanceof ReversedType)
            type = ((ReversedType<?>) type).baseType;

        builder.update(Columns)
               .row(table.name, column.name.toString())
               .add("column_name_bytes", column.name.bytes)
               .add("kind", column.kind.toString().toLowerCase())
               .add("position", column.position())
               .add("clustering_order", column.clusteringOrder().toString().toLowerCase())
               .add("type", type.asCQL3Type().toString());

        ColumnMask mask = column.getMask();
        if (SchemaConstants.isReplicatedSystemKeyspace(table.keyspace))
        {
            // The propagation of system distributed keyspaces at startup can be problematic for old nodes without DDM,
            // since those won't know what to do with the mask mutations. Thus, we don't support DDM on those keyspaces.
            assert mask == null : "Dynamic data masking shouldn't be used on system distributed keyspaces";
        }
        else
        {
            Row.SimpleBuilder maskBuilder = builder.update(ColumnMasks).row(table.name, column.name.toString());

            if (mask == null)
            {
                maskBuilder.delete();
            }
            else
            {
                FunctionName maskFunctionName = mask.function.name();

                // Some arguments of the masking function can be null, but the CQL's list type that stores them doesn't
                // accept nulls, so we use a parallel list of booleans to store what arguments are null.
                List<AbstractType<?>> partialTypes = mask.partialArgumentTypes();
                List<ByteBuffer> partialValues = mask.partialArgumentValues();
                int numArgs = partialTypes.size();
                List<String> types = new ArrayList<>(numArgs);
                List<String> values = new ArrayList<>(numArgs);
                List<Boolean> nulls = new ArrayList<>(numArgs);
                for (int i = 0; i < numArgs; i++)
                {
                    AbstractType<?> argType = partialTypes.get(i);
                    types.add(argType.asCQL3Type().toString());

                    ByteBuffer argValue = partialValues.get(i);
                    boolean isNull = argValue == null;
                    nulls.add(isNull);
                    values.add(isNull ? "" : argType.getString(argValue));
                }

                maskBuilder.add("function_keyspace", maskFunctionName.keyspace)
                           .add("function_name", maskFunctionName.name)
                           .add("function_argument_types", types)
                           .add("function_argument_values", values)
                           .add("function_argument_nulls", nulls);
            }
        }
    }

    private static void dropColumnFromSchemaMutation(TableMetadata table, ColumnMetadata column, Mutation.SimpleBuilder builder)
    {
        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        builder.update(Columns).row(table.name, column.name.toString()).delete();
    }

    private static void addDroppedColumnToSchemaMutation(TableMetadata table, DroppedColumn column, Mutation.SimpleBuilder builder)
    {
        builder.update(DroppedColumns)
               .row(table.name, column.column.name.toString())
               .add("dropped_time", new Date(TimeUnit.MICROSECONDS.toMillis(column.droppedTime)))
               .add("type", column.column.type.asCQL3Type().toString())
               .add("kind", column.column.kind.toString().toLowerCase());
    }

    private static void dropDroppedColumnFromSchemaMutation(TableMetadata table, DroppedColumn column, Mutation.SimpleBuilder builder)
    {
        builder.update(DroppedColumns).row(table.name, column.column.name.toString()).delete();
    }

    private static void addTriggerToSchemaMutation(TableMetadata table, TriggerMetadata trigger, Mutation.SimpleBuilder builder)
    {
        builder.update(Triggers)
               .row(table.name, trigger.name)
               .add("options", Collections.singletonMap("class", trigger.classOption));
    }

    private static void dropTriggerFromSchemaMutation(TableMetadata table, TriggerMetadata trigger, Mutation.SimpleBuilder builder)
    {
        builder.update(Triggers).row(table.name, trigger.name).delete();
    }

    private static void addViewToSchemaMutation(ViewMetadata view, boolean includeColumns, Mutation.SimpleBuilder builder)
    {
        TableMetadata table = view.metadata;
        Row.SimpleBuilder rowBuilder = builder.update(Views)
                                              .row(view.name())
                                              .deletePrevious()
                                              .add("include_all_columns", view.includeAllColumns)
                                              .add("base_table_id", view.baseTableId.asUUID())
                                              .add("base_table_name", view.baseTableName)
                                              .add("where_clause", view.whereClause.toCQLString())
                                              .add("id", table.id.asUUID());

        addTableParamsToRowBuilder(table.params, rowBuilder);

        if (includeColumns)
        {
            for (ColumnMetadata column : table.columns())
                addColumnToSchemaMutation(table, column, builder);

            for (DroppedColumn column : table.droppedColumns.values())
                addDroppedColumnToSchemaMutation(table, column, builder);
        }
    }

    private static void addDropViewToSchemaMutation(ViewMetadata view, Mutation.SimpleBuilder builder)
    {
        builder.update(Views).row(view.name()).delete();

        TableMetadata table = view.metadata;
        for (ColumnMetadata column : table.columns())
            dropColumnFromSchemaMutation(table, column, builder);
    }

    private static void addAlterViewToSchemaMutation(ViewMetadata before, ViewMetadata after, Mutation.SimpleBuilder builder)
    {
        addViewToSchemaMutation(after, false, builder);

        MapDifference<ByteBuffer, ColumnMetadata> columnDiff = Maps.difference(before.metadata.columns, after.metadata.columns);

        // columns that are no longer needed
        for (ColumnMetadata column : columnDiff.entriesOnlyOnLeft().values())
            dropColumnFromSchemaMutation(before.metadata, column, builder);

        // newly added columns
        for (ColumnMetadata column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(after.metadata, column, builder);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(after.metadata, after.metadata.getColumn(name), builder);
    }

    private static void addIndexToSchemaMutation(TableMetadata table, IndexMetadata index, Mutation.SimpleBuilder builder)
    {
        builder.update(Indexes)
               .row(table.name, index.name)
               .add("kind", index.kind.toString())
               .add("options", index.options);
    }

    private static void dropIndexFromSchemaMutation(TableMetadata table, IndexMetadata index, Mutation.SimpleBuilder builder)
    {
        builder.update(Indexes).row(table.name, index.name).delete();
    }

    private static void addUpdatedIndexToSchemaMutation(TableMetadata table,
                                                        IndexMetadata index,
                                                        Mutation.SimpleBuilder builder)
    {
        addIndexToSchemaMutation(table, index, builder);
    }

    private static void addFunctionToSchemaMutation(UDFunction function, Mutation.SimpleBuilder builder)
    {
        builder.update(Functions)
               .row(function.name().name, function.argumentsList())
               .add("body", function.body())
               .add("language", function.language())
               .add("return_type", function.returnType().asCQL3Type().toString())
               .add("called_on_null_input", function.isCalledOnNullInput())
               .add("argument_names", function.argNames().stream().map((c) -> bbToString(c.bytes)).collect(toList()));
    }

    private static String bbToString(ByteBuffer bb)
    {
        try
        {
            return ByteBufferUtil.string(bb);
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void addDropFunctionToSchemaMutation(UDFunction function, Mutation.SimpleBuilder builder)
    {
        builder.update(Functions).row(function.name().name, function.argumentsList()).delete();
    }

    private static void addAggregateToSchemaMutation(UDAggregate aggregate, Mutation.SimpleBuilder builder)
    {
        builder.update(Aggregates)
               .row(aggregate.name().name, aggregate.argumentsList())
               .add("return_type", aggregate.returnType().asCQL3Type().toString())
               .add("state_func", aggregate.stateFunction().name().name)
               .add("state_type", aggregate.stateType().asCQL3Type().toString())
               .add("final_func", aggregate.finalFunction() != null ? aggregate.finalFunction().name().name : null)
               .add("initcond", aggregate.initialCondition() != null
                                // must use the frozen state type here, as 'null' for unfrozen collections may mean 'empty'
                                ? aggregate.stateType().freeze().asCQL3Type().toCQLLiteral(aggregate.initialCondition())
                                : null);
    }

    private static void addDropAggregateToSchemaMutation(UDAggregate aggregate, Mutation.SimpleBuilder builder)
    {
        builder.update(Aggregates).row(aggregate.name().name, aggregate.argumentsList()).delete();
    }

    /*
     * Fetching schema
     */
    public static Keyspaces fetchNonSystemKeyspaces()
    {
        return fetchKeyspacesWithout(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);
    }

    private static Keyspaces fetchKeyspacesWithout(Set<String> excludedKeyspaceNames)
    {
        String query = format("SELECT keyspace_name FROM %s.%s", SchemaConstants.SCHEMA_KEYSPACE_NAME, KEYSPACES);

        Keyspaces.Builder keyspaces = org.apache.cassandra.schema.Keyspaces.builder();
        for (UntypedResultSet.Row row : query(query))
        {
            String keyspaceName = row.getString("keyspace_name");
            if (!excludedKeyspaceNames.contains(keyspaceName))
                keyspaces.add(fetchKeyspace(keyspaceName));
        }
        return keyspaces.build();
    }

    private static KeyspaceMetadata fetchKeyspace(String keyspaceName)
    {
        KeyspaceParams params = fetchKeyspaceParams(keyspaceName);
        Types types = fetchTypes(keyspaceName);
        UserFunctions functions = fetchFunctions(keyspaceName, types);
        Tables tables = fetchTables(keyspaceName, types, functions);
        Views views = fetchViews(keyspaceName, types, functions);
        return KeyspaceMetadata.create(keyspaceName, params, tables, views, types, functions);
    }

    private static KeyspaceParams fetchKeyspaceParams(String keyspaceName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, KEYSPACES);

        UntypedResultSet.Row row = query(query, keyspaceName).one();
        boolean durableWrites = row.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString());
        Map<String, String> replication = row.getFrozenTextMap(KeyspaceParams.Option.REPLICATION.toString());
        return KeyspaceParams.create(durableWrites, replication);
    }

    private static Types fetchTypes(String keyspaceName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TYPES);

        Types.RawBuilder types = org.apache.cassandra.schema.Types.rawBuilder(keyspaceName);
        for (UntypedResultSet.Row row : query(query, keyspaceName))
        {
            String name = row.getString("type_name");
            List<String> fieldNames = row.getFrozenList("field_names", UTF8Type.instance);
            List<String> fieldTypes = row.getFrozenList("field_types", UTF8Type.instance);
            types.add(name, fieldNames, fieldTypes);
        }
        return types.build();
    }

    private static Tables fetchTables(String keyspaceName, Types types, UserFunctions functions)
    {
        String query = format("SELECT table_name FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TABLES);

        Tables.Builder tables = org.apache.cassandra.schema.Tables.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
        {
            String tableName = row.getString("table_name");
            try
            {
                tables.add(fetchTable(keyspaceName, tableName, types, functions));
            }
            catch (MissingColumns exc)
            {
                String errorMsg = String.format("No partition columns found for table %s.%s in %s.%s.  This may be due to " +
                                                "corruption or concurrent dropping and altering of a table. If this table is supposed " +
                                                "to be dropped, {}run the following query to cleanup: " +
                                                "\"DELETE FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s'; " +
                                                "DELETE FROM %s.%s WHERE keyspace_name = '%s' AND table_name = '%s';\" " +
                                                "If the table is not supposed to be dropped, restore %s.%s sstables from backups.",
                                                keyspaceName, tableName, SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMNS,
                                                SchemaConstants.SCHEMA_KEYSPACE_NAME, TABLES, keyspaceName, tableName,
                                                SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMNS, keyspaceName, tableName,
                                                SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMNS);

                if (IGNORE_CORRUPTED_SCHEMA_TABLES_PROPERTY_VALUE)
                {
                    logger.error(errorMsg, "", exc);
                }
                else
                {
                    logger.error(errorMsg, "restart cassandra with -D{}=true and ", IGNORE_CORRUPTED_SCHEMA_TABLES.getKey());
                    throw exc;
                }
            }
        }
        return tables.build();
    }

    private static TableMetadata fetchTable(String keyspaceName, String tableName, Types types, UserFunctions functions)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TABLES);
        UntypedResultSet rows = query(query, keyspaceName, tableName);
        if (rows.isEmpty())
            throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, tableName));
        UntypedResultSet.Row row = rows.one();

        Set<TableMetadata.Flag> flags = TableMetadata.Flag.fromStringSet(row.getFrozenSet("flags", UTF8Type.instance));
        return TableMetadata.builder(keyspaceName, tableName, TableId.fromUUID(row.getUUID("id")))
                            .flags(flags)
                            .params(createTableParamsFromRow(row))
                            .addColumns(fetchColumns(keyspaceName, tableName, types, functions))
                            .droppedColumns(fetchDroppedColumns(keyspaceName, tableName))
                            .indexes(fetchIndexes(keyspaceName, tableName))
                            .triggers(fetchTriggers(keyspaceName, tableName))
                            .build();
    }

    @VisibleForTesting
    static TableParams createTableParamsFromRow(UntypedResultSet.Row row)
    {
        TableParams.Builder builder = TableParams.builder()
                                                 .bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"))
                                                 .caching(CachingParams.fromMap(row.getFrozenTextMap("caching")))
                                                 .comment(row.getString("comment"))
                                                 .compaction(CompactionParams.fromMap(row.getFrozenTextMap("compaction")))
                                                 .compression(CompressionParams.fromMap(row.getFrozenTextMap("compression")))
                                                 .memtable(MemtableParams.getWithFallback(row.has("memtable")
                                                                                          ? row.getString("memtable")
                                                                                          : null)) // memtable column was introduced in 4.1
                                                 .defaultTimeToLive(row.getInt("default_time_to_live"))
                                                 .extensions(row.getFrozenMap("extensions", UTF8Type.instance, BytesType.instance))
                                                 .gcGraceSeconds(row.getInt("gc_grace_seconds"))
                                                 .maxIndexInterval(row.getInt("max_index_interval"))
                                                 .memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms"))
                                                 .minIndexInterval(row.getInt("min_index_interval"))
                                                 .crcCheckChance(row.getDouble("crc_check_chance"))
                                                 .speculativeRetry(SpeculativeRetryPolicy.fromString(row.getString("speculative_retry")))
                                                 .additionalWritePolicy(row.has("additional_write_policy") ?
                                                                        SpeculativeRetryPolicy.fromString(row.getString("additional_write_policy")) :
                                                                        SpeculativeRetryPolicy.fromString("99PERCENTILE"))
                                                 .cdc(row.has("cdc") && row.getBoolean("cdc"))
                                                 .readRepair(getReadRepairStrategy(row));

        // allow_auto_snapshot column was introduced in 4.2
        if (row.has("allow_auto_snapshot"))
            builder.allowAutoSnapshot(row.getBoolean("allow_auto_snapshot"));

        // incremental_backups column was introduced in 4.2
        if (row.has("incremental_backups"))
            builder.incrementalBackups(row.getBoolean("incremental_backups"));

        return builder.build();
    }

    private static List<ColumnMetadata> fetchColumns(String keyspace, String table, Types types, UserFunctions functions)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMNS);
        UntypedResultSet columnRows = query(query, keyspace, table);
        if (columnRows.isEmpty())
            throw new MissingColumns("Columns not found in schema table for " + keyspace + '.' + table);

        List<ColumnMetadata> columns = new ArrayList<>();
        columnRows.forEach(row -> columns.add(createColumnFromRow(row, types, functions)));

        if (columns.stream().noneMatch(ColumnMetadata::isPartitionKey))
            throw new MissingColumns("No partition key columns found in schema table for " + keyspace + "." + table);

        return columns;
    }

    @VisibleForTesting
    public static ColumnMetadata createColumnFromRow(UntypedResultSet.Row row, Types types, UserFunctions functions)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");

        ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());

        int position = row.getInt("position");
        ClusteringOrder order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());

        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
        if (order == ClusteringOrder.DESC)
            type = ReversedType.getInstance(type);

        ColumnIdentifier name = new ColumnIdentifier(row.getBytes("column_name_bytes"), row.getString("column_name"));

        ColumnMask mask = null;
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                              SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMN_MASKS);
        UntypedResultSet columnMasks = query(query, keyspace, table, name.toString());
        if (!columnMasks.isEmpty())
        {
            UntypedResultSet.Row maskRow = columnMasks.one();
            FunctionName functionName = new FunctionName(maskRow.getString("function_keyspace"), maskRow.getString("function_name"));

            List<String> partialArgumentTypes = maskRow.getFrozenList("function_argument_types", UTF8Type.instance);
            List<AbstractType<?>> argumentTypes = new ArrayList<>(1 + partialArgumentTypes.size());
            argumentTypes.add(type);
            for (String argumentType : partialArgumentTypes)
            {
                argumentTypes.add(CQLTypeParser.parse(keyspace, argumentType, types));
            }

            Function function = FunctionResolver.get(keyspace, functionName, argumentTypes, null, null, null, functions);
            if (function == null)
            {
                throw new AssertionError(format("Unable to find masking function %s(%s) for column %s.%s.%s",
                                                functionName, argumentTypes, keyspace, table, name));
            }
            else if (!(function instanceof ScalarFunction))
            {
                throw new AssertionError(format("Column %s.%s.%s is unexpectedly masked with function %s " +
                                                "which is not a scalar masking function",
                                                keyspace, table, name, function));
            }

            // Some arguments of the masking function can be null, but the CQL's list type that stores them doesn't
            // accept nulls, so we use a parallel list of booleans to store what arguments are null.
            List<Boolean> nulls = maskRow.getFrozenList("function_argument_nulls", BooleanType.instance);
            List<String> valuesAsCQL = maskRow.getFrozenList("function_argument_values", UTF8Type.instance);
            ByteBuffer[] values = new ByteBuffer[valuesAsCQL.size()];
            for (int i = 0; i < valuesAsCQL.size(); i++)
            {
                if (nulls.get(i))
                    values[i] = null;
                else
                    values[i] = argumentTypes.get(i + 1).fromString(valuesAsCQL.get(i));
            }

            mask = new ColumnMask((ScalarFunction) function, values);
        }

        return new ColumnMetadata(keyspace, table, name, type, position, kind, mask);
    }

    private static Map<ByteBuffer, DroppedColumn> fetchDroppedColumns(String keyspace, String table)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, DROPPED_COLUMNS);
        Map<ByteBuffer, DroppedColumn> columns = new HashMap<>();
        for (UntypedResultSet.Row row : query(query, keyspace, table))
        {
            DroppedColumn column = createDroppedColumnFromRow(row);
            columns.put(column.column.name.bytes, column);
        }
        return columns;
    }

    private static DroppedColumn createDroppedColumnFromRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");
        String name = row.getString("column_name");
        /*
         * we never store actual UDT names in dropped column types (so that we can safely drop types if nothing refers to
         * them anymore), so before storing dropped columns in schema we expand UDTs to tuples. See expandUserTypes method.
         * Because of that, we can safely pass Types.none() to parse()
         */
        AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), org.apache.cassandra.schema.Types.none());
        ColumnMetadata.Kind kind = row.has("kind")
                                 ? ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase())
                                 : ColumnMetadata.Kind.REGULAR;
        assert kind == ColumnMetadata.Kind.REGULAR || kind == ColumnMetadata.Kind.STATIC
            : "Unexpected dropped column kind: " + kind;

        ColumnMetadata column = new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, ColumnMetadata.NO_POSITION, kind, null);
        long droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getLong("dropped_time"));
        return new DroppedColumn(column, droppedTime);
    }

    private static Indexes fetchIndexes(String keyspace, String table)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, INDEXES);
        Indexes.Builder indexes = org.apache.cassandra.schema.Indexes.builder();
        query(query, keyspace, table).forEach(row -> indexes.add(createIndexMetadataFromRow(row)));
        return indexes.build();
    }

    private static IndexMetadata createIndexMetadataFromRow(UntypedResultSet.Row row)
    {
        String name = row.getString("index_name");
        IndexMetadata.Kind type = IndexMetadata.Kind.valueOf(row.getString("kind"));
        Map<String, String> options = row.getFrozenTextMap("options");
        return IndexMetadata.fromSchemaMetadata(name, type, options);
    }

    private static Triggers fetchTriggers(String keyspace, String table)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TRIGGERS);
        Triggers.Builder triggers = org.apache.cassandra.schema.Triggers.builder();
        query(query, keyspace, table).forEach(row -> triggers.add(createTriggerFromRow(row)));
        return triggers.build();
    }

    private static TriggerMetadata createTriggerFromRow(UntypedResultSet.Row row)
    {
        String name = row.getString("trigger_name");
        String classOption = row.getFrozenTextMap("options").get("class");
        return new TriggerMetadata(name, classOption);
    }

    private static Views fetchViews(String keyspaceName, Types types, UserFunctions functions)
    {
        String query = format("SELECT view_name FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, VIEWS);

        Views.Builder views = org.apache.cassandra.schema.Views.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
            views.put(fetchView(keyspaceName, row.getString("view_name"), types, functions));
        return views.build();
    }

    private static ViewMetadata fetchView(String keyspaceName, String viewName, Types types, UserFunctions functions)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, VIEWS);
        UntypedResultSet rows = query(query, keyspaceName, viewName);
        if (rows.isEmpty())
            throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, viewName));
        UntypedResultSet.Row row = rows.one();

        TableId baseTableId = TableId.fromUUID(row.getUUID("base_table_id"));
        String baseTableName = row.getString("base_table_name");
        boolean includeAll = row.getBoolean("include_all_columns");
        String whereClauseString = row.getString("where_clause");

        List<ColumnMetadata> columns = fetchColumns(keyspaceName, viewName, types, functions);

        TableMetadata metadata =
            TableMetadata.builder(keyspaceName, viewName, TableId.fromUUID(row.getUUID("id")))
                         .kind(TableMetadata.Kind.VIEW)
                         .addColumns(columns)
                         .droppedColumns(fetchDroppedColumns(keyspaceName, viewName))
                         .params(createTableParamsFromRow(row))
                         .build();

        WhereClause whereClause;

        try
        {
            whereClause = WhereClause.parse(whereClauseString);
        }
        catch (RecognitionException e)
        {
            throw new RuntimeException(format("Unexpected error while parsing materialized view's where clause for '%s' (got %s)", viewName, whereClauseString));
        }

        return new ViewMetadata(baseTableId, baseTableName, includeAll, whereClause, metadata);
    }

    private static UserFunctions fetchFunctions(String keyspaceName, Types types)
    {
        Collection<UDFunction> udfs = fetchUDFs(keyspaceName, types);
        Collection<UDAggregate> udas = fetchUDAs(keyspaceName, udfs, types);

        return UserFunctions.builder().add(udfs).add(udas).build();
    }

    private static Collection<UDFunction> fetchUDFs(String keyspaceName, Types types)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, FUNCTIONS);

        Collection<UDFunction> functions = new ArrayList<>();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
            functions.add(createUDFFromRow(row, types));
        return functions;
    }

    private static UDFunction createUDFFromRow(UntypedResultSet.Row row, Types types)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("function_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<ColumnIdentifier> argNames = new ArrayList<>();
        for (String arg : row.getFrozenList("argument_names", UTF8Type.instance))
            argNames.add(new ColumnIdentifier(arg, true));

        List<AbstractType<?>> argTypes = new ArrayList<>();
        for (String type : row.getFrozenList("argument_types", UTF8Type.instance))
            argTypes.add(CQLTypeParser.parse(ksName, type, types).udfType());

        AbstractType<?> returnType = CQLTypeParser.parse(ksName, row.getString("return_type"), types).udfType();

        String language = row.getString("language");
        String body = row.getString("body");
        boolean calledOnNullInput = row.getBoolean("called_on_null_input");

        /*
         * TODO: find a way to get rid of Schema.instance dependency; evaluate if the opimisation below makes a difference
         * in the first place. Remove if it isn't.
         */
        UserFunction existing = Schema.instance.findUserFunction(name, argTypes).orElse(null);
        if (existing instanceof UDFunction)
        {
            // This check prevents duplicate compilation of effectively the same UDF.
            // Duplicate compilation attempts can occur on the coordinator node handling the CREATE FUNCTION
            // statement, since CreateFunctionStatement needs to execute UDFunction.create but schema migration
            // also needs that (since it needs to handle its own change).
            UDFunction udf = (UDFunction) existing;
            if (udf.argNames().equals(argNames) &&
                udf.argTypes().equals(argTypes) &&
                udf.returnType().equals(returnType) &&
                !udf.isAggregate() &&
                udf.language().equals(language) &&
                udf.body().equals(body) &&
                udf.isCalledOnNullInput() == calledOnNullInput)
            {
                logger.trace("Skipping duplicate compilation of already existing UDF {}", name);
                return udf;
            }
        }

        try
        {
            return UDFunction.create(name, argNames, argTypes, returnType, calledOnNullInput, language, body);
        }
        catch (InvalidRequestException e)
        {
            logger.error(String.format("Cannot load function '%s' from schema: this function won't be available (on this node)", name), e);
            return UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, e);
        }
    }

    private static Collection<UDAggregate> fetchUDAs(String keyspaceName, Collection<UDFunction> udfs, Types types)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, AGGREGATES);

        Collection<UDAggregate> aggregates = new ArrayList<>();
        query(query, keyspaceName).forEach(row -> aggregates.add(createUDAFromRow(row, udfs, types)));
        return aggregates;
    }

    private static UDAggregate createUDAFromRow(UntypedResultSet.Row row, Collection<UDFunction> functions, Types types)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<AbstractType<?>> argTypes =
            row.getFrozenList("argument_types", UTF8Type.instance)
               .stream()
               .map(t -> CQLTypeParser.parse(ksName, t, types).udfType())
               .collect(toList());

        AbstractType<?> returnType = CQLTypeParser.parse(ksName, row.getString("return_type"), types).udfType();

        FunctionName stateFunc = new FunctionName(ksName, (row.getString("state_func")));

        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? CQLTypeParser.parse(ksName, row.getString("state_type"), types) : null;
        ByteBuffer initcond = row.has("initcond") ? Terms.asBytes(ksName, row.getString("initcond"), stateType) : null;

        return UDAggregate.create(functions, name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
    }

    private static UntypedResultSet query(String query, Object... variables)
    {
        return executeInternal(query, variables);
    }

    /*
     * Merging schema
     */

    /**
     * Computes the set of names of keyspaces affected by the provided schema mutations.
     */
    static Set<String> affectedKeyspaces(Collection<Mutation> mutations)
    {
        // only compare the keyspaces affected by this set of schema mutations
        return mutations.stream()
                        .map(m -> UTF8Type.instance.compose(m.key().getKey()))
                        .collect(toSet());
    }

    static void applyChanges(Collection<Mutation> mutations)
    {
        mutations.forEach(Mutation::apply);
        if (SchemaKeyspace.FLUSH_SCHEMA_TABLES)
            SchemaKeyspace.flush();
    }

    static Keyspaces fetchKeyspaces(Set<String> toFetch)
    {
        /*
         * We know the keyspace names we are going to query, but we still want to run the SELECT IN
         * query, to filter out the keyspaces that had been dropped by the applied mutation set.
         */
        String query = format("SELECT keyspace_name FROM %s.%s WHERE keyspace_name IN ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, KEYSPACES);

        Keyspaces.Builder keyspaces = org.apache.cassandra.schema.Keyspaces.builder();
        for (UntypedResultSet.Row row : query(query, new ArrayList<>(toFetch)))
            keyspaces.add(fetchKeyspace(row.getString("keyspace_name")));
        return keyspaces.build();
    }

    @VisibleForTesting
    static class MissingColumns extends RuntimeException
    {
        MissingColumns(String message)
        {
            super(message);
        }
    }

    private static ReadRepairStrategy getReadRepairStrategy(UntypedResultSet.Row row)
    {
        return row.has("read_repair")
               ? ReadRepairStrategy.fromString(row.getString("read_repair"))
               : ReadRepairStrategy.BLOCKING;
    }
}
