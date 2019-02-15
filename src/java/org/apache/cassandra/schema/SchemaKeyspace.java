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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.config.CFMetaData.DroppedColumn;
import org.apache.cassandra.config.ColumnDefinition.ClusteringOrder;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.schema.CQLTypeParser.parse;

/**
 * system_schema.* tables and methods for manipulating them.
 */
public final class SchemaKeyspace
{
    private SchemaKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SchemaKeyspace.class);

    private static final boolean FLUSH_SCHEMA_TABLES = Boolean.parseBoolean(System.getProperty("cassandra.test.flush_local_schema_changes", "true"));
    private static final boolean IGNORE_CORRUPTED_SCHEMA_TABLES = Boolean.parseBoolean(System.getProperty("cassandra.ignore_corrupted_schema_tables", "false"));

    public static final String KEYSPACES = "keyspaces";
    public static final String TABLES = "tables";
    public static final String COLUMNS = "columns";
    public static final String DROPPED_COLUMNS = "dropped_columns";
    public static final String TRIGGERS = "triggers";
    public static final String VIEWS = "views";
    public static final String TYPES = "types";
    public static final String FUNCTIONS = "functions";
    public static final String AGGREGATES = "aggregates";
    public static final String INDEXES = "indexes";

    /**
     * The order in this list matters.
     *
     * When flushing schema tables, we want to flush them in a way that mitigates the effects of an abrupt shutdown whilst
     * the tables are being flushed. On startup, we load the schema from disk before replaying the CL, so we need to
     * try to avoid problems like reading a table without columns or types, for example. So columns and types should be
     * flushed before tables, which should be flushed before keyspaces.
     *
     * When truncating, the order should be reversed. For immutable lists this is an efficient operation that simply
     * iterates in reverse order.
     *
     * See CASSANDRA-12213 for more details.
     */
    public static final ImmutableList<String> ALL =
        ImmutableList.of(COLUMNS, DROPPED_COLUMNS, TRIGGERS, TYPES, FUNCTIONS, AGGREGATES, INDEXES, TABLES, VIEWS, KEYSPACES);

    /**
     * The tables to which we added the cdc column. This is used in {@link #makeUpdateForSchema} below to make sure we skip that
     * column is cdc is disabled as the columns breaks pre-cdc to post-cdc upgrades (typically, 3.0 -> 3.X).
     */
    private static final Set<String> TABLES_WITH_CDC_ADDED = ImmutableSet.of(TABLES, VIEWS);


    /**
     * Until we upgrade the messaging service version, that is version 4.0, we must preserve the old order (before CASSANDRA-12213)
     * for digest calculations, otherwise the nodes will never agree on the schema during a rolling upgrade, see CASSANDRA-13559.
     */
    public static final ImmutableList<String> ALL_FOR_DIGEST =
        ImmutableList.of(KEYSPACES, TABLES, COLUMNS, TRIGGERS, VIEWS, TYPES, FUNCTIONS, AGGREGATES, INDEXES);

    private static final CFMetaData Keyspaces =
        compile(KEYSPACES,
                "keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "replication frozen<map<text, text>>,"
                + "PRIMARY KEY ((keyspace_name)))");

    private static final CFMetaData Tables =
        compile(TABLES,
                "table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching frozen<map<text, text>>,"
                + "comment text,"
                + "compaction frozen<map<text, text>>,"
                + "compression frozen<map<text, text>>,"
                + "crc_check_chance double,"
                + "dclocal_read_repair_chance double,"
                + "default_time_to_live int,"
                + "extensions frozen<map<text, blob>>,"
                + "flags frozen<set<text>>," // SUPER, COUNTER, DENSE, COMPOUND
                + "gc_grace_seconds int,"
                + "id uuid,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "cdc boolean,"
                + "PRIMARY KEY ((keyspace_name), table_name))");

    private static final CFMetaData Columns =
        compile(COLUMNS,
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

    private static final CFMetaData DroppedColumns =
        compile(DROPPED_COLUMNS,
                "dropped column registry",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "column_name text,"
                + "dropped_time timestamp,"
                + "kind text,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final CFMetaData Triggers =
        compile(TRIGGERS,
                "trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "trigger_name text,"
                + "options frozen<map<text, text>>,"
                + "PRIMARY KEY ((keyspace_name), table_name, trigger_name))");

    private static final CFMetaData Views =
        compile(VIEWS,
                "view definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "view_name text,"
                + "base_table_id uuid,"
                + "base_table_name text,"
                + "where_clause text,"
                + "bloom_filter_fp_chance double,"
                + "caching frozen<map<text, text>>,"
                + "comment text,"
                + "compaction frozen<map<text, text>>,"
                + "compression frozen<map<text, text>>,"
                + "crc_check_chance double,"
                + "dclocal_read_repair_chance double,"
                + "default_time_to_live int,"
                + "extensions frozen<map<text, blob>>,"
                + "gc_grace_seconds int,"
                + "id uuid,"
                + "include_all_columns boolean,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "cdc boolean,"
                + "PRIMARY KEY ((keyspace_name), view_name))");

    private static final CFMetaData Indexes =
        compile(INDEXES,
                "secondary index definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "index_name text,"
                + "kind text,"
                + "options frozen<map<text, text>>,"
                + "PRIMARY KEY ((keyspace_name), table_name, index_name))");

    private static final CFMetaData Types =
        compile(TYPES,
                "user defined type definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "type_name text,"
                + "field_names frozen<list<text>>,"
                + "field_types frozen<list<text>>,"
                + "PRIMARY KEY ((keyspace_name), type_name))");

    private static final CFMetaData Functions =
        compile(FUNCTIONS,
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

    private static final CFMetaData Aggregates =
        compile(AGGREGATES,
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

    public static final List<CFMetaData> ALL_TABLE_METADATA =
        ImmutableList.of(Keyspaces, Tables, Columns, Triggers, DroppedColumns, Views, Types, Functions, Aggregates, Indexes);

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SchemaConstants.SCHEMA_KEYSPACE_NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SCHEMA_KEYSPACE_NAME, KeyspaceParams.local(), org.apache.cassandra.schema.Tables.of(ALL_TABLE_METADATA));
    }

    /**
     * Add entries to system_schema.* for the hardcoded system keyspaces
     */
    public static void saveSystemKeyspacesSchema()
    {
        KeyspaceMetadata system = Schema.instance.getKSMetaData(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        KeyspaceMetadata schema = Schema.instance.getKSMetaData(SchemaConstants.SCHEMA_KEYSPACE_NAME);

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

    public static void truncate()
    {
        ALL.reverse().forEach(table -> getSchemaCFS(table).truncateBlocking());
    }

    static void flush()
    {
        if (!DatabaseDescriptor.isUnsafeSystem())
            ALL.forEach(table -> FBUtilities.waitOnFuture(getSchemaCFS(table).forceFlush()));
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     *
     * This implementation is special cased for 3.11 as it returns the schema digests for 3.11
     * <em>and</em> 3.0 - i.e. with and without the beloved {@code cdc} column.
     */
    public static Pair<UUID, UUID> calculateSchemaDigest()
    {
        Set<ByteBuffer> cdc = Collections.singleton(ByteBufferUtil.bytes("cdc"));

        return calculateSchemaDigest(cdc);
    }

    @VisibleForTesting
    static Pair<UUID, UUID> calculateSchemaDigest(Set<ByteBuffer> columnsToExclude)
    {
        MessageDigest digest;
        MessageDigest digest30;
        try
        {
            digest = MessageDigest.getInstance("MD5");
            digest30 = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        for (String table : ALL_FOR_DIGEST)
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
                        {
                            RowIterators.digest(partition, digest, digest30, columnsToExclude);
                        }
                    }
                }
            }
        }

        return Pair.create(UUID.nameUUIDFromBytes(digest.digest()), UUID.nameUUIDFromBytes(digest30.digest()));
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
        return PartitionRangeReadCommand.allDataRead(cfs.metadata, FBUtilities.nowInSeconds());
    }

    public static Collection<Mutation> convertSchemaToMutations()
    {
        Map<DecoratedKey, Mutation> mutationMap = new HashMap<>();

        for (String table : ALL)
            convertSchemaToMutations(mutationMap, table);

        return mutationMap.values();
    }

    private static void convertSchemaToMutations(Map<DecoratedKey, Mutation> mutationMap, String schemaTableName)
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
                    Mutation mutation = mutationMap.get(key);
                    if (mutation == null)
                    {
                        mutation = new Mutation(SchemaConstants.SCHEMA_KEYSPACE_NAME, key);
                        mutationMap.put(key, mutation);
                    }

                    mutation.add(makeUpdateForSchema(partition, cmd.columnFilter()));
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
        if (DatabaseDescriptor.isCDCEnabled() || !TABLES_WITH_CDC_ADDED.contains(partition.metadata().cfName))
            return PartitionUpdate.fromIterator(partition, filter);

        // We want to skip the 'cdc' column. A simple solution for that is based on the fact that
        // 'PartitionUpdate.fromIterator()' will ignore any columns that are marked as 'fetched' but not 'queried'.
        ColumnFilter.Builder builder = ColumnFilter.allColumnsBuilder(partition.metadata());
        for (ColumnDefinition column : filter.fetchedColumns())
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

    private static DecoratedKey decorate(CFMetaData metadata, Object value)
    {
        return metadata.decorateKey(((AbstractType)metadata.getKeyValidator()).decompose(value));
    }

    public static Mutation.SimpleBuilder makeCreateKeyspaceMutation(String name, KeyspaceParams params, long timestamp)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(Keyspaces.ksName, decorate(Keyspaces, name))
                                                 .timestamp(timestamp);

        builder.update(Keyspaces)
               .row()
               .add(KeyspaceParams.Option.DURABLE_WRITES.toString(), params.durableWrites)
               .add(KeyspaceParams.Option.REPLICATION.toString(), params.replication.asMap());

        return builder;
    }

    public static Mutation.SimpleBuilder makeCreateKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        keyspace.tables.forEach(table -> addTableToSchemaMutation(table, true, builder));
        keyspace.views.forEach(view -> addViewToSchemaMutation(view, true, builder));
        keyspace.types.forEach(type -> addTypeToSchemaMutation(type, builder));
        keyspace.functions.udfs().forEach(udf -> addFunctionToSchemaMutation(udf, builder));
        keyspace.functions.udas().forEach(uda -> addAggregateToSchemaMutation(uda, builder));

        return builder;
    }

    public static Mutation.SimpleBuilder makeDropKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(SchemaConstants.SCHEMA_KEYSPACE_NAME, decorate(Keyspaces, keyspace.name))
                                                 .timestamp(timestamp);

        for (CFMetaData schemaTable : ALL_TABLE_METADATA)
            builder.update(schemaTable).delete();

        return builder;
    }

    public static Mutation.SimpleBuilder makeCreateTypeMutation(KeyspaceMetadata keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addTypeToSchemaMutation(type, builder);
        return builder;
    }

    static void addTypeToSchemaMutation(UserType type, Mutation.SimpleBuilder mutation)
    {
        mutation.update(Types)
                .row(type.getNameAsString())
                .add("field_names", type.fieldNames().stream().map(FieldIdentifier::toString).collect(toList()))
                .add("field_types", type.fieldTypes().stream().map(AbstractType::asCQL3Type).map(CQL3Type::toString).collect(toList()));
    }

    public static Mutation.SimpleBuilder dropTypeFromSchemaMutation(KeyspaceMetadata keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        builder.update(Types).row(type.name).delete();
        return builder;
    }

    public static Mutation.SimpleBuilder makeCreateTableMutation(KeyspaceMetadata keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addTableToSchemaMutation(table, true, builder);
        return builder;
    }

    public static void addTableToSchemaMutation(CFMetaData table, boolean withColumnsAndTriggers, Mutation.SimpleBuilder builder)
    {
        Row.SimpleBuilder rowBuilder = builder.update(Tables)
                                              .row(table.cfName)
                                              .add("id", table.cfId)
                                              .add("flags", CFMetaData.flagsToStrings(table.flags()));

        addTableParamsToRowBuilder(table.params, rowBuilder);

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, builder);

            for (CFMetaData.DroppedColumn column : table.getDroppedColumns().values())
                addDroppedColumnToSchemaMutation(table, column, builder);

            for (TriggerMetadata trigger : table.getTriggers())
                addTriggerToSchemaMutation(table, trigger, builder);

            for (IndexMetadata index : table.getIndexes())
                addIndexToSchemaMutation(table, index, builder);
        }
    }

    private static void addTableParamsToRowBuilder(TableParams params, Row.SimpleBuilder builder)
    {
        builder.add("bloom_filter_fp_chance", params.bloomFilterFpChance)
               .add("comment", params.comment)
               .add("dclocal_read_repair_chance", params.dcLocalReadRepairChance)
               .add("default_time_to_live", params.defaultTimeToLive)
               .add("gc_grace_seconds", params.gcGraceSeconds)
               .add("max_index_interval", params.maxIndexInterval)
               .add("memtable_flush_period_in_ms", params.memtableFlushPeriodInMs)
               .add("min_index_interval", params.minIndexInterval)
               .add("read_repair_chance", params.readRepairChance)
               .add("speculative_retry", params.speculativeRetry.toString())
               .add("crc_check_chance", params.crcCheckChance)
               .add("caching", params.caching.asMap())
               .add("compaction", params.compaction.asMap())
               .add("compression", params.compression.asMap())
               .add("extensions", params.extensions);

        // Only add CDC-enabled flag to schema if it's enabled on the node. This is to work around RTE's post-8099 if a 3.8+
        // node sends table schema to a < 3.8 versioned node with an unknown column.
        if (DatabaseDescriptor.isCDCEnabled())
            builder.add("cdc", params.cdc);
    }

    public static Mutation.SimpleBuilder makeUpdateTableMutation(KeyspaceMetadata keyspace,
                                                                 CFMetaData oldTable,
                                                                 CFMetaData newTable,
                                                                 long timestamp)
    {
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        addTableToSchemaMutation(newTable, false, builder);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldTable.getColumnMetadata(),
                                                                                 newTable.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
            dropColumnFromSchemaMutation(oldTable, column, builder);

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, builder);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumnDefinition(name), builder);

        // dropped columns
        MapDifference<ByteBuffer, CFMetaData.DroppedColumn> droppedColumnDiff =
            Maps.difference(oldTable.getDroppedColumns(), newTable.getDroppedColumns());

        // newly dropped columns
        for (CFMetaData.DroppedColumn column : droppedColumnDiff.entriesOnlyOnRight().values())
            addDroppedColumnToSchemaMutation(newTable, column, builder);

        // columns added then dropped again
        for (ByteBuffer name : droppedColumnDiff.entriesDiffering().keySet())
            addDroppedColumnToSchemaMutation(newTable, newTable.getDroppedColumns().get(name), builder);

        MapDifference<String, TriggerMetadata> triggerDiff = triggersDiff(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, builder);

        // newly created triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, builder);

        MapDifference<String, IndexMetadata> indexesDiff = indexesDiff(oldTable.getIndexes(),
                                                                       newTable.getIndexes());

        // dropped indexes
        for (IndexMetadata index : indexesDiff.entriesOnlyOnLeft().values())
            dropIndexFromSchemaMutation(oldTable, index, builder);

        // newly created indexes
        for (IndexMetadata index : indexesDiff.entriesOnlyOnRight().values())
            addIndexToSchemaMutation(newTable, index, builder);

        // updated indexes need to be updated
        for (MapDifference.ValueDifference<IndexMetadata> diff : indexesDiff.entriesDiffering().values())
            addUpdatedIndexToSchemaMutation(newTable, diff.rightValue(), builder);

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

    public static Mutation.SimpleBuilder makeDropTableMutation(KeyspaceMetadata keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        builder.update(Tables).row(table.cfName).delete();

        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, builder);

        for (CFMetaData.DroppedColumn column : table.getDroppedColumns().values())
            dropDroppedColumnFromSchemaMutation(table, column, timestamp, builder);

        for (TriggerMetadata trigger : table.getTriggers())
            dropTriggerFromSchemaMutation(table, trigger, builder);

        for (IndexMetadata index : table.getIndexes())
            dropIndexFromSchemaMutation(table, index, builder);

        return builder;
    }

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, Mutation.SimpleBuilder builder)
    {
        AbstractType<?> type = column.type;
        if (type instanceof ReversedType)
            type = ((ReversedType) type).baseType;

        builder.update(Columns)
               .row(table.cfName, column.name.toString())
               .add("column_name_bytes", column.name.bytes)
               .add("kind", column.kind.toString().toLowerCase())
               .add("position", column.position())
               .add("clustering_order", column.clusteringOrder().toString().toLowerCase())
               .add("type", type.asCQL3Type().toString());
    }

    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, Mutation.SimpleBuilder builder)
    {
        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        builder.update(Columns).row(table.cfName, column.name.toString()).delete();
    }

    private static void addDroppedColumnToSchemaMutation(CFMetaData table, CFMetaData.DroppedColumn column, Mutation.SimpleBuilder builder)
    {
        builder.update(DroppedColumns)
               .row(table.cfName, column.name)
               .add("dropped_time", new Date(TimeUnit.MICROSECONDS.toMillis(column.droppedTime)))
               .add("kind", null != column.kind ? column.kind.toString().toLowerCase() : null)
               .add("type", expandUserTypes(column.type).asCQL3Type().toString());
    }

    private static void dropDroppedColumnFromSchemaMutation(CFMetaData table, DroppedColumn column, long timestamp, Mutation.SimpleBuilder builder)
    {
        builder.update(DroppedColumns).row(table.cfName, column.name).delete();
    }

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerMetadata trigger, Mutation.SimpleBuilder builder)
    {
        builder.update(Triggers)
               .row(table.cfName, trigger.name)
               .add("options", Collections.singletonMap("class", trigger.classOption));
    }

    private static void dropTriggerFromSchemaMutation(CFMetaData table, TriggerMetadata trigger, Mutation.SimpleBuilder builder)
    {
        builder.update(Triggers).row(table.cfName, trigger.name).delete();
    }

    public static Mutation.SimpleBuilder makeCreateViewMutation(KeyspaceMetadata keyspace, ViewDefinition view, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addViewToSchemaMutation(view, true, builder);
        return builder;
    }

    private static void addViewToSchemaMutation(ViewDefinition view, boolean includeColumns, Mutation.SimpleBuilder builder)
    {
        CFMetaData table = view.metadata;
        Row.SimpleBuilder rowBuilder = builder.update(Views)
                                              .row(view.viewName)
                                              .add("include_all_columns", view.includeAllColumns)
                                              .add("base_table_id", view.baseTableId)
                                              .add("base_table_name", view.baseTableMetadata().cfName)
                                              .add("where_clause", view.whereClause)
                                              .add("id", table.cfId);

        addTableParamsToRowBuilder(table.params, rowBuilder);

        if (includeColumns)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, builder);

            for (CFMetaData.DroppedColumn column : table.getDroppedColumns().values())
                addDroppedColumnToSchemaMutation(table, column, builder);
        }
    }

    public static Mutation.SimpleBuilder makeDropViewMutation(KeyspaceMetadata keyspace, ViewDefinition view, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        builder.update(Views).row(view.viewName).delete();

        CFMetaData table = view.metadata;
        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, builder);

        for (IndexMetadata index : table.getIndexes())
            dropIndexFromSchemaMutation(table, index, builder);

        return builder;
    }

    public static Mutation.SimpleBuilder makeUpdateViewMutation(Mutation.SimpleBuilder builder,
                                                                ViewDefinition oldView,
                                                                ViewDefinition newView)
    {
        addViewToSchemaMutation(newView, false, builder);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldView.metadata.getColumnMetadata(),
                                                                                 newView.metadata.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
            dropColumnFromSchemaMutation(oldView.metadata, column, builder);

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newView.metadata, column, builder);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newView.metadata, newView.metadata.getColumnDefinition(name), builder);

        // dropped columns
        MapDifference<ByteBuffer, CFMetaData.DroppedColumn> droppedColumnDiff =
            Maps.difference(oldView.metadata.getDroppedColumns(), oldView.metadata.getDroppedColumns());

        // newly dropped columns
        for (CFMetaData.DroppedColumn column : droppedColumnDiff.entriesOnlyOnRight().values())
            addDroppedColumnToSchemaMutation(oldView.metadata, column, builder);

        // columns added then dropped again
        for (ByteBuffer name : droppedColumnDiff.entriesDiffering().keySet())
            addDroppedColumnToSchemaMutation(newView.metadata, newView.metadata.getDroppedColumns().get(name), builder);

        return builder;
    }

    private static void addIndexToSchemaMutation(CFMetaData table,
                                                 IndexMetadata index,
                                                 Mutation.SimpleBuilder builder)
    {
        builder.update(Indexes)
               .row(table.cfName, index.name)
               .add("kind", index.kind.toString())
               .add("options", index.options);
    }

    private static void dropIndexFromSchemaMutation(CFMetaData table,
                                                    IndexMetadata index,
                                                    Mutation.SimpleBuilder builder)
    {
        builder.update(Indexes).row(table.cfName, index.name).delete();
    }

    private static void addUpdatedIndexToSchemaMutation(CFMetaData table,
                                                        IndexMetadata index,
                                                        Mutation.SimpleBuilder builder)
    {
        addIndexToSchemaMutation(table, index, builder);
    }

    public static Mutation.SimpleBuilder makeCreateFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addFunctionToSchemaMutation(function, builder);
        return builder;
    }

    static void addFunctionToSchemaMutation(UDFunction function, Mutation.SimpleBuilder builder)
    {
        builder.update(Functions)
               .row(function.name().name, functionArgumentsList(function))
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

    private static List<String> functionArgumentsList(AbstractFunction fun)
    {
        return fun.argTypes()
                  .stream()
                  .map(AbstractType::asCQL3Type)
                  .map(CQL3Type::toString)
                  .collect(toList());
    }

    public static Mutation.SimpleBuilder makeDropFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        builder.update(Functions).row(function.name().name, functionArgumentsList(function)).delete();
        return builder;
    }

    public static Mutation.SimpleBuilder makeCreateAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addAggregateToSchemaMutation(aggregate, builder);
        return builder;
    }

    static void addAggregateToSchemaMutation(UDAggregate aggregate, Mutation.SimpleBuilder builder)
    {
        builder.update(Aggregates)
               .row(aggregate.name().name, functionArgumentsList(aggregate))
               .add("return_type", aggregate.returnType().asCQL3Type().toString())
               .add("state_func", aggregate.stateFunction().name().name)
               .add("state_type", aggregate.stateType().asCQL3Type().toString())
               .add("final_func", aggregate.finalFunction() != null ? aggregate.finalFunction().name().name : null)
               .add("initcond", aggregate.initialCondition() != null
                                // must use the frozen state type here, as 'null' for unfrozen collections may mean 'empty'
                                ? aggregate.stateType().freeze().asCQL3Type().toCQLLiteral(aggregate.initialCondition(), ProtocolVersion.CURRENT)
                                : null);
    }

    public static Mutation.SimpleBuilder makeDropAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation.SimpleBuilder builder = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        builder.update(Aggregates).row(aggregate.name().name, functionArgumentsList(aggregate)).delete();
        return builder;
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

    private static Keyspaces fetchKeyspacesOnly(Set<String> includedKeyspaceNames)
    {
        /*
         * We know the keyspace names we are going to query, but we still want to run the SELECT IN
         * query, to filter out the keyspaces that had been dropped by the applied mutation set.
         */
        String query = format("SELECT keyspace_name FROM %s.%s WHERE keyspace_name IN ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, KEYSPACES);

        Keyspaces.Builder keyspaces = org.apache.cassandra.schema.Keyspaces.builder();
        for (UntypedResultSet.Row row : query(query, new ArrayList<>(includedKeyspaceNames)))
            keyspaces.add(fetchKeyspace(row.getString("keyspace_name")));
        return keyspaces.build();
    }

    private static KeyspaceMetadata fetchKeyspace(String keyspaceName)
    {
        KeyspaceParams params = fetchKeyspaceParams(keyspaceName);
        Types types = fetchTypes(keyspaceName);
        Tables tables = fetchTables(keyspaceName, types);
        Views views = fetchViews(keyspaceName, types);
        Functions functions = fetchFunctions(keyspaceName, types);
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

    private static Tables fetchTables(String keyspaceName, Types types)
    {
        String query = format("SELECT table_name FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TABLES);

        Tables.Builder tables = org.apache.cassandra.schema.Tables.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
        {
            String tableName = row.getString("table_name");
            try
            {
                tables.add(fetchTable(keyspaceName, tableName, types));
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

                if (IGNORE_CORRUPTED_SCHEMA_TABLES)
                {
                    logger.error(errorMsg, "", exc);
                }
                else
                {
                    logger.error(errorMsg, "restart cassandra with -Dcassandra.ignore_corrupted_schema_tables=true and ");
                    throw exc;
                }
            }
        }
        return tables.build();
    }

    private static CFMetaData fetchTable(String keyspaceName, String tableName, Types types)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, TABLES);
        UntypedResultSet rows = query(query, keyspaceName, tableName);
        if (rows.isEmpty())
            throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, tableName));
        UntypedResultSet.Row row = rows.one();

        UUID id = row.getUUID("id");

        Set<CFMetaData.Flag> flags = CFMetaData.flagsFromStrings(row.getFrozenSet("flags", UTF8Type.instance));

        boolean isSuper = flags.contains(CFMetaData.Flag.SUPER);
        boolean isCounter = flags.contains(CFMetaData.Flag.COUNTER);
        boolean isDense = flags.contains(CFMetaData.Flag.DENSE);
        boolean isCompound = flags.contains(CFMetaData.Flag.COMPOUND);

        List<ColumnDefinition> columns = fetchColumns(keyspaceName, tableName, types);
        if (!columns.stream().anyMatch(ColumnDefinition::isPartitionKey))
        {
            String msg = String.format("Table %s.%s did not have any partition key columns in the schema tables", keyspaceName, tableName);
            throw new AssertionError(msg);
        }

        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = fetchDroppedColumns(keyspaceName, tableName);
        Indexes indexes = fetchIndexes(keyspaceName, tableName);
        Triggers triggers = fetchTriggers(keyspaceName, tableName);

        return CFMetaData.create(keyspaceName,
                                 tableName,
                                 id,
                                 isDense,
                                 isCompound,
                                 isSuper,
                                 isCounter,
                                 false,
                                 columns,
                                 DatabaseDescriptor.getPartitioner())
                         .params(createTableParamsFromRow(row))
                         .droppedColumns(droppedColumns)
                         .indexes(indexes)
                         .triggers(triggers);
    }

    public static TableParams createTableParamsFromRow(UntypedResultSet.Row row)
    {
        return TableParams.builder()
                          .bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"))
                          .caching(CachingParams.fromMap(row.getFrozenTextMap("caching")))
                          .comment(row.getString("comment"))
                          .compaction(CompactionParams.fromMap(row.getFrozenTextMap("compaction")))
                          .compression(CompressionParams.fromMap(row.getFrozenTextMap("compression")))
                          .dcLocalReadRepairChance(row.getDouble("dclocal_read_repair_chance"))
                          .defaultTimeToLive(row.getInt("default_time_to_live"))
                          .extensions(row.getFrozenMap("extensions", UTF8Type.instance, BytesType.instance))
                          .gcGraceSeconds(row.getInt("gc_grace_seconds"))
                          .maxIndexInterval(row.getInt("max_index_interval"))
                          .memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms"))
                          .minIndexInterval(row.getInt("min_index_interval"))
                          .readRepairChance(row.getDouble("read_repair_chance"))
                          .crcCheckChance(row.getDouble("crc_check_chance"))
                          .speculativeRetry(SpeculativeRetryParam.fromString(row.getString("speculative_retry")))
                          .cdc(row.has("cdc") ? row.getBoolean("cdc") : false)
                          .build();
    }

    private static List<ColumnDefinition> fetchColumns(String keyspace, String table, Types types)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, COLUMNS);
        UntypedResultSet columnRows = query(query, keyspace, table);
        if (columnRows.isEmpty())
            throw new MissingColumns("Columns not found in schema table for " + keyspace + "." + table);

        List<ColumnDefinition> columns = new ArrayList<>();
        columnRows.forEach(row -> columns.add(createColumnFromRow(row, types)));

        if (columns.stream().noneMatch(ColumnDefinition::isPartitionKey))
            throw new MissingColumns("No partition key columns found in schema table for " + keyspace + "." + table);

        return columns;
    }

    public static ColumnDefinition createColumnFromRow(UntypedResultSet.Row row, Types types)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");

        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase());

        int position = row.getInt("position");
        ClusteringOrder order = ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());

        AbstractType<?> type = parse(keyspace, row.getString("type"), types);
        if (order == ClusteringOrder.DESC)
            type = ReversedType.getInstance(type);

        ColumnIdentifier name = new ColumnIdentifier(row.getBytes("column_name_bytes"), row.getString("column_name"));

        return new ColumnDefinition(keyspace, table, name, type, position, kind);
    }

    private static Map<ByteBuffer, CFMetaData.DroppedColumn> fetchDroppedColumns(String keyspace, String table)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, DROPPED_COLUMNS);
        Map<ByteBuffer, CFMetaData.DroppedColumn> columns = new HashMap<>();
        for (UntypedResultSet.Row row : query(query, keyspace, table))
        {
            CFMetaData.DroppedColumn column = createDroppedColumnFromRow(row);
            columns.put(UTF8Type.instance.decompose(column.name), column);
        }
        return columns;
    }

    private static CFMetaData.DroppedColumn createDroppedColumnFromRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        String name = row.getString("column_name");

        ColumnDefinition.Kind kind =
            row.has("kind") ? ColumnDefinition.Kind.valueOf(row.getString("kind").toUpperCase())
                            : null;
        /*
         * we never store actual UDT names in dropped column types (so that we can safely drop types if nothing refers to
         * them anymore), so before storing dropped columns in schema we expand UDTs to tuples. See expandUserTypes method.
         * Because of that, we can safely pass Types.none() to parse()
         */
        AbstractType<?> type = parse(keyspace, row.getString("type"), org.apache.cassandra.schema.Types.none());
        long droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getLong("dropped_time"));
        return new CFMetaData.DroppedColumn(name, kind, type, droppedTime);
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

    private static Views fetchViews(String keyspaceName, Types types)
    {
        String query = format("SELECT view_name FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, VIEWS);

        Views.Builder views = org.apache.cassandra.schema.Views.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
            views.add(fetchView(keyspaceName, row.getString("view_name"), types));
        return views.build();
    }

    private static ViewDefinition fetchView(String keyspaceName, String viewName, Types types)
    {
        String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, VIEWS);
        UntypedResultSet rows = query(query, keyspaceName, viewName);
        if (rows.isEmpty())
            throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspaceName, viewName));
        UntypedResultSet.Row row = rows.one();

        UUID id = row.getUUID("id");
        UUID baseTableId = row.getUUID("base_table_id");
        String baseTableName = row.getString("base_table_name");
        boolean includeAll = row.getBoolean("include_all_columns");
        String whereClause = row.getString("where_clause");

        List<ColumnDefinition> columns = fetchColumns(keyspaceName, viewName, types);

        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = fetchDroppedColumns(keyspaceName, viewName);

        CFMetaData cfm = CFMetaData.create(keyspaceName,
                                           viewName,
                                           id,
                                           false,
                                           true,
                                           false,
                                           false,
                                           true,
                                           columns,
                                           DatabaseDescriptor.getPartitioner())
                                   .params(createTableParamsFromRow(row))
                                   .droppedColumns(droppedColumns);

            String rawSelect = View.buildSelectStatement(baseTableName, columns, whereClause);
            SelectStatement.RawStatement rawStatement = (SelectStatement.RawStatement) QueryProcessor.parseStatement(rawSelect);

            return new ViewDefinition(keyspaceName, viewName, baseTableId, baseTableName, includeAll, rawStatement, whereClause, cfm);
    }

    private static Functions fetchFunctions(String keyspaceName, Types types)
    {
        Functions udfs = fetchUDFs(keyspaceName, types);
        Functions udas = fetchUDAs(keyspaceName, udfs, types);

        return org.apache.cassandra.schema.Functions.builder()
                                                    .add(udfs)
                                                    .add(udas)
                                                    .build();
    }

    private static Functions fetchUDFs(String keyspaceName, Types types)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, FUNCTIONS);

        Functions.Builder functions = org.apache.cassandra.schema.Functions.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
            functions.add(createUDFFromRow(row, types));
        return functions.build();
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
            argTypes.add(parse(ksName, type, types));

        AbstractType<?> returnType = parse(ksName, row.getString("return_type"), types);

        String language = row.getString("language");
        String body = row.getString("body");
        boolean calledOnNullInput = row.getBoolean("called_on_null_input");

        org.apache.cassandra.cql3.functions.Function existing = Schema.instance.findFunction(name, argTypes).orElse(null);
        if (existing instanceof UDFunction)
        {
            // This check prevents duplicate compilation of effectively the same UDF.
            // Duplicate compilation attempts can occur on the coordinator node handling the CREATE FUNCTION
            // statement, since CreateFunctionStatement needs to execute UDFunction.create but schema migration
            // also needs that (since it needs to handle its own change).
            UDFunction udf = (UDFunction) existing;
            if (udf.argNames().equals(argNames) && // arg types checked in Functions.find call
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

    private static Functions fetchUDAs(String keyspaceName, Functions udfs, Types types)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?", SchemaConstants.SCHEMA_KEYSPACE_NAME, AGGREGATES);

        Functions.Builder aggregates = org.apache.cassandra.schema.Functions.builder();
        for (UntypedResultSet.Row row : query(query, keyspaceName))
            aggregates.add(createUDAFromRow(row, udfs, types));
        return aggregates.build();
    }

    private static UDAggregate createUDAFromRow(UntypedResultSet.Row row, Functions functions, Types types)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<AbstractType<?>> argTypes =
            row.getFrozenList("argument_types", UTF8Type.instance)
               .stream()
               .map(t -> parse(ksName, t, types))
               .collect(toList());

        AbstractType<?> returnType = parse(ksName, row.getString("return_type"), types);

        FunctionName stateFunc = new FunctionName(ksName, (row.getString("state_func")));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? parse(ksName, row.getString("state_type"), types) : null;
        ByteBuffer initcond = row.has("initcond") ? Terms.asBytes(ksName, row.getString("initcond"), stateType) : null;

        try
        {
            return UDAggregate.create(functions, name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
        }
        catch (InvalidRequestException reason)
        {
            return UDAggregate.createBroken(name, argTypes, returnType, initcond, reason);
        }
    }

    private static UntypedResultSet query(String query, Object... variables)
    {
        return executeInternal(query, variables);
    }

    /*
     * Merging schema
     */

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    public static synchronized void reloadSchemaAndAnnounceVersion()
    {
        Keyspaces before = Schema.instance.getReplicatedKeyspaces();
        Keyspaces after = fetchNonSystemKeyspaces();
        mergeSchema(before, after);
        Schema.instance.updateVersionAndAnnounce();
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     */
    public static synchronized void mergeSchemaAndAnnounceVersion(Collection<Mutation> mutations) throws ConfigurationException
    {
        mergeSchema(mutations);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchema(Collection<Mutation> mutations)
    {
        // only compare the keyspaces affected by this set of schema mutations
        Set<String> affectedKeyspaces =
        mutations.stream()
                 .map(m -> UTF8Type.instance.compose(m.key().getKey()))
                 .collect(Collectors.toSet());

        // fetch the current state of schema for the affected keyspaces only
        Keyspaces before = Schema.instance.getKeyspaces(affectedKeyspaces);

        // apply the schema mutations and flush
        mutations.forEach(Mutation::apply);
        if (FLUSH_SCHEMA_TABLES)
            flush();

        // fetch the new state of schema from schema tables (not applied to Schema.instance yet)
        Keyspaces after = fetchKeyspacesOnly(affectedKeyspaces);

        mergeSchema(before, after);
    }

    private static synchronized void mergeSchema(Keyspaces before, Keyspaces after)
    {
        MapDifference<String, KeyspaceMetadata> keyspacesDiff = before.diff(after);

        // dropped keyspaces
        for (KeyspaceMetadata keyspace : keyspacesDiff.entriesOnlyOnLeft().values())
        {
            keyspace.functions.udas().forEach(Schema.instance::dropAggregate);
            keyspace.functions.udfs().forEach(Schema.instance::dropFunction);
            keyspace.views.forEach(v -> Schema.instance.dropView(v.ksName, v.viewName));
            keyspace.tables.forEach(t -> Schema.instance.dropTable(t.ksName, t.cfName));
            keyspace.types.forEach(Schema.instance::dropType);
            Schema.instance.dropKeyspace(keyspace.name);
        }

        // new keyspaces
        for (KeyspaceMetadata keyspace : keyspacesDiff.entriesOnlyOnRight().values())
        {
            Schema.instance.addKeyspace(KeyspaceMetadata.create(keyspace.name, keyspace.params));
            keyspace.types.forEach(Schema.instance::addType);
            keyspace.tables.forEach(Schema.instance::addTable);
            keyspace.views.forEach(Schema.instance::addView);
            keyspace.functions.udfs().forEach(Schema.instance::addFunction);
            keyspace.functions.udas().forEach(Schema.instance::addAggregate);
        }

        // updated keyspaces
        for (Map.Entry<String, MapDifference.ValueDifference<KeyspaceMetadata>> diff : keyspacesDiff.entriesDiffering().entrySet())
            updateKeyspace(diff.getKey(), diff.getValue().leftValue(), diff.getValue().rightValue());
    }

    private static void updateKeyspace(String keyspaceName, KeyspaceMetadata keyspaceBefore, KeyspaceMetadata keyspaceAfter)
    {
        // calculate the deltas
        MapDifference<String, CFMetaData> tablesDiff = keyspaceBefore.tables.diff(keyspaceAfter.tables);
        MapDifference<String, ViewDefinition> viewsDiff = keyspaceBefore.views.diff(keyspaceAfter.views);
        MapDifference<ByteBuffer, UserType> typesDiff = keyspaceBefore.types.diff(keyspaceAfter.types);

        Map<Pair<FunctionName, List<String>>, UDFunction> udfsBefore = new HashMap<>();
        keyspaceBefore.functions.udfs().forEach(f -> udfsBefore.put(Pair.create(f.name(), functionArgumentsList(f)), f));
        Map<Pair<FunctionName, List<String>>, UDFunction> udfsAfter = new HashMap<>();
        keyspaceAfter.functions.udfs().forEach(f -> udfsAfter.put(Pair.create(f.name(), functionArgumentsList(f)), f));
        MapDifference<Pair<FunctionName, List<String>>, UDFunction> udfsDiff = Maps.difference(udfsBefore, udfsAfter);

        Map<Pair<FunctionName, List<String>>, UDAggregate> udasBefore = new HashMap<>();
        keyspaceBefore.functions.udas().forEach(f -> udasBefore.put(Pair.create(f.name(), functionArgumentsList(f)), f));
        Map<Pair<FunctionName, List<String>>, UDAggregate> udasAfter = new HashMap<>();
        keyspaceAfter.functions.udas().forEach(f -> udasAfter.put(Pair.create(f.name(), functionArgumentsList(f)), f));
        MapDifference<Pair<FunctionName, List<String>>, UDAggregate> udasDiff = Maps.difference(udasBefore, udasAfter);

        // update keyspace params, if changed
        if (!keyspaceBefore.params.equals(keyspaceAfter.params))
            Schema.instance.updateKeyspace(keyspaceName, keyspaceAfter.params);

        // drop everything removed
        udasDiff.entriesOnlyOnLeft().values().forEach(Schema.instance::dropAggregate);
        udfsDiff.entriesOnlyOnLeft().values().forEach(Schema.instance::dropFunction);
        viewsDiff.entriesOnlyOnLeft().values().forEach(v -> Schema.instance.dropView(v.ksName, v.viewName));
        tablesDiff.entriesOnlyOnLeft().values().forEach(t -> Schema.instance.dropTable(t.ksName, t.cfName));
        typesDiff.entriesOnlyOnLeft().values().forEach(Schema.instance::dropType);

        // add everything created
        typesDiff.entriesOnlyOnRight().values().forEach(Schema.instance::addType);
        tablesDiff.entriesOnlyOnRight().values().forEach(Schema.instance::addTable);
        viewsDiff.entriesOnlyOnRight().values().forEach(Schema.instance::addView);
        udfsDiff.entriesOnlyOnRight().values().forEach(Schema.instance::addFunction);
        udasDiff.entriesOnlyOnRight().values().forEach(Schema.instance::addAggregate);

        // update everything altered
        for (MapDifference.ValueDifference<UserType> diff : typesDiff.entriesDiffering().values())
            Schema.instance.updateType(diff.rightValue());
        for (MapDifference.ValueDifference<CFMetaData> diff : tablesDiff.entriesDiffering().values())
            Schema.instance.updateTable(diff.rightValue());
        for (MapDifference.ValueDifference<ViewDefinition> diff : viewsDiff.entriesDiffering().values())
            Schema.instance.updateView(diff.rightValue());
        for (MapDifference.ValueDifference<UDFunction> diff : udfsDiff.entriesDiffering().values())
            Schema.instance.updateFunction(diff.rightValue());
        for (MapDifference.ValueDifference<UDAggregate> diff : udasDiff.entriesDiffering().values())
            Schema.instance.updateAggregate(diff.rightValue());
    }

    /*
     * Type parsing and transformation
     */

    /*
     * Recursively replaces any instances of UserType with an equivalent TupleType.
     * We do it for dropped_columns, to allow safely dropping unused user types without retaining any references
     * in dropped_columns.
     */
    @VisibleForTesting
    public static AbstractType<?> expandUserTypes(AbstractType<?> original)
    {
        if (original instanceof UserType)
            return new TupleType(expandUserTypes(((UserType) original).fieldTypes()));

        if (original instanceof TupleType)
            return new TupleType(expandUserTypes(((TupleType) original).allTypes()));

        if (original instanceof ListType<?>)
            return ListType.getInstance(expandUserTypes(((ListType<?>) original).getElementsType()), original.isMultiCell());

        if (original instanceof MapType<?,?>)
        {
            MapType<?, ?> mt = (MapType<?, ?>) original;
            return MapType.getInstance(expandUserTypes(mt.getKeysType()), expandUserTypes(mt.getValuesType()), mt.isMultiCell());
        }

        if (original instanceof SetType<?>)
            return SetType.getInstance(expandUserTypes(((SetType<?>) original).getElementsType()), original.isMultiCell());

        // this is very unlikely to ever happen, but it's better to be safe than sorry
        if (original instanceof ReversedType<?>)
            return ReversedType.getInstance(expandUserTypes(((ReversedType) original).baseType));

        if (original instanceof CompositeType)
            return CompositeType.getInstance(expandUserTypes(original.getComponents()));

        return original;
    }

    private static List<AbstractType<?>> expandUserTypes(List<AbstractType<?>> types)
    {
        return types.stream()
                    .map(SchemaKeyspace::expandUserTypes)
                    .collect(toList());
    }

    @VisibleForTesting
    static class MissingColumns extends RuntimeException
    {
        MissingColumns(String message)
        {
            super(message);
        }
    }
}
