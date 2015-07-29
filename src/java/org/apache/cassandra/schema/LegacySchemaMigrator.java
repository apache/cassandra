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
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.lang.String.format;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;

/**
 * This majestic class performs migration from legacy (pre-3.0) system.schema_* schema tables to the new and glorious
 * system_schema keyspace.
 *
 * The goal is to not lose any information in the migration - including the timestamps.
 */
@SuppressWarnings("deprecation")
public final class LegacySchemaMigrator
{
    private LegacySchemaMigrator()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaMigrator.class);

    static final List<CFMetaData> LegacySchemaTables =
        ImmutableList.of(SystemKeyspace.LegacyKeyspaces,
                         SystemKeyspace.LegacyColumnfamilies,
                         SystemKeyspace.LegacyColumns,
                         SystemKeyspace.LegacyTriggers,
                         SystemKeyspace.LegacyUsertypes,
                         SystemKeyspace.LegacyFunctions,
                         SystemKeyspace.LegacyAggregates);

    public static void migrate()
    {
        // read metadata from the legacy schema tables
        Collection<Keyspace> keyspaces = readSchema();

        // if already upgraded, or starting a new 3.0 node, abort early
        if (keyspaces.isEmpty())
        {
            unloadLegacySchemaTables();
            return;
        }

        // write metadata to the new schema tables
        logger.info("Moving {} keyspaces from legacy schema tables to the new schema keyspace ({})",
                    keyspaces.size(),
                    SchemaKeyspace.NAME);
        keyspaces.forEach(LegacySchemaMigrator::storeKeyspaceInNewSchemaTables);

        // flush the new tables before truncating the old ones
        SchemaKeyspace.flush();

        // truncate the original tables (will be snapshotted now, and will have been snapshotted by pre-flight checks)
        logger.info("Truncating legacy schema tables");
        truncateLegacySchemaTables();

        // remove legacy schema tables from Schema, so that their presence doesn't give the users any wrong ideas
        unloadLegacySchemaTables();

        logger.info("Completed migration of legacy schema tables");
    }

    static void unloadLegacySchemaTables()
    {
        KeyspaceMetadata systemKeyspace = Schema.instance.getKSMetaData(SystemKeyspace.NAME);

        Tables systemTables = systemKeyspace.tables;
        for (CFMetaData table : LegacySchemaTables)
            systemTables = systemTables.without(table.cfName);

        LegacySchemaTables.forEach(Schema.instance::unload);

        Schema.instance.setKeyspaceMetadata(systemKeyspace.withSwapped(systemTables));
    }

    private static void truncateLegacySchemaTables()
    {
        LegacySchemaTables.forEach(table -> Schema.instance.getColumnFamilyStoreInstance(table.cfId).truncateBlocking());
    }

    private static void storeKeyspaceInNewSchemaTables(Keyspace keyspace)
    {
        Mutation mutation = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, keyspace.timestamp);

        for (Table table : keyspace.tables)
            SchemaKeyspace.addTableToSchemaMutation(table.metadata, table.timestamp, true, mutation);

        for (Type type : keyspace.types)
            SchemaKeyspace.addTypeToSchemaMutation(type.metadata, type.timestamp, mutation);

        for (Function function : keyspace.functions)
            SchemaKeyspace.addFunctionToSchemaMutation(function.metadata, function.timestamp, mutation);

        for (Aggregate aggregate : keyspace.aggregates)
            SchemaKeyspace.addAggregateToSchemaMutation(aggregate.metadata, aggregate.timestamp, mutation);

        mutation.apply();
    }

    /*
     * Read all keyspaces metadata (including nested tables, types, and functions), with their modification timestamps
     */
    private static Collection<Keyspace> readSchema()
    {
        String query = format("SELECT keyspace_name FROM %s.%s", SystemKeyspace.NAME, SystemKeyspace.LEGACY_KEYSPACES);
        Collection<String> keyspaceNames = new ArrayList<>();
        query(query).forEach(row -> keyspaceNames.add(row.getString("keyspace_name")));
        keyspaceNames.removeAll(Schema.SYSTEM_KEYSPACE_NAMES);

        Collection<Keyspace> keyspaces = new ArrayList<>();
        keyspaceNames.forEach(name -> keyspaces.add(readKeyspace(name)));
        return keyspaces;
    }

    private static Keyspace readKeyspace(String keyspaceName)
    {
        long timestamp = readKeyspaceTimestamp(keyspaceName);
        KeyspaceParams params = readKeyspaceParams(keyspaceName);

        Collection<Table> tables = readTables(keyspaceName);
        Collection<Type> types = readTypes(keyspaceName);
        Collection<Function> functions = readFunctions(keyspaceName);
        Collection<Aggregate> aggregates = readAggregates(keyspaceName);

        return new Keyspace(timestamp, keyspaceName, params, tables, types, functions, aggregates);
    }

    /*
     * Reading keyspace params
     */

    private static long readKeyspaceTimestamp(String keyspaceName)
    {
        String query = format("SELECT writeTime(durable_writes) AS timestamp FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_KEYSPACES);
        return query(query, keyspaceName).one().getLong("timestamp");
    }

    private static KeyspaceParams readKeyspaceParams(String keyspaceName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_KEYSPACES);
        UntypedResultSet.Row row = query(query, keyspaceName).one();

        boolean durableWrites = row.getBoolean("durable_writes");

        Map<String, String> replication = new HashMap<>();
        replication.putAll(fromJsonMap(row.getString("strategy_options")));
        replication.put(KeyspaceParams.Replication.CLASS, row.getString("strategy_class"));

        return KeyspaceParams.create(durableWrites, replication);
    }

    /*
     * Reading tables
     */

    private static Collection<Table> readTables(String keyspaceName)
    {
        String query = format("SELECT columnfamily_name FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_COLUMNFAMILIES);
        Collection<String> tableNames = new ArrayList<>();
        query(query, keyspaceName).forEach(row -> tableNames.add(row.getString("columnfamily_name")));

        Collection<Table> tables = new ArrayList<>();
        tableNames.forEach(name -> tables.add(readTable(keyspaceName, name)));
        return tables;
    }

    private static Table readTable(String keyspaceName, String tableName)
    {
        long timestamp = readTableTimestamp(keyspaceName, tableName);
        CFMetaData metadata = readTableMetadata(keyspaceName, tableName);
        return new Table(timestamp, metadata);
    }

    private static long readTableTimestamp(String keyspaceName, String tableName)
    {
        String query = format("SELECT writeTime(type) AS timestamp FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_COLUMNFAMILIES);
        return query(query, keyspaceName, tableName).one().getLong("timestamp");
    }

    private static CFMetaData readTableMetadata(String keyspaceName, String tableName)
    {
        String tableQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                   SystemKeyspace.NAME,
                                   SystemKeyspace.LEGACY_COLUMNFAMILIES);
        UntypedResultSet.Row tableRow = query(tableQuery, keyspaceName, tableName).one();

        String columnsQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.LEGACY_COLUMNS);
        UntypedResultSet columnRows = query(columnsQuery, keyspaceName, tableName);

        String triggersQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                      SystemKeyspace.NAME,
                                      SystemKeyspace.LEGACY_TRIGGERS);
        UntypedResultSet triggerRows = query(triggersQuery, keyspaceName, tableName);

        return decodeTableMetadata(tableRow, columnRows, triggerRows);
    }

    private static CFMetaData decodeTableMetadata(UntypedResultSet.Row tableRow,
                                                  UntypedResultSet columnRows,
                                                  UntypedResultSet triggerRows)
    {
        String ksName = tableRow.getString("keyspace_name");
        String cfName = tableRow.getString("columnfamily_name");

        AbstractType<?> rawComparator = TypeParser.parse(tableRow.getString("comparator"));
        AbstractType<?> subComparator = tableRow.has("subcomparator") ? TypeParser.parse(tableRow.getString("subcomparator")) : null;

        boolean isSuper = "super".equals(tableRow.getString("type").toLowerCase());
        boolean isDense = tableRow.getBoolean("is_dense");
        boolean isCompound = rawComparator instanceof CompositeType;

        // We don't really use the default validator but as we have it for backward compatibility, we use it to know if it's a counter table
        AbstractType<?> defaultValidator = TypeParser.parse(tableRow.getString("default_validator"));
        boolean isCounter = defaultValidator instanceof CounterColumnType;

        /*
         * With CASSANDRA-5202 we stopped inferring the cf id from the combination of keyspace/table names,
         * and started storing the generated uuids in system.schema_columnfamilies.
         *
         * In 3.0 we SHOULD NOT see tables like that (2.0-created, non-upgraded).
         * But in the off-chance that we do, we generate the deterministic uuid here.
         */
        UUID cfId = tableRow.has("cf_id")
                  ? tableRow.getUUID("cf_id")
                  : CFMetaData.generateLegacyCfId(ksName, cfName);

        boolean isCQLTable = !isSuper && !isDense && isCompound;
        boolean isStaticCompactTable = !isDense && !isCompound;

        // Internally, compact tables have a specific layout, see CompactTables. But when upgrading from
        // previous versions, they may not have the expected schema, so detect if we need to upgrade and do
        // it in createColumnsFromColumnRows.
        // We can remove this once we don't support upgrade from versions < 3.0.
        boolean needsUpgrade = !isCQLTable && checkNeedsUpgrade(columnRows, isSuper, isStaticCompactTable);

        List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(columnRows,
                                                                        ksName,
                                                                        cfName,
                                                                        rawComparator,
                                                                        subComparator,
                                                                        isSuper,
                                                                        isCQLTable,
                                                                        isStaticCompactTable,
                                                                        needsUpgrade);

        if (needsUpgrade)
        {
            addDefinitionForUpgrade(columnDefs,
                                    ksName,
                                    cfName,
                                    isStaticCompactTable,
                                    isSuper,
                                    rawComparator,
                                    subComparator,
                                    defaultValidator);
        }

        CFMetaData cfm = CFMetaData.create(ksName, cfName, cfId, isDense, isCompound, isSuper, isCounter, columnDefs);

        cfm.readRepairChance(tableRow.getDouble("read_repair_chance"));
        cfm.dcLocalReadRepairChance(tableRow.getDouble("local_read_repair_chance"));
        cfm.gcGraceSeconds(tableRow.getInt("gc_grace_seconds"));
        cfm.minCompactionThreshold(tableRow.getInt("min_compaction_threshold"));
        cfm.maxCompactionThreshold(tableRow.getInt("max_compaction_threshold"));
        if (tableRow.has("comment"))
            cfm.comment(tableRow.getString("comment"));
        if (tableRow.has("memtable_flush_period_in_ms"))
            cfm.memtableFlushPeriod(tableRow.getInt("memtable_flush_period_in_ms"));
        cfm.caching(CachingOptions.fromString(tableRow.getString("caching")));
        if (tableRow.has("default_time_to_live"))
            cfm.defaultTimeToLive(tableRow.getInt("default_time_to_live"));
        if (tableRow.has("speculative_retry"))
            cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(tableRow.getString("speculative_retry")));
        cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(tableRow.getString("compaction_strategy_class")));
        cfm.compressionParameters(CompressionParameters.fromMap(fromJsonMap(tableRow.getString("compression_parameters"))));
        cfm.compactionStrategyOptions(fromJsonMap(tableRow.getString("compaction_strategy_options")));

        if (tableRow.has("min_index_interval"))
            cfm.minIndexInterval(tableRow.getInt("min_index_interval"));

        if (tableRow.has("max_index_interval"))
            cfm.maxIndexInterval(tableRow.getInt("max_index_interval"));

        if (tableRow.has("bloom_filter_fp_chance"))
            cfm.bloomFilterFpChance(tableRow.getDouble("bloom_filter_fp_chance"));
        else
            cfm.bloomFilterFpChance(cfm.getBloomFilterFpChance());

        if (tableRow.has("dropped_columns"))
            addDroppedColumns(cfm, rawComparator, tableRow.getMap("dropped_columns", UTF8Type.instance, LongType.instance));

        cfm.triggers(createTriggersFromTriggerRows(triggerRows));

        return cfm;
    }

    // Should only be called on compact tables
    private static boolean checkNeedsUpgrade(UntypedResultSet defs, boolean isSuper, boolean isStaticCompactTable)
    {
        if (isSuper)
        {
            // Check if we've added the "supercolumn map" column yet or not
            for (UntypedResultSet.Row row : defs)
                if (row.getString("column_name").isEmpty())
                    return false;
            return true;
        }

        // For static compact tables, we need to upgrade if the regular definitions haven't been converted to static yet,
        // i.e. if we don't have a static definition yet.
        if (isStaticCompactTable)
            return !hasKind(defs, ColumnDefinition.Kind.STATIC);

        // For dense compact tables, we need to upgrade if we don't have a compact value definition
        return !hasRegularColumns(defs);
    }

    private static boolean hasRegularColumns(UntypedResultSet columnRows)
    {
        for (UntypedResultSet.Row row : columnRows)
        {
            /*
             * We need to special case and ignore the empty compact column (pre-3.0, COMPACT STORAGE, primary-key only tables),
             * since deserializeKind() will otherwise just return a REGULAR.
             * We want the proper EmptyType regular column to be added by addDefinitionForUpgrade(), so we need
             * checkNeedsUpgrade() to return true in this case.
             * See CASSANDRA-9874.
             */
            if (isEmptyCompactValueColumn(row))
                return false;

            if (deserializeKind(row.getString("type")) == ColumnDefinition.Kind.REGULAR)
                return true;
        }

        return false;
    }

    private static boolean isEmptyCompactValueColumn(UntypedResultSet.Row row)
    {
        return "compact_value".equals(row.getString("type")) && row.getString("column_name").isEmpty();
    }

    private static void addDefinitionForUpgrade(List<ColumnDefinition> defs,
                                                String ksName,
                                                String cfName,
                                                boolean isStaticCompactTable,
                                                boolean isSuper,
                                                AbstractType<?> rawComparator,
                                                AbstractType<?> subComparator,
                                                AbstractType<?> defaultValidator)
    {
        CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(defs);

        if (isSuper)
        {
            defs.add(ColumnDefinition.regularDef(ksName, cfName, CompactTables.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(subComparator, defaultValidator, true)));
        }
        else if (isStaticCompactTable)
        {
            defs.add(ColumnDefinition.clusteringKeyDef(ksName, cfName, names.defaultClusteringName(), rawComparator, null));
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), defaultValidator));
        }
        else
        {
            // For dense compact tables, we get here if we don't have a compact value column, in which case we should add it
            // (we use EmptyType to recognize that the compact value was not declared by the use (see CreateTableStatement too))
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), EmptyType.instance));
        }
    }

    private static boolean hasKind(UntypedResultSet defs, ColumnDefinition.Kind kind)
    {
        for (UntypedResultSet.Row row : defs)
            if (deserializeKind(row.getString("type")) == kind)
                return true;

        return false;
    }

    /*
     * Prior to 3.0 we used to not store the type of the dropped columns, relying on all collection info being
     * present in the comparator, forever. That allowed us to perform certain validations in AlterTableStatement
     * (namely not allowing to re-add incompatible collection columns, with the same name, but a different type).
     *
     * In 3.0, we no longer preserve the original comparator, and reconstruct it from the columns instead. That means
     * that we should preserve the type of the dropped columns now, and, during migration, fetch the types from
     * the original comparator if necessary.
     */
    private static void addDroppedColumns(CFMetaData cfm, AbstractType<?> comparator, Map<String, Long> droppedTimes)
    {
        AbstractType<?> last = comparator.getComponents().get(comparator.componentsCount() - 1);
        Map<ByteBuffer, CollectionType> collections = last instanceof ColumnToCollectionType
                                                    ? ((ColumnToCollectionType) last).defined
                                                    : Collections.emptyMap();

        for (Map.Entry<String, Long> entry : droppedTimes.entrySet())
        {
            String name = entry.getKey();
            ByteBuffer nameBytes = UTF8Type.instance.decompose(name);
            long time = entry.getValue();

            AbstractType<?> type = collections.containsKey(nameBytes)
                                 ? collections.get(nameBytes)
                                 : BytesType.instance;

            cfm.getDroppedColumns().put(nameBytes, new CFMetaData.DroppedColumn(name, type, time));
        }
    }

    private static List<ColumnDefinition> createColumnsFromColumnRows(UntypedResultSet rows,
                                                                      String keyspace,
                                                                      String table,
                                                                      AbstractType<?> rawComparator,
                                                                      AbstractType<?> rawSubComparator,
                                                                      boolean isSuper,
                                                                      boolean isCQLTable,
                                                                      boolean isStaticCompactTable,
                                                                      boolean needsUpgrade)
    {
        List<ColumnDefinition> columns = new ArrayList<>();

        for (UntypedResultSet.Row row : rows)
        {
            // Skip the empty compact value column. Make addDefinitionForUpgrade() re-add the proper REGULAR one.
            if (isEmptyCompactValueColumn(row))
                continue;

            ColumnDefinition.Kind kind = deserializeKind(row.getString("type"));
            if (needsUpgrade && isStaticCompactTable && kind == ColumnDefinition.Kind.REGULAR)
                kind = ColumnDefinition.Kind.STATIC;

            Integer componentIndex = null;
            // Note that the component_index is not useful for non-primary key parts (it never really in fact since there is
            // no particular ordering of non-PK columns, we only used to use it as a simplification but that's not needed
            // anymore)
            if (kind.isPrimaryKeyKind() && row.has("component_index"))
                componentIndex = row.getInt("component_index");

            // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
            // we need to use the comparator fromString method
            AbstractType<?> comparator = isCQLTable
                                       ? UTF8Type.instance
                                       : CompactTables.columnDefinitionComparator(kind, isSuper, rawComparator, rawSubComparator);
            ColumnIdentifier name = ColumnIdentifier.getInterned(comparator.fromString(row.getString("column_name")), comparator);

            AbstractType<?> validator = parseType(row.getString("validator"));

            IndexType indexType = null;
            if (row.has("index_type"))
                indexType = IndexType.valueOf(row.getString("index_type"));

            Map<String, String> indexOptions = null;
            if (row.has("index_options"))
                indexOptions = fromJsonMap(row.getString("index_options"));

            String indexName = null;
            if (row.has("index_name"))
                indexName = row.getString("index_name");

            columns.add(new ColumnDefinition(keyspace, table, name, validator, indexType, indexOptions, indexName, componentIndex, kind));
        }

        return columns;
    }

    private static ColumnDefinition.Kind deserializeKind(String kind)
    {
        if ("clustering_key".equalsIgnoreCase(kind))
            return ColumnDefinition.Kind.CLUSTERING;

        if ("compact_value".equalsIgnoreCase(kind))
            return ColumnDefinition.Kind.REGULAR;

        return Enum.valueOf(ColumnDefinition.Kind.class, kind.toUpperCase());
    }

    private static Triggers createTriggersFromTriggerRows(UntypedResultSet rows)
    {
        Triggers.Builder triggers = org.apache.cassandra.schema.Triggers.builder();
        rows.forEach(row -> triggers.add(createTriggerFromTriggerRow(row)));
        return triggers.build();
    }

    private static TriggerMetadata createTriggerFromTriggerRow(UntypedResultSet.Row row)
    {
        String name = row.getString("trigger_name");
        String classOption = row.getTextMap("trigger_options").get("class");
        return new TriggerMetadata(name, classOption);
    }

    /*
     * Reading user types
     */

    private static Collection<Type> readTypes(String keyspaceName)
    {
        String query = format("SELECT type_name FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_USERTYPES);
        Collection<String> typeNames = new ArrayList<>();
        query(query, keyspaceName).forEach(row -> typeNames.add(row.getString("type_name")));

        Collection<Type> types = new ArrayList<>();
        typeNames.forEach(name -> types.add(readType(keyspaceName, name)));
        return types;
    }

    private static Type readType(String keyspaceName, String typeName)
    {
        long timestamp = readTypeTimestamp(keyspaceName, typeName);
        UserType metadata = readTypeMetadata(keyspaceName, typeName);
        return new Type(timestamp, metadata);
    }

    /*
     * Unfortunately there is not a single REGULAR column in system.schema_usertypes, so annoyingly we cannot
     * use the writeTime() CQL function, and must resort to a lower level.
     */
    private static long readTypeTimestamp(String keyspaceName, String typeName)
    {
        ColumnFamilyStore store = org.apache.cassandra.db.Keyspace.open(SystemKeyspace.NAME)
                                                                  .getColumnFamilyStore(SystemKeyspace.LEGACY_USERTYPES);

        ClusteringComparator comparator = store.metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator, typeName));
        int nowInSec = FBUtilities.nowInSeconds();
        DecoratedKey key = StorageService.getPartitioner().decorateKey(AsciiType.instance.fromString(keyspaceName));
        SinglePartitionReadCommand command = SinglePartitionSliceCommand.create(store.metadata, nowInSec, key, slices);

        try (OpOrder.Group op = store.readOrdering.start();
             RowIterator partition = UnfilteredRowIterators.filter(command.queryMemtableAndDisk(store, op), nowInSec))
        {
            return partition.next().primaryKeyLivenessInfo().timestamp();
        }
    }

    private static UserType readTypeMetadata(String keyspaceName, String typeName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND type_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_USERTYPES);
        UntypedResultSet.Row row = query(query, keyspaceName, typeName).one();

        List<ByteBuffer> names =
            row.getList("field_names", UTF8Type.instance)
               .stream()
               .map(ByteBufferUtil::bytes)
               .collect(Collectors.toList());

        List<AbstractType<?>> types =
            row.getList("field_types", UTF8Type.instance)
               .stream()
               .map(LegacySchemaMigrator::parseType)
               .collect(Collectors.toList());

        return new UserType(keyspaceName, bytes(typeName), names, types);
    }

    /*
     * Reading UDFs
     */

    private static Collection<Function> readFunctions(String keyspaceName)
    {
        String query = format("SELECT function_name, signature FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_FUNCTIONS);
        HashMultimap<String, List<String>> functionSignatures = HashMultimap.create();
        query(query, keyspaceName).forEach(row ->
        {
            functionSignatures.put(row.getString("function_name"), row.getList("signature", UTF8Type.instance));
        });

        Collection<Function> functions = new ArrayList<>();
        functionSignatures.entries().forEach(pair -> functions.add(readFunction(keyspaceName, pair.getKey(), pair.getValue())));
        return functions;
    }

    private static Function readFunction(String keyspaceName, String functionName, List<String> signature)
    {
        long timestamp = readFunctionTimestamp(keyspaceName, functionName, signature);
        UDFunction metadata = readFunctionMetadata(keyspaceName, functionName, signature);
        return new Function(timestamp, metadata);
    }

    private static long readFunctionTimestamp(String keyspaceName, String functionName, List<String> signature)
    {
        String query = format("SELECT writeTime(return_type) AS timestamp " +
                              "FROM %s.%s " +
                              "WHERE keyspace_name = ? AND function_name = ? AND signature = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_FUNCTIONS);
        return query(query, keyspaceName, functionName, signature).one().getLong("timestamp");
    }

    private static UDFunction readFunctionMetadata(String keyspaceName, String functionName, List<String> signature)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND function_name = ? AND signature = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_FUNCTIONS);
        UntypedResultSet.Row row = query(query, keyspaceName, functionName, signature).one();

        FunctionName name = new FunctionName(keyspaceName, functionName);

        List<ColumnIdentifier> argNames = new ArrayList<>();
        if (row.has("argument_names"))
            for (String arg : row.getList("argument_names", UTF8Type.instance))
                argNames.add(new ColumnIdentifier(arg, true));

        List<AbstractType<?>> argTypes = new ArrayList<>();
        if (row.has("argument_types"))
            for (String type : row.getList("argument_types", UTF8Type.instance))
                argTypes.add(parseType(type));

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        String language = row.getString("language");
        String body = row.getString("body");
        boolean calledOnNullInput = row.getBoolean("called_on_null_input");

        try
        {
            return UDFunction.create(name, argNames, argTypes, returnType, calledOnNullInput, language, body);
        }
        catch (InvalidRequestException e)
        {
            return UDFunction.createBrokenFunction(name, argNames, argTypes, returnType, calledOnNullInput, language, body, e);
        }
    }

    /*
     * Reading UDAs
     */

    private static Collection<Aggregate> readAggregates(String keyspaceName)
    {
        String query = format("SELECT aggregate_name, signature FROM %s.%s WHERE keyspace_name = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_AGGREGATES);
        HashMultimap<String, List<String>> aggregateSignatures = HashMultimap.create();
        query(query, keyspaceName).forEach(row ->
        {
            aggregateSignatures.put(row.getString("aggregate_name"), row.getList("signature", UTF8Type.instance));
        });

        Collection<Aggregate> aggregates = new ArrayList<>();
        aggregateSignatures.entries().forEach(pair -> aggregates.add(readAggregate(keyspaceName, pair.getKey(), pair.getValue())));
        return aggregates;
    }

    private static Aggregate readAggregate(String keyspaceName, String aggregateName, List<String> signature)
    {
        long timestamp = readAggregateTimestamp(keyspaceName, aggregateName, signature);
        UDAggregate metadata = readAggregateMetadata(keyspaceName, aggregateName, signature);
        return new Aggregate(timestamp, metadata);
    }

    private static long readAggregateTimestamp(String keyspaceName, String aggregateName, List<String> signature)
    {
        String query = format("SELECT writeTime(return_type) AS timestamp " +
                              "FROM %s.%s " +
                              "WHERE keyspace_name = ? AND aggregate_name = ? AND signature = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_AGGREGATES);
        return query(query, keyspaceName, aggregateName, signature).one().getLong("timestamp");
    }

    private static UDAggregate readAggregateMetadata(String keyspaceName, String functionName, List<String> signature)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND function_name = ? AND signature = ?",
                              SystemKeyspace.NAME,
                              SystemKeyspace.LEGACY_AGGREGATES);
        UntypedResultSet.Row row = query(query, keyspaceName, functionName, signature).one();

        FunctionName name = new FunctionName(keyspaceName, functionName);

        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<AbstractType<?>> argTypes = new ArrayList<>();
        if (types != null)
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        FunctionName stateFunc = new FunctionName(keyspaceName, row.getString("state_func"));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(keyspaceName, row.getString("final_func")) : null;
        AbstractType<?> stateType = row.has("state_type") ? parseType(row.getString("state_type")) : null;
        ByteBuffer initcond = row.has("initcond") ? row.getBytes("initcond") : null;

        try
        {
            return UDAggregate.create(name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
        }
        catch (InvalidRequestException reason)
        {
            return UDAggregate.createBroken(name, argTypes, returnType, initcond, reason);
        }
    }

    private static UntypedResultSet query(String query, Object... values)
    {
        return QueryProcessor.executeOnceInternal(query, values);
    }

    private static AbstractType<?> parseType(String str)
    {
        return TypeParser.parse(str);
    }

    private static final class Keyspace
    {
        final long timestamp;
        final String name;
        final KeyspaceParams params;
        final Collection<Table> tables;
        final Collection<Type> types;
        final Collection<Function> functions;
        final Collection<Aggregate> aggregates;

        Keyspace(long timestamp,
                 String name,
                 KeyspaceParams params,
                 Collection<Table> tables,
                 Collection<Type> types,
                 Collection<Function> functions,
                 Collection<Aggregate> aggregates)
        {
            this.timestamp = timestamp;
            this.name = name;
            this.params = params;
            this.tables = tables;
            this.types = types;
            this.functions = functions;
            this.aggregates = aggregates;
        }
    }

    private static final class Table
    {
        final long timestamp;
        final CFMetaData metadata;

        Table(long timestamp, CFMetaData metadata)
        {
            this.timestamp = timestamp;
            this.metadata = metadata;
        }
    }

    private static final class Type
    {
        final long timestamp;
        final UserType metadata;

        Type(long timestamp, UserType metadata)
        {
            this.timestamp = timestamp;
            this.metadata = metadata;
        }
    }

    private static final class Function
    {
        final long timestamp;
        final UDFunction metadata;

        Function(long timestamp, UDFunction metadata)
        {
            this.timestamp = timestamp;
            this.metadata = metadata;
        }
    }

    private static final class Aggregate
    {
        final long timestamp;
        final UDAggregate metadata;

        Aggregate(long timestamp, UDAggregate metadata)
        {
            this.timestamp = timestamp;
            this.metadata = metadata;
        }
    }
}
