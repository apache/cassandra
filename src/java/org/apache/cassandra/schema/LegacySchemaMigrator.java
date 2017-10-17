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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

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
                    SchemaConstants.SCHEMA_KEYSPACE_NAME);
        keyspaces.forEach(LegacySchemaMigrator::storeKeyspaceInNewSchemaTables);
        keyspaces.forEach(LegacySchemaMigrator::migrateBuiltIndexesForKeyspace);

        // flush the new tables before truncating the old ones
        SchemaKeyspace.flush();

        // truncate the original tables (will be snapshotted now, and will have been snapshotted by pre-flight checks)
        logger.info("Truncating legacy schema tables");
        truncateLegacySchemaTables();

        // remove legacy schema tables from Schema, so that their presence doesn't give the users any wrong ideas
        unloadLegacySchemaTables();

        logger.info("Completed migration of legacy schema tables");
    }

    private static void migrateBuiltIndexesForKeyspace(Keyspace keyspace)
    {
        keyspace.tables.forEach(LegacySchemaMigrator::migrateBuiltIndexesForTable);
    }

    private static void migrateBuiltIndexesForTable(Table table)
    {
        table.metadata.getIndexes().forEach((index) -> migrateIndexBuildStatus(table.metadata.ksName,
                                                                               table.metadata.cfName,
                                                                               index));
    }

    private static void migrateIndexBuildStatus(String keyspace, String table, IndexMetadata index)
    {
        if (SystemKeyspace.isIndexBuilt(keyspace, table + '.' + index.name))
        {
            SystemKeyspace.setIndexBuilt(keyspace, index.name);
            SystemKeyspace.setIndexRemoved(keyspace, table + '.' + index.name);
        }
    }

    static void unloadLegacySchemaTables()
    {
        KeyspaceMetadata systemKeyspace = Schema.instance.getKSMetaData(SchemaConstants.SYSTEM_KEYSPACE_NAME);

        Tables systemTables = systemKeyspace.tables;
        for (CFMetaData table : LegacySchemaTables)
            systemTables = systemTables.without(table.cfName);

        LegacySchemaTables.forEach(Schema.instance::unload);
        LegacySchemaTables.forEach((cfm) -> org.apache.cassandra.db.Keyspace.openAndGetStore(cfm).invalidate());

        Schema.instance.setKeyspaceMetadata(systemKeyspace.withSwapped(systemTables));
    }

    private static void truncateLegacySchemaTables()
    {
        LegacySchemaTables.forEach(table -> Schema.instance.getColumnFamilyStoreInstance(table.cfId).truncateBlocking());
    }

    private static void storeKeyspaceInNewSchemaTables(Keyspace keyspace)
    {
        logger.info("Migrating keyspace {}", keyspace);

        Mutation.SimpleBuilder builder = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, keyspace.timestamp);
        for (Table table : keyspace.tables)
            SchemaKeyspace.addTableToSchemaMutation(table.metadata, true, builder.timestamp(table.timestamp));

        for (Type type : keyspace.types)
            SchemaKeyspace.addTypeToSchemaMutation(type.metadata, builder.timestamp(type.timestamp));

        for (Function function : keyspace.functions)
            SchemaKeyspace.addFunctionToSchemaMutation(function.metadata, builder.timestamp(function.timestamp));

        for (Aggregate aggregate : keyspace.aggregates)
            SchemaKeyspace.addAggregateToSchemaMutation(aggregate.metadata, builder.timestamp(aggregate.timestamp));

        builder.build().apply();
    }

    /*
     * Read all keyspaces metadata (including nested tables, types, and functions), with their modification timestamps
     */
    private static Collection<Keyspace> readSchema()
    {
        String query = format("SELECT keyspace_name FROM %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.LEGACY_KEYSPACES);
        Collection<String> keyspaceNames = new ArrayList<>();
        query(query).forEach(row -> keyspaceNames.add(row.getString("keyspace_name")));
        keyspaceNames.removeAll(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES);

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
        Functions.Builder functionsBuilder = Functions.builder();
        functions.forEach(udf -> functionsBuilder.add(udf.metadata));
        Collection<Aggregate> aggregates = readAggregates(functionsBuilder.build(), keyspaceName);

        return new Keyspace(timestamp, keyspaceName, params, tables, types, functions, aggregates);
    }

    /*
     * Reading keyspace params
     */

    private static long readKeyspaceTimestamp(String keyspaceName)
    {
        String query = format("SELECT writeTime(durable_writes) AS timestamp FROM %s.%s WHERE keyspace_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_KEYSPACES);
        return query(query, keyspaceName).one().getLong("timestamp");
    }

    private static KeyspaceParams readKeyspaceParams(String keyspaceName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_KEYSPACES);
        UntypedResultSet.Row row = query(query, keyspaceName).one();

        boolean durableWrites = row.getBoolean("durable_writes");

        Map<String, String> replication = new HashMap<>();
        replication.putAll(fromJsonMap(row.getString("strategy_options")));
        replication.put(ReplicationParams.CLASS, row.getString("strategy_class"));

        return KeyspaceParams.create(durableWrites, replication);
    }

    /*
     * Reading tables
     */

    private static Collection<Table> readTables(String keyspaceName)
    {
        String query = format("SELECT columnfamily_name FROM %s.%s WHERE keyspace_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
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
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_COLUMNFAMILIES);
        return query(query, keyspaceName, tableName).one().getLong("timestamp");
    }

    private static CFMetaData readTableMetadata(String keyspaceName, String tableName)
    {
        String tableQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                   SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                   SystemKeyspace.LEGACY_COLUMNFAMILIES);
        UntypedResultSet.Row tableRow = query(tableQuery, keyspaceName, tableName).one();

        String columnsQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.LEGACY_COLUMNS);
        UntypedResultSet columnRows = query(columnsQuery, keyspaceName, tableName);

        String triggersQuery = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND columnfamily_name = ?",
                                      SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                      SystemKeyspace.LEGACY_TRIGGERS);
        UntypedResultSet triggerRows = query(triggersQuery, keyspaceName, tableName);

        return decodeTableMetadata(tableName, tableRow, columnRows, triggerRows);
    }

    private static CFMetaData decodeTableMetadata(String tableName,
                                                  UntypedResultSet.Row tableRow,
                                                  UntypedResultSet columnRows,
                                                  UntypedResultSet triggerRows)
    {
        String ksName = tableRow.getString("keyspace_name");
        String cfName = tableRow.getString("columnfamily_name");

        AbstractType<?> rawComparator = TypeParser.parse(tableRow.getString("comparator"));
        AbstractType<?> subComparator = tableRow.has("subcomparator") ? TypeParser.parse(tableRow.getString("subcomparator")) : null;

        boolean isSuper = "super".equals(tableRow.getString("type").toLowerCase(Locale.ENGLISH));
        boolean isCompound = rawComparator instanceof CompositeType || isSuper;

        /*
         * Determine whether or not the table is *really* dense
         * We cannot trust is_dense value of true (see CASSANDRA-11502, that fixed the issue for 2.2 only, and not retroactively),
         * but we can trust is_dense value of false.
         */
        Boolean rawIsDense = tableRow.has("is_dense") ? tableRow.getBoolean("is_dense") : null;
        boolean isDense;
        if (rawIsDense != null && !rawIsDense)
            isDense = false;
        else
            isDense = calculateIsDense(rawComparator, columnRows, isSuper);

        // now, if switched to sparse, remove redundant compact_value column and the last clustering column,
        // directly copying CASSANDRA-11502 logic. See CASSANDRA-11315.
        Iterable<UntypedResultSet.Row> filteredColumnRows = !isDense && (rawIsDense == null || rawIsDense)
                                                          ? filterOutRedundantRowsForSparse(columnRows, isSuper, isCompound)
                                                          : columnRows;

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
        boolean needsUpgrade = !isCQLTable && checkNeedsUpgrade(filteredColumnRows, isSuper, isStaticCompactTable);

        List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(filteredColumnRows,
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

        CFMetaData cfm = CFMetaData.create(ksName,
                                           cfName,
                                           cfId,
                                           isDense,
                                           isCompound,
                                           isSuper,
                                           isCounter,
                                           false, // legacy schema did not contain views
                                           columnDefs,
                                           DatabaseDescriptor.getPartitioner());

        Indexes indexes = createIndexesFromColumnRows(cfm,
                                                      filteredColumnRows,
                                                      ksName,
                                                      cfName,
                                                      rawComparator,
                                                      subComparator,
                                                      isSuper,
                                                      isCQLTable,
                                                      isStaticCompactTable,
                                                      needsUpgrade);
        cfm.indexes(indexes);

        if (tableRow.has("dropped_columns"))
            addDroppedColumns(cfm, rawComparator, tableRow.getMap("dropped_columns", UTF8Type.instance, LongType.instance));

        return cfm.params(decodeTableParams(tableRow))
                  .triggers(createTriggersFromTriggerRows(triggerRows));
    }

    /*
     * We call dense a CF for which each component of the comparator is a clustering column, i.e. no
     * component is used to store a regular column names. In other words, non-composite static "thrift"
     * and CQL3 CF are *not* dense.
     * We save whether the table is dense or not during table creation through CQL, but we don't have this
     * information for table just created through thrift, nor for table prior to CASSANDRA-7744, so this
     * method does its best to infer whether the table is dense or not based on other elements.
     */
    private static boolean calculateIsDense(AbstractType<?> comparator, UntypedResultSet columnRows, boolean isSuper)
    {
        /*
         * As said above, this method is only here because we need to deal with thrift upgrades.
         * Once a CF has been "upgraded", i.e. we've rebuilt and save its CQL3 metadata at least once,
         * then we'll have saved the "is_dense" value and will be good to go.
         *
         * But non-upgraded thrift CF (and pre-7744 CF) will have no value for "is_dense", so we need
         * to infer that information without relying on it in that case. And for the most part this is
         * easy, a CF that has at least one REGULAR definition is not dense. But the subtlety is that not
         * having a REGULAR definition may not mean dense because of CQL3 definitions that have only the
         * PRIMARY KEY defined.
         *
         * So we need to recognize those special case CQL3 table with only a primary key. If we have some
         * clustering columns, we're fine as said above. So the only problem is that we cannot decide for
         * sure if a CF without REGULAR columns nor CLUSTERING_COLUMN definition is meant to be dense, or if it
         * has been created in CQL3 by say:
         *    CREATE TABLE test (k int PRIMARY KEY)
         * in which case it should not be dense. However, we can limit our margin of error by assuming we are
         * in the latter case only if the comparator is exactly CompositeType(UTF8Type).
         */
        for (UntypedResultSet.Row columnRow : columnRows)
        {
            if ("regular".equals(columnRow.getString("type")))
                return false;
        }

        // If we've checked the columns for supercf and found no regulars, it's dense. Relying on the emptiness
        // of the value column is not enough due to index calculation.
        if (isSuper)
            return true;

        int maxClusteringIdx = -1;
        for (UntypedResultSet.Row columnRow : columnRows)
            if ("clustering_key".equals(columnRow.getString("type")))
                maxClusteringIdx = Math.max(maxClusteringIdx, columnRow.has("component_index") ? columnRow.getInt("component_index") : 0);

        return maxClusteringIdx >= 0
             ? maxClusteringIdx == comparator.componentsCount() - 1
             : !isCQL3OnlyPKComparator(comparator);
    }

    private static Iterable<UntypedResultSet.Row> filterOutRedundantRowsForSparse(UntypedResultSet columnRows, boolean isSuper, boolean isCompound)
    {
        Collection<UntypedResultSet.Row> filteredRows = new ArrayList<>();
        for (UntypedResultSet.Row columnRow : columnRows)
        {
            String kind = columnRow.getString("type");

            if (!isSuper && "compact_value".equals(kind))
                continue;

            if ("clustering_key".equals(kind) && !isSuper && !isCompound)
                continue;

            filteredRows.add(columnRow);
        }

        return filteredRows;
    }

    private static boolean isCQL3OnlyPKComparator(AbstractType<?> comparator)
    {
        if (!(comparator instanceof CompositeType))
            return false;

        CompositeType ct = (CompositeType)comparator;
        return ct.types.size() == 1 && ct.types.get(0) instanceof UTF8Type;
    }

    private static TableParams decodeTableParams(UntypedResultSet.Row row)
    {
        TableParams.Builder params = TableParams.builder();

        params.readRepairChance(row.getDouble("read_repair_chance"))
              .dcLocalReadRepairChance(row.getDouble("local_read_repair_chance"))
              .gcGraceSeconds(row.getInt("gc_grace_seconds"));

        if (row.has("comment"))
            params.comment(row.getString("comment"));

        if (row.has("memtable_flush_period_in_ms"))
            params.memtableFlushPeriodInMs(row.getInt("memtable_flush_period_in_ms"));

        params.caching(cachingFromRow(row.getString("caching")));

        if (row.has("default_time_to_live"))
            params.defaultTimeToLive(row.getInt("default_time_to_live"));

        if (row.has("speculative_retry"))
            params.speculativeRetry(SpeculativeRetryParam.fromString(row.getString("speculative_retry")));

        Map<String, String> compressionParameters = fromJsonMap(row.getString("compression_parameters"));
        String crcCheckChance = compressionParameters.remove("crc_check_chance");
        //crc_check_chance was promoted from a compression property to a top-level property
        if (crcCheckChance != null)
            params.crcCheckChance(Double.parseDouble(crcCheckChance));

        params.compression(CompressionParams.fromMap(compressionParameters));

        params.compaction(compactionFromRow(row));

        if (row.has("min_index_interval"))
            params.minIndexInterval(row.getInt("min_index_interval"));

        if (row.has("max_index_interval"))
            params.maxIndexInterval(row.getInt("max_index_interval"));

        if (row.has("bloom_filter_fp_chance"))
            params.bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"));

        return params.build();
    }


    /**
     *
      * 2.1 and newer use JSON'ified map of caching parameters, but older versions had valid Strings
      * NONE, KEYS_ONLY, ROWS_ONLY, and ALL
      *
      * @param caching, the string representing the table's caching options
      * @return CachingParams object corresponding to the input string
      */
    @VisibleForTesting
    public static CachingParams cachingFromRow(String caching)
    {
        switch(caching)
        {
            case "NONE":
                return CachingParams.CACHE_NOTHING;
            case "KEYS_ONLY":
                return CachingParams.CACHE_KEYS;
            case "ROWS_ONLY":
                return new CachingParams(false, Integer.MAX_VALUE);
            case "ALL":
                return CachingParams.CACHE_EVERYTHING;
            default:
                return CachingParams.fromMap(fromJsonMap(caching));
        }
    }

    /*
     * The method is needed - to migrate max_compaction_threshold and min_compaction_threshold
     * to the compaction map, where they belong.
     *
     * We must use reflection to validate the options because not every compaction strategy respects and supports
     * the threshold params (LCS doesn't, STCS and DTCS do).
     */
    @SuppressWarnings("unchecked")
    private static CompactionParams compactionFromRow(UntypedResultSet.Row row)
    {
        Class<? extends AbstractCompactionStrategy> klass =
            CFMetaData.createCompactionStrategy(row.getString("compaction_strategy_class"));
        Map<String, String> options = fromJsonMap(row.getString("compaction_strategy_options"));

        int minThreshold = row.getInt("min_compaction_threshold");
        int maxThreshold = row.getInt("max_compaction_threshold");

        Map<String, String> optionsWithThresholds = new HashMap<>(options);
        optionsWithThresholds.putIfAbsent(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(minThreshold));
        optionsWithThresholds.putIfAbsent(CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(maxThreshold));

        try
        {
            Map<String, String> unrecognizedOptions =
                (Map<String, String>) klass.getMethod("validateOptions", Map.class).invoke(null, optionsWithThresholds);

            if (unrecognizedOptions.isEmpty())
                options = optionsWithThresholds;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        return CompactionParams.create(klass, options);
    }

    // Should only be called on compact tables
    private static boolean checkNeedsUpgrade(Iterable<UntypedResultSet.Row> defs, boolean isSuper, boolean isStaticCompactTable)
    {
        // For SuperColumn tables, re-create a compact value column
        if (isSuper)
            return true;

        // For static compact tables, we need to upgrade if the regular definitions haven't been converted to static yet,
        // i.e. if we don't have a static definition yet.
        if (isStaticCompactTable)
            return !hasKind(defs, ColumnDefinition.Kind.STATIC);

        // For dense compact tables, we need to upgrade if we don't have a compact value definition
        return !hasRegularColumns(defs);
    }

    private static boolean hasRegularColumns(Iterable<UntypedResultSet.Row> columnRows)
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
            defs.add(ColumnDefinition.regularDef(ksName, cfName, SuperColumnCompatibility.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(subComparator, defaultValidator, true)));
        }
        else if (isStaticCompactTable)
        {
            defs.add(ColumnDefinition.clusteringDef(ksName, cfName, names.defaultClusteringName(), rawComparator, 0));
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), defaultValidator));
        }
        else
        {
            // For dense compact tables, we get here if we don't have a compact value column, in which case we should add it
            // (we use EmptyType to recognize that the compact value was not declared by the use (see CreateTableStatement too))
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), EmptyType.instance));
        }
    }

    private static boolean hasKind(Iterable<UntypedResultSet.Row> defs, ColumnDefinition.Kind kind)
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

    private static List<ColumnDefinition> createColumnsFromColumnRows(Iterable<UntypedResultSet.Row> rows,
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

            columns.add(createColumnFromColumnRow(row,
                                                  keyspace,
                                                  table,
                                                  rawComparator,
                                                  rawSubComparator,
                                                  isSuper,
                                                  isCQLTable,
                                                  isStaticCompactTable,
                                                  needsUpgrade));
        }

        return columns;
    }

    private static ColumnDefinition createColumnFromColumnRow(UntypedResultSet.Row row,
                                                              String keyspace,
                                                              String table,
                                                              AbstractType<?> rawComparator,
                                                              AbstractType<?> rawSubComparator,
                                                              boolean isSuper,
                                                              boolean isCQLTable,
                                                              boolean isStaticCompactTable,
                                                              boolean needsUpgrade)
    {
        String rawKind = row.getString("type");

        ColumnDefinition.Kind kind = deserializeKind(rawKind);
        if (needsUpgrade && isStaticCompactTable && kind == ColumnDefinition.Kind.REGULAR)
            kind = ColumnDefinition.Kind.STATIC;

        int componentIndex = ColumnDefinition.NO_POSITION;
        // Note that the component_index is not useful for non-primary key parts (it never really in fact since there is
        // no particular ordering of non-PK columns, we only used to use it as a simplification but that's not needed
        // anymore)
        if (kind.isPrimaryKeyKind())
            // We use to not have a component index when there was a single partition key, we don't anymore (#10491)
            componentIndex = row.has("component_index") ? row.getInt("component_index") : 0;

        // Note: we save the column name as string, but we should not assume that it is an UTF8 name, we
        // we need to use the comparator fromString method
        AbstractType<?> comparator = isCQLTable
                                     ? UTF8Type.instance
                                     : CompactTables.columnDefinitionComparator(rawKind, isSuper, rawComparator, rawSubComparator);
        ColumnIdentifier name = ColumnIdentifier.getInterned(comparator.fromString(row.getString("column_name")), comparator);

        AbstractType<?> validator = parseType(row.getString("validator"));

        // In the 2.x schema we didn't store UDT's with a FrozenType wrapper because they were implicitly frozen.  After
        // CASSANDRA-7423 (non-frozen UDTs), this is no longer true, so we need to freeze UDTs and nested freezable
        // types (UDTs and collections) to properly migrate the schema.  See CASSANDRA-11609 and CASSANDRA-11613.
        if (validator.isUDT() && validator.isMultiCell())
            validator = validator.freeze();
        else
            validator = validator.freezeNestedMulticellTypes();

        return new ColumnDefinition(keyspace, table, name, validator, componentIndex, kind);
    }

    private static Indexes createIndexesFromColumnRows(CFMetaData cfm,
                                                       Iterable<UntypedResultSet.Row> rows,
                                                       String keyspace,
                                                       String table,
                                                       AbstractType<?> rawComparator,
                                                       AbstractType<?> rawSubComparator,
                                                       boolean isSuper,
                                                       boolean isCQLTable,
                                                       boolean isStaticCompactTable,
                                                       boolean needsUpgrade)
    {
        Indexes.Builder indexes = Indexes.builder();

        for (UntypedResultSet.Row row : rows)
        {
            IndexMetadata.Kind kind = null;
            if (row.has("index_type"))
                kind = IndexMetadata.Kind.valueOf(row.getString("index_type"));

            if (kind == null)
                continue;

            Map<String, String> indexOptions = null;
            if (row.has("index_options"))
                indexOptions = fromJsonMap(row.getString("index_options"));

            if (row.has("index_name"))
            {
                String indexName = row.getString("index_name");

                ColumnDefinition column = createColumnFromColumnRow(row,
                                                                    keyspace,
                                                                    table,
                                                                    rawComparator,
                                                                    rawSubComparator,
                                                                    isSuper,
                                                                    isCQLTable,
                                                                    isStaticCompactTable,
                                                                    needsUpgrade);

                indexes.add(IndexMetadata.fromLegacyMetadata(cfm, column, indexName, kind, indexOptions));
            }
            else
            {
                logger.error("Failed to find index name for legacy migration of index on {}.{}", keyspace, table);
            }
        }

        return indexes.build();
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
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
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
        ColumnFamilyStore store = org.apache.cassandra.db.Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME)
                                                                  .getColumnFamilyStore(SystemKeyspace.LEGACY_USERTYPES);

        ClusteringComparator comparator = store.metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator, typeName));
        int nowInSec = FBUtilities.nowInSeconds();
        DecoratedKey key = store.metadata.decorateKey(AsciiType.instance.fromString(keyspaceName));
        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(store.metadata, nowInSec, key, slices);

        try (ReadExecutionController controller = command.executionController();
             RowIterator partition = UnfilteredRowIterators.filter(command.queryMemtableAndDisk(store, controller), nowInSec))
        {
            return partition.next().primaryKeyLivenessInfo().timestamp();
        }
    }

    private static UserType readTypeMetadata(String keyspaceName, String typeName)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND type_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_USERTYPES);
        UntypedResultSet.Row row = query(query, keyspaceName, typeName).one();

        List<FieldIdentifier> names =
            row.getList("field_names", UTF8Type.instance)
               .stream()
               .map(t -> FieldIdentifier.forInternalString(t))
               .collect(Collectors.toList());

        List<AbstractType<?>> types =
            row.getList("field_types", UTF8Type.instance)
               .stream()
               .map(LegacySchemaMigrator::parseType)
               .collect(Collectors.toList());

        return new UserType(keyspaceName, bytes(typeName), names, types, true);
    }

    /*
     * Reading UDFs
     */

    private static Collection<Function> readFunctions(String keyspaceName)
    {
        String query = format("SELECT function_name, signature FROM %s.%s WHERE keyspace_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_FUNCTIONS);
        HashMultimap<String, List<String>> functionSignatures = HashMultimap.create();
        query(query, keyspaceName).forEach(row -> functionSignatures.put(row.getString("function_name"), row.getList("signature", UTF8Type.instance)));

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
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_FUNCTIONS);
        return query(query, keyspaceName, functionName, signature).one().getLong("timestamp");
    }

    private static UDFunction readFunctionMetadata(String keyspaceName, String functionName, List<String> signature)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND function_name = ? AND signature = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
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

    private static Collection<Aggregate> readAggregates(Functions functions, String keyspaceName)
    {
        String query = format("SELECT aggregate_name, signature FROM %s.%s WHERE keyspace_name = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_AGGREGATES);
        HashMultimap<String, List<String>> aggregateSignatures = HashMultimap.create();
        query(query, keyspaceName).forEach(row -> aggregateSignatures.put(row.getString("aggregate_name"), row.getList("signature", UTF8Type.instance)));

        Collection<Aggregate> aggregates = new ArrayList<>();
        aggregateSignatures.entries().forEach(pair -> aggregates.add(readAggregate(functions, keyspaceName, pair.getKey(), pair.getValue())));
        return aggregates;
    }

    private static Aggregate readAggregate(Functions functions, String keyspaceName, String aggregateName, List<String> signature)
    {
        long timestamp = readAggregateTimestamp(keyspaceName, aggregateName, signature);
        UDAggregate metadata = readAggregateMetadata(functions, keyspaceName, aggregateName, signature);
        return new Aggregate(timestamp, metadata);
    }

    private static long readAggregateTimestamp(String keyspaceName, String aggregateName, List<String> signature)
    {
        String query = format("SELECT writeTime(return_type) AS timestamp " +
                              "FROM %s.%s " +
                              "WHERE keyspace_name = ? AND aggregate_name = ? AND signature = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
                              SystemKeyspace.LEGACY_AGGREGATES);
        return query(query, keyspaceName, aggregateName, signature).one().getLong("timestamp");
    }

    private static UDAggregate readAggregateMetadata(Functions functions, String keyspaceName, String functionName, List<String> signature)
    {
        String query = format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND aggregate_name = ? AND signature = ?",
                              SchemaConstants.SYSTEM_KEYSPACE_NAME,
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
        AbstractType<?> stateType = parseType(row.getString("state_type"));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(keyspaceName, row.getString("final_func")) : null;
        ByteBuffer initcond = row.has("initcond") ? row.getBytes("initcond") : null;

        try
        {
            return UDAggregate.create(functions, name, argTypes, returnType, stateFunc, finalFunc, stateType, initcond);
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
