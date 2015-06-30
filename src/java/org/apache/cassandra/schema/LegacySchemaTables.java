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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.functions.AbstractFunction;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;
import static org.apache.cassandra.utils.FBUtilities.json;

/** system.schema_* tables used to store keyspace/table/type attributes prior to C* 3.0 */
public final class LegacySchemaTables
{
    private LegacySchemaTables()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(LegacySchemaTables.class);

    public static final String KEYSPACES = "schema_keyspaces";
    public static final String COLUMNFAMILIES = "schema_columnfamilies";
    public static final String COLUMNS = "schema_columns";
    public static final String TRIGGERS = "schema_triggers";
    public static final String USERTYPES = "schema_usertypes";
    public static final String FUNCTIONS = "schema_functions";
    public static final String AGGREGATES = "schema_aggregates";

    public static final List<String> ALL = Arrays.asList(KEYSPACES, COLUMNFAMILIES, COLUMNS, TRIGGERS, USERTYPES, FUNCTIONS, AGGREGATES);

    private static final CFMetaData Keyspaces =
        compile(KEYSPACES,
                "keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "strategy_class text,"
                + "strategy_options text,"
                + "PRIMARY KEY ((keyspace_name))) "
                + "WITH COMPACT STORAGE");

    private static final CFMetaData Columnfamilies =
        compile(COLUMNFAMILIES,
                "table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching text,"
                + "cf_id uuid," // post-2.1 UUID cfid
                + "comment text,"
                + "compaction_strategy_class text,"
                + "compaction_strategy_options text,"
                + "comparator text,"
                + "compression_parameters text,"
                + "default_time_to_live int,"
                + "default_validator text,"
                + "dropped_columns map<text, bigint>,"
                + "dropped_columns_types map<text, text>,"
                + "gc_grace_seconds int,"
                + "is_dense boolean,"
                + "key_validator text,"
                + "local_read_repair_chance double,"
                + "max_compaction_threshold int,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_compaction_threshold int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "subcomparator text,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name))");

    private static final CFMetaData Columns =
        compile(COLUMNS,
                "column definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "column_name text,"
                + "component_index int,"
                + "index_name text,"
                + "index_options text,"
                + "index_type text,"
                + "type text,"
                + "validator text,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, column_name))");

    private static final CFMetaData Triggers =
        compile(TRIGGERS,
                "trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "columnfamily_name text,"
                + "trigger_name text,"
                + "trigger_options map<text, text>,"
                + "PRIMARY KEY ((keyspace_name), columnfamily_name, trigger_name))");

    private static final CFMetaData Usertypes =
        compile(USERTYPES,
                "user defined type definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "type_name text,"
                + "field_names list<text>,"
                + "field_types list<text>,"
                + "PRIMARY KEY ((keyspace_name), type_name))");

    private static final CFMetaData Functions =
        compile(FUNCTIONS,
                "user defined function definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "function_name text,"
                + "signature frozen<list<text>>,"
                + "argument_names list<text>,"
                + "argument_types list<text>,"
                + "body text,"
                + "language text,"
                + "return_type text,"
                + "called_on_null_input boolean,"
                + "PRIMARY KEY ((keyspace_name), function_name, signature))");

    private static final CFMetaData Aggregates =
        compile(AGGREGATES,
                "user defined aggregate definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "aggregate_name text,"
                + "signature frozen<list<text>>,"
                + "argument_types list<text>,"
                + "final_func text,"
                + "initcond blob,"
                + "return_type text,"
                + "state_func text,"
                + "state_type text,"
                + "PRIMARY KEY ((keyspace_name), aggregate_name, signature))");

    public static final List<CFMetaData> All = Arrays.asList(Keyspaces, Columnfamilies, Columns, Triggers, Usertypes, Functions, Aggregates);

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SystemKeyspace.NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
    }

    /** add entries to system.schema_* for the hardcoded system definitions */
    public static void saveSystemKeyspaceSchema()
    {
        KSMetaData keyspace = Schema.instance.getKSMetaData(SystemKeyspace.NAME);
        long timestamp = FBUtilities.timestampMicros();
        // delete old, possibly obsolete entries in schema tables
        for (String table : ALL)
        {
            executeOnceInternal(String.format("DELETE FROM system.%s USING TIMESTAMP ? WHERE keyspace_name = ?", table),
                                timestamp,
                                keyspace.name);
        }
        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        makeCreateKeyspaceMutation(keyspace, timestamp + 1).apply();
    }

    public static Collection<KSMetaData> readSchemaFromSystemTables()
    {
        ReadCommand cmd = getReadCommandForTableSchema(KEYSPACES);
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); PartitionIterator schema = cmd.executeInternal(orderGroup))
        {
            List<KSMetaData> keyspaces = new ArrayList<>();

            while (schema.hasNext())
            {
                try (RowIterator partition = schema.next())
                {
                    if (isSystemKeyspaceSchemaPartition(partition.partitionKey()))
                        continue;

                    DecoratedKey key = partition.partitionKey();

                    readSchemaPartitionForKeyspaceAndApply(USERTYPES, key,
                        types -> readSchemaPartitionForKeyspaceAndApply(COLUMNFAMILIES, key,
                        tables -> readSchemaPartitionForKeyspaceAndApply(FUNCTIONS, key,
                        functions -> readSchemaPartitionForKeyspaceAndApply(AGGREGATES, key,
                        aggregates -> keyspaces.add(createKeyspaceFromSchemaPartitions(partition, tables, types, functions, aggregates)))))
                    );
                }
            }
            return keyspaces;
        }
    }

    public static void truncateSchemaTables()
    {
        for (String table : ALL)
            getSchemaCFS(table).truncateBlocking();
    }

    private static void flushSchemaTables()
    {
        for (String table : ALL)
            SystemKeyspace.forceBlockingFlush(table);
    }

    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public static UUID calculateSchemaDigest()
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

        for (String table : ALL)
        {
            ReadCommand cmd = getReadCommandForTableSchema(table);
            try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); PartitionIterator schema = cmd.executeInternal(orderGroup))
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
        return Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(schemaTableName);
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
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); UnfilteredPartitionIterator iter = cmd.executeLocally(orderGroup))
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
                        mutation = new Mutation(SystemKeyspace.NAME, key);
                        mutationMap.put(key, mutation);
                    }

                    mutation.add(UnfilteredRowIterators.toUpdate(partition));
                }
            }
        }
    }

    private static Map<DecoratedKey, FilteredPartition> readSchemaForKeyspaces(String schemaTableName, Set<String> keyspaceNames)
    {
        Map<DecoratedKey, FilteredPartition> schema = new HashMap<>();

        for (String keyspaceName : keyspaceNames)
        {
            // We don't to return the RowIterator directly because we should guarantee that this iterator
            // will be closed, and putting it in a Map make that harder/more awkward.
            readSchemaPartitionForKeyspaceAndApply(schemaTableName, keyspaceName,
                partition -> {
                    if (!partition.isEmpty())
                        schema.put(partition.partitionKey(), FilteredPartition.create(partition));
                    return null;
                }
            );
        }

        return schema;
    }

    private static ByteBuffer getSchemaKSKey(String ksName)
    {
        return AsciiType.instance.fromString(ksName);
    }

    private static DecoratedKey getSchemaKSDecoratedKey(String ksName)
    {
        return StorageService.getPartitioner().decorateKey(getSchemaKSKey(ksName));
    }

    private static <T> T readSchemaPartitionForKeyspaceAndApply(String schemaTableName, String keyspaceName, Function<RowIterator, T> fct)
    {
        return readSchemaPartitionForKeyspaceAndApply(schemaTableName, getSchemaKSDecoratedKey(keyspaceName), fct);
    }

    private static <T> T readSchemaPartitionForKeyspaceAndApply(String schemaTableName, DecoratedKey keyspaceKey, Function<RowIterator, T> fct)
    {
        ColumnFamilyStore store = getSchemaCFS(schemaTableName);
        int nowInSec = FBUtilities.nowInSeconds();
        try (OpOrder.Group op = store.readOrdering.start();
             RowIterator partition = UnfilteredRowIterators.filter(SinglePartitionReadCommand.fullPartitionRead(store.metadata, nowInSec, keyspaceKey)
                                                                                             .queryMemtableAndDisk(store, op), nowInSec))
        {
            return fct.apply(partition);
        }
    }

    private static <T> T readSchemaPartitionForTableAndApply(String schemaTableName, String keyspaceName, String tableName, Function<RowIterator, T> fct)
    {
        ColumnFamilyStore store = getSchemaCFS(schemaTableName);

        ClusteringComparator comparator = store.metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator, tableName));
        int nowInSec = FBUtilities.nowInSeconds();
        try (OpOrder.Group op = store.readOrdering.start();
             RowIterator partition =  UnfilteredRowIterators.filter(SinglePartitionSliceCommand.create(store.metadata, nowInSec, getSchemaKSDecoratedKey(keyspaceName), slices)
                                                                                               .queryMemtableAndDisk(store, op), nowInSec))
        {
            return fct.apply(partition);
        }
    }

    private static boolean isSystemKeyspaceSchemaPartition(DecoratedKey partitionKey)
    {
        return getSchemaKSKey(SystemKeyspace.NAME).equals(partitionKey.getKey());
    }

    /**
     * Merge remote schema in form of mutations with local and mutate ks/cf metadata objects
     * (which also involves fs operations on add/drop ks/cf)
     *
     * @param mutations the schema changes to apply
     *
     * @throws ConfigurationException If one of metadata attributes has invalid value
     * @throws IOException If data was corrupted during transportation or failed to apply fs operations
     */
    public static synchronized void mergeSchema(Collection<Mutation> mutations) throws ConfigurationException, IOException
    {
        mergeSchema(mutations, true);
        Schema.instance.updateVersionAndAnnounce();
    }

    public static synchronized void mergeSchema(Collection<Mutation> mutations, boolean doFlush) throws IOException
    {
        // compare before/after schemas of the affected keyspaces only
        Set<String> keyspaces = new HashSet<>(mutations.size());
        for (Mutation mutation : mutations)
            keyspaces.add(ByteBufferUtil.string(mutation.key().getKey()));

        // current state of the schema
        Map<DecoratedKey, FilteredPartition> oldKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        for (Mutation mutation : mutations)
            mutation.apply();

        if (doFlush)
            flushSchemaTables();

        // with new data applied
        Map<DecoratedKey, FilteredPartition> newKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newColumnFamilies = readSchemaForKeyspaces(COLUMNFAMILIES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newTypes = readSchemaForKeyspaces(USERTYPES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, FilteredPartition> newAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeTables(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);
        mergeAggregates(oldAggregates, newAggregates);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        for (String keyspaceToDrop : keyspacesToDrop)
            Schema.instance.dropKeyspace(keyspaceToDrop);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        for (FilteredPartition newPartition : after.values())
        {
            FilteredPartition oldPartition = before.remove(newPartition.partitionKey());
            if (oldPartition == null || oldPartition.isEmpty())
            {
                Schema.instance.addKeyspace(createKeyspaceFromSchemaPartition(newPartition.rowIterator()));
            }
            else
            {
                String name = AsciiType.instance.compose(newPartition.partitionKey().getKey());
                Schema.instance.updateKeyspace(name);
            }
        }

        // What's remain in old is those keyspace that are not in updated, i.e. the dropped ones.
        return asKeyspaceNamesSet(before.keySet());
    }

    private static Set<String> asKeyspaceNamesSet(Set<DecoratedKey> keys)
    {
        Set<String> names = new HashSet(keys.size());
        for (DecoratedKey key : keys)
            names.add(AsciiType.instance.compose(key.getKey()));
        return names;
    }

    private static void mergeTables(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropTable(oldRow.getString("keyspace_name"), oldRow.getString("columnfamily_name"));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addTable(createTableFromTableRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateTable(newRow.getString("keyspace_name"), newRow.getString("columnfamily_name"));
            }
        });
    }

    private static void mergeTypes(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropType(createTypeFromRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addType(createTypeFromRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateType(createTypeFromRow(newRow));
            }
        });
    }

    private static void mergeFunctions(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropFunction(createFunctionFromFunctionRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addFunction(createFunctionFromFunctionRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateFunction(createFunctionFromFunctionRow(newRow));
            }
        });
    }

    private static void mergeAggregates(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        diffSchema(before, after, new Differ()
        {
            public void onDropped(UntypedResultSet.Row oldRow)
            {
                Schema.instance.dropAggregate(createAggregateFromAggregateRow(oldRow));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addAggregate(createAggregateFromAggregateRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateAggregate(createAggregateFromAggregateRow(newRow));
            }
        });
    }

    public interface Differ
    {
        public void onDropped(UntypedResultSet.Row oldRow);
        public void onAdded(UntypedResultSet.Row newRow);
        public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow);
    }

    private static void diffSchema(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after, Differ differ)
    {
        for (FilteredPartition newPartition : after.values())
        {
            CFMetaData metadata = newPartition.metadata();
            DecoratedKey key = newPartition.partitionKey();

            FilteredPartition oldPartition = before.remove(key);

            if (oldPartition == null || oldPartition.isEmpty())
            {
                // Means everything is to be added
                for (Row row : newPartition)
                    differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, row));
                continue;
            }

            Iterator<Row> oldIter = oldPartition.iterator();
            Iterator<Row> newIter = newPartition.iterator();

            Row oldRow = oldIter.hasNext() ? oldIter.next() : null;
            Row newRow = newIter.hasNext() ? newIter.next() : null;
            while (oldRow != null && newRow != null)
            {
                int cmp = metadata.comparator.compare(oldRow.clustering(), newRow.clustering());
                if (cmp < 0)
                {
                    differ.onDropped(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow));
                    oldRow = oldIter.hasNext() ? oldIter.next() : null;
                }
                else if (cmp > 0)
                {

                    differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));
                    newRow = newIter.hasNext() ? newIter.next() : null;
                }
                else
                {
                    if (!oldRow.equals(newRow))
                        differ.onUpdated(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow), UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));

                    oldRow = oldIter.hasNext() ? oldIter.next() : null;
                    newRow = newIter.hasNext() ? newIter.next() : null;
                }
            }

            while (oldRow != null)
            {
                differ.onDropped(UntypedResultSet.Row.fromInternalRow(metadata, key, oldRow));
                oldRow = oldIter.hasNext() ? oldIter.next() : null;
            }
            while (newRow != null)
            {
                differ.onAdded(UntypedResultSet.Row.fromInternalRow(metadata, key, newRow));
                newRow = newIter.hasNext() ? newIter.next() : null;
            }
        }

        // What remains is those keys that were only in before.
        for (FilteredPartition partition : before.values())
            for (Row row : partition)
                differ.onDropped(UntypedResultSet.Row.fromInternalRow(partition.metadata(), partition.partitionKey(), row));
    }

    /*
     * Keyspace metadata serialization/deserialization.
     */

    public static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        return makeCreateKeyspaceMutation(keyspace, timestamp, true);
    }

    public static Mutation makeCreateKeyspaceMutation(KSMetaData keyspace, long timestamp, boolean withTablesAndTypesAndFunctions)
    {
        // Note that because Keyspaces is a COMPACT TABLE, we're really only setting static columns internally and shouldn't set any clustering.
        RowUpdateBuilder adder = new RowUpdateBuilder(Keyspaces, timestamp, keyspace.name);

        adder.add("durable_writes", keyspace.durableWrites);
        adder.add("strategy_class", keyspace.strategyClass.getName());
        adder.add("strategy_options", json(keyspace.strategyOptions));

        Mutation mutation = adder.build();

        if (withTablesAndTypesAndFunctions)
        {
            keyspace.userTypes.getAllTypes().values().forEach(type -> addTypeToSchemaMutation(type, timestamp, mutation));
            keyspace.cfMetaData().values().forEach(table -> addTableToSchemaMutation(table, timestamp, true, mutation));
            keyspace.functions.udfs().forEach(udf -> addFunctionToSchemaMutation(udf, timestamp, mutation));
            keyspace.functions.udas().forEach(uda -> addAggregateToSchemaMutation(uda, timestamp, mutation));
        }

        return mutation;
    }

    public static Mutation makeDropKeyspaceMutation(KSMetaData keyspace, long timestamp)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        Mutation mutation = new Mutation(SystemKeyspace.NAME, getSchemaKSDecoratedKey(keyspace.name));
        for (CFMetaData schemaTable : All)
            mutation.add(PartitionUpdate.fullPartitionDelete(schemaTable, mutation.key(), timestamp, nowInSec));
        mutation.add(PartitionUpdate.fullPartitionDelete(SystemKeyspace.BuiltIndexes, mutation.key(), timestamp, nowInSec));
        return mutation;
    }

    private static KSMetaData createKeyspaceFromSchemaPartitions(RowIterator serializedKeyspace,
                                                                 RowIterator serializedTables,
                                                                 RowIterator serializedTypes,
                                                                 RowIterator serializedFunctions,
                                                                 RowIterator seriazliedAggregates)
    {
        Collection<CFMetaData> tables = createTablesFromTablesPartition(serializedTables);
        UTMetaData types = new UTMetaData(createTypesFromPartition(serializedTypes));

        Collection<UDFunction> udfs = createFunctionsFromFunctionsPartition(serializedFunctions);
        Collection<UDAggregate> udas = createAggregatesFromAggregatesPartition(seriazliedAggregates);
        Functions functions = org.apache.cassandra.schema.Functions.builder().add(udfs).add(udas).build();

        return createKeyspaceFromSchemaPartition(serializedKeyspace).cloneWith(tables, types, functions);
    }

    public static KSMetaData createKeyspaceFromName(String keyspace)
    {
        return readSchemaPartitionForKeyspaceAndApply(KEYSPACES, keyspace, partition ->
        {
            if (partition.isEmpty())
                throw new RuntimeException(String.format("%s not found in the schema definitions keyspaceName (%s).", keyspace, KEYSPACES));

            return createKeyspaceFromSchemaPartition(partition);
        });
    }


    /**
     * Deserialize only Keyspace attributes without nested tables or types
     *
     * @param partition Keyspace attributes in serialized form
     */
    private static KSMetaData createKeyspaceFromSchemaPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, KEYSPACES);
        UntypedResultSet.Row row = QueryProcessor.resultify(query, partition).one();
        return new KSMetaData(row.getString("keyspace_name"),
                              AbstractReplicationStrategy.getClass(row.getString("strategy_class")),
                              fromJsonMap(row.getString("strategy_options")),
                              row.getBoolean("durable_writes"));
    }

    /*
     * User type metadata serialization/deserialization.
     */

    public static Mutation makeCreateTypeMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTypeToSchemaMutation(type, timestamp, mutation);
        return mutation;
    }

    private static void addTypeToSchemaMutation(UserType type, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Usertypes, timestamp, mutation)
                                 .clustering(type.name);

        adder.resetCollection("field_names");
        adder.resetCollection("field_types");

        for (int i = 0; i < type.size(); i++)
        {
            adder.addListEntry("field_names", type.fieldName(i));
            adder.addListEntry("field_types", type.fieldType(i).toString());
        }

        adder.build();
    }

    public static Mutation dropTypeFromSchemaMutation(KSMetaData keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Usertypes, timestamp, mutation, type.name);
    }

    private static Map<ByteBuffer, UserType> createTypesFromPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, USERTYPES);
        Map<ByteBuffer, UserType> types = new HashMap<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            UserType type = createTypeFromRow(row);
            types.put(type.name, type);
        }
        return types;
    }

    private static UserType createTypeFromRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        ByteBuffer name = ByteBufferUtil.bytes(row.getString("type_name"));
        List<String> rawColumns = row.getList("field_names", UTF8Type.instance);
        List<String> rawTypes = row.getList("field_types", UTF8Type.instance);

        List<ByteBuffer> columns = new ArrayList<>(rawColumns.size());
        for (String rawColumn : rawColumns)
            columns.add(ByteBufferUtil.bytes(rawColumn));

        List<AbstractType<?>> types = new ArrayList<>(rawTypes.size());
        for (String rawType : rawTypes)
            types.add(parseType(rawType));

        return new UserType(keyspace, name, columns, types);
    }

    /*
     * Table metadata serialization/deserialization.
     */

    public static Mutation makeCreateTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addTableToSchemaMutation(table, timestamp, true, mutation);
        return mutation;
    }

    private static void addTableToSchemaMutation(CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        RowUpdateBuilder adder = new RowUpdateBuilder(Columnfamilies, timestamp, mutation)
                                 .clustering(table.cfName);

        adder.add("cf_id", table.cfId);
        adder.add("type", table.isSuper() ? "Super" : "Standard");

        if (table.isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", table.comparator.subtype(0).toString());
            adder.add("subcomparator", ((MapType)table.compactValueColumn().type).getKeysType().toString());
        }
        else
        {
            adder.add("comparator", LegacyLayout.makeLegacyComparator(table).toString());
        }

        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance());
        adder.add("caching", table.getCaching().toString());
        adder.add("comment", table.getComment());
        adder.add("compaction_strategy_class", table.compactionStrategyClass.getName());
        adder.add("compaction_strategy_options", json(table.compactionStrategyOptions));
        adder.add("compression_parameters", json(table.compressionParameters.asThriftOptions()));
        adder.add("default_time_to_live", table.getDefaultTimeToLive());
        adder.add("gc_grace_seconds", table.getGcGraceSeconds());
        adder.add("key_validator", table.getKeyValidator().toString());
        adder.add("local_read_repair_chance", table.getDcLocalReadRepairChance());
        adder.add("max_compaction_threshold", table.getMaxCompactionThreshold());
        adder.add("max_index_interval", table.getMaxIndexInterval());
        adder.add("memtable_flush_period_in_ms", table.getMemtableFlushPeriod());
        adder.add("min_compaction_threshold", table.getMinCompactionThreshold());
        adder.add("min_index_interval", table.getMinIndexInterval());
        adder.add("read_repair_chance", table.getReadRepairChance());
        adder.add("speculative_retry", table.getSpeculativeRetry().toString());

        for (Map.Entry<ColumnIdentifier, CFMetaData.DroppedColumn> entry : table.getDroppedColumns().entrySet())
        {
            String name = entry.getKey().toString();
            CFMetaData.DroppedColumn column = entry.getValue();
            adder.addMapEntry("dropped_columns", name, column.droppedTime);
            if (column.type != null)
                adder.addMapEntry("dropped_columns_types", name, column.type.toString());
        }

        adder.add("is_dense", table.isDense());

        adder.add("default_validator", table.makeLegacyDefaultValidator().toString());

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, timestamp, mutation);

            for (TriggerDefinition trigger : table.getTriggers().values())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);
        }

        adder.build();
    }

    public static Mutation makeUpdateTableMutation(KSMetaData keyspace,
                                                   CFMetaData oldTable,
                                                   CFMetaData newTable,
                                                   long timestamp,
                                                   boolean fromThrift)
    {
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        addTableToSchemaMutation(newTable, timestamp, false, mutation);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldTable.getColumnMetadata(),
                                                                                 newTable.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (fromThrift && column.kind != ColumnDefinition.Kind.REGULAR)
                continue;

            dropColumnFromSchemaMutation(oldTable, column, timestamp, mutation);
        }

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, timestamp, mutation);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumnDefinition(name), timestamp, mutation);

        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, timestamp, mutation);

        // newly created triggers
        for (TriggerDefinition trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, timestamp, mutation);

        return mutation;
    }

    public static Mutation makeDropTableMutation(KSMetaData keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);

        RowUpdateBuilder.deleteRow(Columnfamilies, timestamp, mutation, table.cfName);

        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, timestamp, mutation);

        for (TriggerDefinition trigger : table.getTriggers().values())
            dropTriggerFromSchemaMutation(table, trigger, timestamp, mutation);

        // TODO: get rid of in #6717
        for (String indexName : Keyspace.open(keyspace.name).getColumnFamilyStore(table.cfName).getBuiltIndexes())
            RowUpdateBuilder.deleteRow(SystemKeyspace.BuiltIndexes, timestamp, mutation, indexName);

        return mutation;
    }

    public static CFMetaData createTableFromName(String keyspace, String table)
    {
        return readSchemaPartitionForTableAndApply(COLUMNFAMILIES, keyspace, table, partition ->
        {
            if (partition.isEmpty())
                throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspace, table));

            return createTableFromTablePartition(partition);
        });
    }

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the table and its metadata for faster lookup
     */
    private static Collection<CFMetaData> createTablesFromTablesPartition(RowIterator partition)
    {
        if (partition.isEmpty())
            return Collections.emptyList();

        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        List<CFMetaData> tables = new ArrayList<>();
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            tables.add(createTableFromTableRow(row));
        return tables;
    }

    public static CFMetaData createTableFromTablePartitionAndColumnsPartition(RowIterator serializedTable, RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRowAndColumnsPartition(QueryProcessor.resultify(query, serializedTable).one(), serializedColumns);
    }

    private static CFMetaData createTableFromTableRowAndColumnsPartition(UntypedResultSet.Row tableRow, RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNS);
        return createTableFromTableRowAndColumnRows(tableRow, QueryProcessor.resultify(query, serializedColumns));
    }

    private static CFMetaData createTableFromTablePartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, COLUMNFAMILIES);
        return createTableFromTableRow(QueryProcessor.resultify(query, partition).one());
    }

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    private static CFMetaData createTableFromTableRow(UntypedResultSet.Row result)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        CFMetaData cfm = readSchemaPartitionForTableAndApply(COLUMNS, ksName, cfName, partition -> createTableFromTableRowAndColumnsPartition(result, partition));

        readSchemaPartitionForTableAndApply(TRIGGERS, ksName, cfName,
            partition -> { createTriggersFromTriggersPartition(partition).forEach(trigger -> cfm.addTriggerDefinition(trigger)); return null; }
        );

        return cfm;
    }

    public static CFMetaData createTableFromTableRowAndColumnRows(UntypedResultSet.Row result,
                                                                  UntypedResultSet serializedColumnDefinitions)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
        AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;

        boolean isSuper = result.getString("type").toLowerCase().equals("super");
        boolean isDense = result.getBoolean("is_dense");
        boolean isCompound = rawComparator instanceof CompositeType;

        // We don't really use the default validator but as we have it for backward compatibility, we use it to know if it's a counter table
        AbstractType<?> defaultValidator = TypeParser.parse(result.getString("default_validator"));
        boolean isCounter =  defaultValidator instanceof CounterColumnType;

        // if we are upgrading, we use id generated from names initially
        UUID cfId = result.has("cf_id")
                  ? result.getUUID("cf_id")
                  : CFMetaData.generateLegacyCfId(ksName, cfName);

        boolean isCQLTable = !isSuper && !isDense && isCompound;
        boolean isStaticCompactTable = !isDense && !isCompound;

        // Internally, compact tables have a specific layout, see CompactTables. But when upgrading from
        // previous versions, they may not have the expected schema, so detect if we need to upgrade and do
        // it in createColumnsFromColumnRows.
        // We can remove this once we don't support upgrade from versions < 3.0.
        boolean needsUpgrade = isCQLTable ? false : checkNeedsUpgrade(serializedColumnDefinitions, isSuper, isStaticCompactTable);

        List<ColumnDefinition> columnDefs = createColumnsFromColumnRows(serializedColumnDefinitions,
                                                                        ksName,
                                                                        cfName,
                                                                        rawComparator,
                                                                        subComparator,
                                                                        isSuper,
                                                                        isCQLTable,
                                                                        isStaticCompactTable,
                                                                        needsUpgrade);

        if (needsUpgrade)
            addDefinitionForUpgrade(columnDefs, ksName, cfName, isStaticCompactTable, isSuper, rawComparator, subComparator, defaultValidator);

        CFMetaData cfm = CFMetaData.create(ksName, cfName, cfId, isDense, isCompound, isSuper, isCounter, columnDefs);

        cfm.readRepairChance(result.getDouble("read_repair_chance"));
        cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
        cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
        cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
        cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
        if (result.has("comment"))
            cfm.comment(result.getString("comment"));
        if (result.has("memtable_flush_period_in_ms"))
            cfm.memtableFlushPeriod(result.getInt("memtable_flush_period_in_ms"));
        cfm.caching(CachingOptions.fromString(result.getString("caching")));
        if (result.has("default_time_to_live"))
            cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
        if (result.has("speculative_retry"))
            cfm.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(result.getString("speculative_retry")));
        cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(result.getString("compaction_strategy_class")));
        cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
        cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));

        if (result.has("min_index_interval"))
            cfm.minIndexInterval(result.getInt("min_index_interval"));

        if (result.has("max_index_interval"))
            cfm.maxIndexInterval(result.getInt("max_index_interval"));

        if (result.has("bloom_filter_fp_chance"))
            cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
        else
            cfm.bloomFilterFpChance(cfm.getBloomFilterFpChance());

        if (result.has("dropped_columns"))
        {
            Map<String, String> types = result.has("dropped_columns_types")
                                      ? result.getMap("dropped_columns_types", UTF8Type.instance, UTF8Type.instance) 
                                      : Collections.<String, String>emptyMap();
            addDroppedColumns(cfm, result.getMap("dropped_columns", UTF8Type.instance, LongType.instance), types);
        }

        return cfm;
    }

    // Should only be called on compact tables
    private static boolean checkNeedsUpgrade(UntypedResultSet defs, boolean isSuper, boolean isStaticCompactTable)
    {
        if (isSuper)
        {
            // Check if we've added the "supercolumn map" column yet or not
            for (UntypedResultSet.Row row : defs)
            {
                if (row.getString("column_name").isEmpty())
                    return false;
            }
            return true;
        }

        // For static compact tables, we need to upgrade if the regular definitions haven't been converted to static yet,
        // i.e. if we don't have a static definition yet.
        if (isStaticCompactTable)
            return !hasKind(defs, ColumnDefinition.Kind.STATIC);

        // For dense compact tables, we need to upgrade if we don't have a compact value definition
        return !hasKind(defs, ColumnDefinition.Kind.REGULAR);
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
            defs.add(ColumnDefinition.regularDef(ksName, cfName, CompactTables.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(subComparator, defaultValidator, true), null));
        }
        else if (isStaticCompactTable)
        {
            defs.add(ColumnDefinition.clusteringKeyDef(ksName, cfName, names.defaultClusteringName(), rawComparator, null));
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), defaultValidator, null));
        }
        else
        {
            // For dense compact tables, we get here if we don't have a compact value column, in which case we should add it
            // (we use EmptyType to recognize that the compact value was not declared by the use (see CreateTableStatement too))
            defs.add(ColumnDefinition.regularDef(ksName, cfName, names.defaultCompactValueName(), EmptyType.instance, null));
        }
    }

    private static boolean hasKind(UntypedResultSet defs, ColumnDefinition.Kind kind)
    {
        for (UntypedResultSet.Row row : defs)
        {
            if (deserializeKind(row.getString("type")) == kind)
                return true;
        }
        return false;
    }

    private static void addDroppedColumns(CFMetaData cfm, Map<String, Long> droppedTimes, Map<String, String> types)
    {
        for (Map.Entry<String, Long> entry : droppedTimes.entrySet())
        {
            String name = entry.getKey();
            long time = entry.getValue();
            AbstractType<?> type = types.containsKey(name) ? TypeParser.parse(types.get(name)) : null;
            cfm.getDroppedColumns().put(ColumnIdentifier.getInterned(name, true), new CFMetaData.DroppedColumn(type, time));
        }
    }

    /*
     * Column metadata serialization/deserialization.
     */

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Columns, timestamp, mutation)
                                 .clustering(table.cfName, column.name.toString());

        adder.add("validator", column.type.toString());
        adder.add("type", serializeKind(column.kind, table.isDense()));
        adder.add("component_index", column.isOnAllComponents() ? null : column.position());
        adder.add("index_name", column.getIndexName());
        adder.add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString());
        adder.add("index_options", json(column.getIndexOptions()));

        adder.build();
    }

    private static String serializeKind(ColumnDefinition.Kind kind, boolean isDense)
    {
        // For backward compatibility, we special case CLUSTERING_COLUMN and the case where the table is dense.
        if (kind == ColumnDefinition.Kind.CLUSTERING_COLUMN)
            return "clustering_key";

        if (kind == ColumnDefinition.Kind.REGULAR && isDense)
            return "compact_value";

        return kind.toString().toLowerCase();
    }

    public static ColumnDefinition.Kind deserializeKind(String kind)
    {
        if (kind.equalsIgnoreCase("clustering_key"))
            return ColumnDefinition.Kind.CLUSTERING_COLUMN;
        if (kind.equalsIgnoreCase("compact_value"))
            return ColumnDefinition.Kind.REGULAR;
        return Enum.valueOf(ColumnDefinition.Kind.class, kind.toUpperCase());
    }

    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        RowUpdateBuilder.deleteRow(Columns, timestamp, mutation, table.cfName, column.name.toString());
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
            columns.add(createColumnFromColumnRow(row, keyspace, table, rawComparator, rawSubComparator, isSuper, isCQLTable, isStaticCompactTable, needsUpgrade));
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
        ColumnDefinition.Kind kind = deserializeKind(row.getString("type"));
        if (needsUpgrade && isStaticCompactTable && kind == ColumnDefinition.Kind.REGULAR)
            kind = ColumnDefinition.Kind.STATIC;

        Integer componentIndex = null;
        if (row.has("component_index"))
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

        return new ColumnDefinition(keyspace, table, name, validator, indexType, indexOptions, indexName, componentIndex, kind);
    }

    /*
     * Trigger metadata serialization/deserialization.
     */

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        new RowUpdateBuilder(Triggers, timestamp, mutation)
            .clustering(table.cfName, trigger.name)
            .addMapEntry("trigger_options", "class", trigger.classOption)
            .build();
    }

    private static void dropTriggerFromSchemaMutation(CFMetaData table, TriggerDefinition trigger, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder.deleteRow(Triggers, timestamp, mutation, table.cfName, trigger.name);
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param partition storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    private static List<TriggerDefinition> createTriggersFromTriggersPartition(RowIterator partition)
    {
        List<TriggerDefinition> triggers = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, TRIGGERS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            String name = row.getString("trigger_name");
            String classOption = row.getMap("trigger_options", UTF8Type.instance, UTF8Type.instance).get("class");
            triggers.add(new TriggerDefinition(name, classOption));
        }
        return triggers;
    }

    /*
     * UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addFunctionToSchemaMutation(function, timestamp, mutation);
        return mutation;
    }

    private static void addFunctionToSchemaMutation(UDFunction function, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Functions, timestamp, mutation)
                                 .clustering(function.name().name, functionSignatureWithTypes(function));

        adder.add("body", function.body());
        adder.add("language", function.language());
        adder.add("return_type", function.returnType().toString());
        adder.add("called_on_null_input", function.isCalledOnNullInput());

        adder.resetCollection("argument_names");
        adder.resetCollection("argument_types");
        for (int i = 0; i < function.argNames().size(); i++)
        {
            adder.addListEntry("argument_names", function.argNames().get(i).bytes);
            adder.addListEntry("argument_types", function.argTypes().get(i).toString());
        }
        adder.build();
    }

    public static Mutation makeDropFunctionMutation(KSMetaData keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Functions, timestamp, mutation, function.name().name, functionSignatureWithTypes(function));
    }

    private static Collection<UDFunction> createFunctionsFromFunctionsPartition(RowIterator partition)
    {
        List<UDFunction> functions = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, FUNCTIONS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            functions.add(createFunctionFromFunctionRow(row));
        return functions;
    }

    private static UDFunction createFunctionFromFunctionRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("function_name");
        FunctionName name = new FunctionName(ksName, functionName);

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
                logger.debug("Skipping duplicate compilation of already existing UDF {}", name);
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

    /*
     * Aggregate UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        addAggregateToSchemaMutation(aggregate, timestamp, mutation);
        return mutation;
    }

    private static void addAggregateToSchemaMutation(UDAggregate aggregate, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Aggregates, timestamp, mutation)
                                 .clustering(aggregate.name().name, functionSignatureWithTypes(aggregate));

        adder.resetCollection("argument_types");
        adder.add("return_type", aggregate.returnType().toString());
        adder.add("state_func", aggregate.stateFunction().name().toString());
        if (aggregate.stateType() != null)
            adder.add("state_type", aggregate.stateType().toString());
        if (aggregate.finalFunction() != null)
            adder.add("final_func", aggregate.finalFunction().name().toString());
        if (aggregate.initialCondition() != null)
            adder.add("initcond", aggregate.initialCondition());

        for (AbstractType<?> argType : aggregate.argTypes())
            adder.addListEntry("argument_types", argType.toString());

        adder.build();
    }

    private static Collection<UDAggregate> createAggregatesFromAggregatesPartition(RowIterator partition)
    {
        List<UDAggregate> aggregates = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", SystemKeyspace.NAME, AGGREGATES);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
            aggregates.add(createAggregateFromAggregateRow(row));
        return aggregates;
    }

    private static UDAggregate createAggregateFromAggregateRow(UntypedResultSet.Row row)
    {
        String ksName = row.getString("keyspace_name");
        String functionName = row.getString("aggregate_name");
        FunctionName name = new FunctionName(ksName, functionName);

        List<String> types = row.getList("argument_types", UTF8Type.instance);

        List<AbstractType<?>> argTypes;
        if (types == null)
        {
            argTypes = Collections.emptyList();
        }
        else
        {
            argTypes = new ArrayList<>(types.size());
            for (String type : types)
                argTypes.add(parseType(type));
        }

        AbstractType<?> returnType = parseType(row.getString("return_type"));

        FunctionName stateFunc = aggregateParseFunctionName(ksName, row.getString("state_func"));
        FunctionName finalFunc = row.has("final_func") ? aggregateParseFunctionName(ksName, row.getString("final_func")) : null;
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

    private static FunctionName aggregateParseFunctionName(String ksName, String func)
    {
        int i = func.indexOf('.');

        // function name can be abbreviated (pre 2.2rc2) - it is in the same keyspace as the aggregate
        if (i == -1)
            return new FunctionName(ksName, func);

        String ks = func.substring(0, i);
        String f = func.substring(i + 1);

        // only aggregate's function keyspace and system keyspace are allowed
        assert ks.equals(ksName) || ks.equals(SystemKeyspace.NAME);

        return new FunctionName(ks, f);
    }

    public static Mutation makeDropAggregateMutation(KSMetaData keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace, timestamp, false);
        return RowUpdateBuilder.deleteRow(Aggregates, timestamp, mutation, aggregate.name().name, functionSignatureWithTypes(aggregate));
    }

    private static AbstractType<?> parseType(String str)
    {
        return TypeParser.parse(str);
    }

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema
    // we use a "signature" which is just a list of it's CQL argument types (we could replace that by
    // using a "signature" UDT that would be comprised of the function name and argument types,
    // which we could then use as clustering column. But as we haven't yet used UDT in system tables,
    // We'll leave that decision to #6717).
    public static ByteBuffer functionSignatureWithTypes(AbstractFunction fun)
    {
        ListType<String> list = ListType.getInstance(UTF8Type.instance, false);
        List<String> strList = new ArrayList<>(fun.argTypes().size());
        for (AbstractType<?> argType : fun.argTypes())
            strList.add(argType.asCQL3Type().toString());
        return list.decompose(strList);
    }
}
