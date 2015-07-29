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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.cql3.statements.CFPropDefs;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.stream.Collectors.toSet;

import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;
import static org.apache.cassandra.utils.FBUtilities.json;

/**
 * system_schema.* tables and methods for manipulating them.
 */
public final class SchemaKeyspace
{
    private SchemaKeyspace()
    {
    }

    private static final Logger logger = LoggerFactory.getLogger(SchemaKeyspace.class);

    public static final String NAME = "system_schema";

    public static final String KEYSPACES = "keyspaces";
    public static final String TABLES = "tables";
    public static final String COLUMNS = "columns";
    public static final String DROPPED_COLUMNS = "dropped_columns";
    public static final String TRIGGERS = "triggers";
    public static final String MATERIALIZED_VIEWS = "materialized_views";
    public static final String TYPES = "types";
    public static final String FUNCTIONS = "functions";
    public static final String AGGREGATES = "aggregates";

    public static final List<String> ALL =
        ImmutableList.of(KEYSPACES, TABLES, COLUMNS, TRIGGERS, MATERIALIZED_VIEWS, TYPES, FUNCTIONS, AGGREGATES);

    private static final CFMetaData Keyspaces =
        compile(KEYSPACES,
                "keyspace definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "durable_writes boolean,"
                + "replication map<text, text>,"
                + "PRIMARY KEY ((keyspace_name)))");

    private static final CFMetaData Tables =
        compile(TABLES,
                "table definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "bloom_filter_fp_chance double,"
                + "caching map<text, text>,"
                + "comment text,"
                + "compaction map<text, text>,"
                + "compression map<text, text>,"
                + "dclocal_read_repair_chance double,"
                + "default_time_to_live int,"
                + "flags set<text>," // SUPER, COUNTER, DENSE, COMPOUND
                + "gc_grace_seconds int,"
                + "id uuid,"
                + "max_index_interval int,"
                + "memtable_flush_period_in_ms int,"
                + "min_index_interval int,"
                + "read_repair_chance double,"
                + "speculative_retry text,"
                + "PRIMARY KEY ((keyspace_name), table_name))");

    private static final CFMetaData Columns =
        compile(COLUMNS,
                "column definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "column_name text,"
                + "column_name_bytes blob,"
                + "component_index int,"
                + "index_name text,"
                + "index_options text,"
                + "index_type text,"
                + "type text,"
                + "validator text,"
                + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final CFMetaData DroppedColumns =
        compile(DROPPED_COLUMNS,
                "dropped column registry",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "column_name text,"
                + "dropped_time timestamp,"
                + "type text,"
                + "PRIMARY KEY ((keyspace_name), table_name, column_name))");

    private static final CFMetaData Triggers =
        compile(TRIGGERS,
                "trigger definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "trigger_name text,"
                + "trigger_options map<text, text>,"
                + "PRIMARY KEY ((keyspace_name), table_name, trigger_name))");

    private static final CFMetaData MaterializedViews =
        compile(MATERIALIZED_VIEWS,
                "materialized views definitions",
                "CREATE TABLE %s ("
                + "keyspace_name text,"
                + "table_name text,"
                + "view_name text,"
                + "target_columns list<text>,"
                + "clustering_columns list<text>,"
                + "included_columns list<text>,"
                + "PRIMARY KEY ((keyspace_name), table_name, view_name))");

    private static final CFMetaData Types =
        compile(TYPES,
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

    public static final List<CFMetaData> All =
        ImmutableList.of(Keyspaces, Tables, Columns, Triggers, DroppedColumns, MaterializedViews, Types, Functions, Aggregates);

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description)
                         .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(7));
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.local(), org.apache.cassandra.schema.Tables.of(All));
    }

    /**
     * Add entries to system_schema.* for the hardcoded system keyspaces
     */
    public static void saveSystemKeyspacesSchema()
    {
        KeyspaceMetadata system = Schema.instance.getKSMetaData(SystemKeyspace.NAME);
        KeyspaceMetadata schema = Schema.instance.getKSMetaData(NAME);

        long timestamp = FBUtilities.timestampMicros();

        // delete old, possibly obsolete entries in schema tables
        for (String schemaTable : ALL)
        {
            String query = String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE keyspace_name = ?", NAME, schemaTable);
            for (String systemKeyspace : Schema.SYSTEM_KEYSPACE_NAMES)
                executeOnceInternal(query, timestamp, systemKeyspace);
        }

        // (+1 to timestamp to make sure we don't get shadowed by the tombstones we just added)
        makeCreateKeyspaceMutation(system, timestamp + 1).apply();
        makeCreateKeyspaceMutation(schema, timestamp + 1).apply();
    }

    public static List<KeyspaceMetadata> readSchemaFromSystemTables()
    {
        ReadCommand cmd = getReadCommandForTableSchema(KEYSPACES);
        try (ReadOrderGroup orderGroup = cmd.startOrderGroup(); PartitionIterator schema = cmd.executeInternal(orderGroup))
        {
            List<KeyspaceMetadata> keyspaces = new ArrayList<>();

            while (schema.hasNext())
            {
                try (RowIterator partition = schema.next())
                {
                    if (isSystemKeyspaceSchemaPartition(partition.partitionKey()))
                        continue;

                    DecoratedKey key = partition.partitionKey();

                    readSchemaPartitionForKeyspaceAndApply(TYPES, key,
                        types -> readSchemaPartitionForKeyspaceAndApply(TABLES, key,
                        tables -> readSchemaPartitionForKeyspaceAndApply(FUNCTIONS, key,
                        functions -> readSchemaPartitionForKeyspaceAndApply(AGGREGATES, key,
                        aggregates -> keyspaces.add(createKeyspaceFromSchemaPartitions(partition, tables, types, functions, aggregates)))))
                    );
                }
            }
            return keyspaces;
        }
    }

    public static void truncate()
    {
        ALL.forEach(table -> getSchemaCFS(table).truncateBlocking());
    }

    static void flush()
    {
        if (!Boolean.getBoolean("cassandra.unsafesystem"))
            ALL.forEach(table -> FBUtilities.waitOnFuture(getSchemaCFS(table).forceFlush()));
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
            try (ReadOrderGroup orderGroup = cmd.startOrderGroup();
                 PartitionIterator schema = cmd.executeInternal(orderGroup))
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
        return Keyspace.open(NAME).getColumnFamilyStore(schemaTableName);
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
                        mutation = new Mutation(NAME, key);
                        mutationMap.put(key, mutation);
                    }

                    mutation.add(PartitionUpdate.fromIterator(partition));
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
        return Schema.isSystemKeyspace(UTF8Type.instance.compose(partitionKey.getKey()));
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
        Map<DecoratedKey, FilteredPartition> oldColumnFamilies = readSchemaForKeyspaces(TABLES, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldTypes = readSchemaForKeyspaces(TYPES, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, FilteredPartition> oldAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        mutations.forEach(Mutation::apply);

        if (doFlush)
            flush();

        // with new data applied
        Map<DecoratedKey, FilteredPartition> newKeyspaces = readSchemaForKeyspaces(KEYSPACES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newColumnFamilies = readSchemaForKeyspaces(TABLES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newTypes = readSchemaForKeyspaces(TYPES, keyspaces);
        Map<DecoratedKey, FilteredPartition> newFunctions = readSchemaForKeyspaces(FUNCTIONS, keyspaces);
        Map<DecoratedKey, FilteredPartition> newAggregates = readSchemaForKeyspaces(AGGREGATES, keyspaces);

        Set<String> keyspacesToDrop = mergeKeyspaces(oldKeyspaces, newKeyspaces);
        mergeTables(oldColumnFamilies, newColumnFamilies);
        mergeTypes(oldTypes, newTypes);
        mergeFunctions(oldFunctions, newFunctions);
        mergeAggregates(oldAggregates, newAggregates);

        // it is safe to drop a keyspace only when all nested ColumnFamilies where deleted
        keyspacesToDrop.forEach(Schema.instance::dropKeyspace);
    }

    private static Set<String> mergeKeyspaces(Map<DecoratedKey, FilteredPartition> before, Map<DecoratedKey, FilteredPartition> after)
    {
        for (FilteredPartition newPartition : after.values())
        {
            String name = AsciiType.instance.compose(newPartition.partitionKey().getKey());
            KeyspaceParams params = createKeyspaceParamsFromSchemaPartition(newPartition.rowIterator());

            FilteredPartition oldPartition = before.remove(newPartition.partitionKey());
            if (oldPartition == null || oldPartition.isEmpty())
                Schema.instance.addKeyspace(KeyspaceMetadata.create(name, params));
            else
                Schema.instance.updateKeyspace(name, params);
        }

        // What's remain in old is those keyspace that are not in updated, i.e. the dropped ones.
        return asKeyspaceNamesSet(before.keySet());
    }

    private static Set<String> asKeyspaceNamesSet(Set<DecoratedKey> keys)
    {
        Set<String> names = new HashSet<>(keys.size());
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
                Schema.instance.dropTable(oldRow.getString("keyspace_name"), oldRow.getString("table_name"));
            }

            public void onAdded(UntypedResultSet.Row newRow)
            {
                Schema.instance.addTable(createTableFromTableRow(newRow));
            }

            public void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow)
            {
                Schema.instance.updateTable(newRow.getString("keyspace_name"), newRow.getString("table_name"));
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
        void onDropped(UntypedResultSet.Row oldRow);
        void onAdded(UntypedResultSet.Row newRow);
        void onUpdated(UntypedResultSet.Row oldRow, UntypedResultSet.Row newRow);
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

    public static Mutation makeCreateKeyspaceMutation(String name, KeyspaceParams params, long timestamp)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Keyspaces, timestamp, name).clustering();
        return adder.add(KeyspaceParams.Option.DURABLE_WRITES.toString(), params.durableWrites)
                    .map(KeyspaceParams.Option.REPLICATION.toString(), params.replication.asMap())
                    .build();
    }

    public static Mutation makeCreateKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        keyspace.tables.forEach(table -> addTableToSchemaMutation(table, timestamp, true, mutation));
        keyspace.types.forEach(type -> addTypeToSchemaMutation(type, timestamp, mutation));
        keyspace.functions.udfs().forEach(udf -> addFunctionToSchemaMutation(udf, timestamp, mutation));
        keyspace.functions.udas().forEach(uda -> addAggregateToSchemaMutation(uda, timestamp, mutation));

        return mutation;
    }

    public static Mutation makeDropKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        Mutation mutation = new Mutation(NAME, getSchemaKSDecoratedKey(keyspace.name));
        for (CFMetaData schemaTable : All)
            mutation.add(PartitionUpdate.fullPartitionDelete(schemaTable, mutation.key(), timestamp, nowInSec));
        mutation.add(PartitionUpdate.fullPartitionDelete(SystemKeyspace.BuiltMaterializedViews, mutation.key(), timestamp, nowInSec));
        mutation.add(PartitionUpdate.fullPartitionDelete(SystemKeyspace.MaterializedViewsBuildsInProgress, mutation.key(), timestamp, nowInSec));
        return mutation;
    }

    private static KeyspaceMetadata createKeyspaceFromSchemaPartitions(RowIterator serializedParams,
                                                                       RowIterator serializedTables,
                                                                       RowIterator serializedTypes,
                                                                       RowIterator serializedFunctions,
                                                                       RowIterator serializedAggregates)
    {
        String name = AsciiType.instance.compose(serializedParams.partitionKey().getKey());

        KeyspaceParams params = createKeyspaceParamsFromSchemaPartition(serializedParams);
        Tables tables = createTablesFromTablesPartition(serializedTables);
        Types types = createTypesFromPartition(serializedTypes);

        Collection<UDFunction> udfs = createFunctionsFromFunctionsPartition(serializedFunctions);
        Collection<UDAggregate> udas = createAggregatesFromAggregatesPartition(serializedAggregates);
        Functions functions = org.apache.cassandra.schema.Functions.builder().add(udfs).add(udas).build();

        return KeyspaceMetadata.create(name, params, tables, types, functions);
    }

    /**
     * Deserialize only Keyspace attributes without nested tables or types
     *
     * @param partition Keyspace attributes in serialized form
     */

    private static KeyspaceParams createKeyspaceParamsFromSchemaPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, KEYSPACES);
        UntypedResultSet.Row row = QueryProcessor.resultify(query, partition).one();

        return KeyspaceParams.create(row.getBoolean(KeyspaceParams.Option.DURABLE_WRITES.toString()),
                                     row.getTextMap(KeyspaceParams.Option.REPLICATION.toString()));
    }

    /*
     * User type metadata serialization/deserialization.
     */

    public static Mutation makeCreateTypeMutation(KeyspaceMetadata keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addTypeToSchemaMutation(type, timestamp, mutation);
        return mutation;
    }

    static void addTypeToSchemaMutation(UserType type, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Types, timestamp, mutation)
                                 .clustering(type.getNameAsString());

        adder.resetCollection("field_names")
             .resetCollection("field_types");

        for (int i = 0; i < type.size(); i++)
        {
            adder.addListEntry("field_names", type.fieldName(i))
                 .addListEntry("field_types", type.fieldType(i).toString());
        }

        adder.build();
    }

    public static Mutation dropTypeFromSchemaMutation(KeyspaceMetadata keyspace, UserType type, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        return RowUpdateBuilder.deleteRow(Types, timestamp, mutation, type.name);
    }

    private static Types createTypesFromPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, TYPES);
        Types.Builder types = org.apache.cassandra.schema.Types.builder();
        QueryProcessor.resultify(query, partition).forEach(row -> types.add(createTypeFromRow(row)));
        return types.build();
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

    public static Mutation makeCreateTableMutation(KeyspaceMetadata keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addTableToSchemaMutation(table, timestamp, true, mutation);
        return mutation;
    }

    static void addTableToSchemaMutation(CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Tables, timestamp, mutation).clustering(table.cfName);

        adder.add("bloom_filter_fp_chance", table.getBloomFilterFpChance())
             .add("comment", table.getComment())
             .add("dclocal_read_repair_chance", table.getDcLocalReadRepairChance())
             .add("default_time_to_live", table.getDefaultTimeToLive())
             .add("gc_grace_seconds", table.getGcGraceSeconds())
             .add("id", table.cfId)
             .add("max_index_interval", table.getMaxIndexInterval())
             .add("memtable_flush_period_in_ms", table.getMemtableFlushPeriod())
             .add("min_index_interval", table.getMinIndexInterval())
             .add("read_repair_chance", table.getReadRepairChance())
             .add("speculative_retry", table.getSpeculativeRetry().toString())
             .map("caching", table.getCaching().asMap())
             .map("compaction", buildCompactionMap(table))
             .map("compression", table.compressionParameters().asMap())
             .set("flags", flagsToStrings(table.flags()))
             .build();

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, timestamp, mutation);

            for (CFMetaData.DroppedColumn column : table.getDroppedColumns().values())
                addDroppedColumnToSchemaMutation(table, column, timestamp, mutation);

            for (TriggerMetadata trigger : table.getTriggers())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);

            for (MaterializedViewDefinition materializedView: table.getMaterializedViews())
                addMaterializedViewToSchemaMutation(table, materializedView, timestamp, mutation);
        }
    }

    /*
     * The method is needed - temporarily - to migrate max_compaction_threshold and min_compaction_threshold
     * to the compaction map, where they belong.
     *
     * We must use reflection to validate the options because not every compaction strategy respects and supports
     * the threshold params (LCS doesn't, STCS and DTCS don't).
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> buildCompactionMap(CFMetaData cfm)
    {
        Map<String, String> options = new HashMap<>(cfm.compactionStrategyOptions);

        Map<String, String> optionsWithThresholds = new HashMap<>(options);
        options.putIfAbsent(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, Integer.toString(cfm.getMinCompactionThreshold()));
        options.putIfAbsent(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, Integer.toString(cfm.getMaxCompactionThreshold()));

        try
        {
            Map<String, String> unrecognizedOptions = (Map<String, String>) cfm.compactionStrategyClass
                                                                               .getMethod("validateOptions", Map.class)
                                                                               .invoke(null, optionsWithThresholds);
            if (unrecognizedOptions.isEmpty())
                options = optionsWithThresholds;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        options.put("class", cfm.compactionStrategyClass.getName());

        return options;
    }

    public static Mutation makeUpdateTableMutation(KeyspaceMetadata keyspace,
                                                   CFMetaData oldTable,
                                                   CFMetaData newTable,
                                                   long timestamp,
                                                   boolean fromThrift)
    {
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        addTableToSchemaMutation(newTable, timestamp, false, mutation);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(oldTable.getColumnMetadata(),
                                                                                 newTable.getColumnMetadata());

        // columns that are no longer needed
        for (ColumnDefinition column : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (!fromThrift ||
                column.kind == ColumnDefinition.Kind.REGULAR ||
                (newTable.isStaticCompactTable() && column.kind == ColumnDefinition.Kind.STATIC))
            {
                dropColumnFromSchemaMutation(oldTable, column, timestamp, mutation);
            }
        }

        // newly added columns
        for (ColumnDefinition column : columnDiff.entriesOnlyOnRight().values())
            addColumnToSchemaMutation(newTable, column, timestamp, mutation);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            addColumnToSchemaMutation(newTable, newTable.getColumnDefinition(name), timestamp, mutation);

        // dropped columns
        MapDifference<ByteBuffer, CFMetaData.DroppedColumn> droppedColumnDiff =
            Maps.difference(oldTable.getDroppedColumns(), newTable.getDroppedColumns());

        // newly dropped columns
        for (CFMetaData.DroppedColumn column : droppedColumnDiff.entriesOnlyOnRight().values())
            addDroppedColumnToSchemaMutation(newTable, column, timestamp, mutation);

        // columns added then dropped again
        for (ByteBuffer name : droppedColumnDiff.entriesDiffering().keySet())
            addDroppedColumnToSchemaMutation(newTable, newTable.getDroppedColumns().get(name), timestamp, mutation);

        MapDifference<String, TriggerMetadata> triggerDiff = triggersDiff(oldTable.getTriggers(), newTable.getTriggers());

        // dropped triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnLeft().values())
            dropTriggerFromSchemaMutation(oldTable, trigger, timestamp, mutation);

        // newly created triggers
        for (TriggerMetadata trigger : triggerDiff.entriesOnlyOnRight().values())
            addTriggerToSchemaMutation(newTable, trigger, timestamp, mutation);

        MapDifference<String, MaterializedViewDefinition> materializedViewDiff = materializedViewsDiff(oldTable.getMaterializedViews(), newTable.getMaterializedViews());

        // dropped materialized views
        for (MaterializedViewDefinition materializedView : materializedViewDiff.entriesOnlyOnLeft().values())
            dropMaterializedViewFromSchemaMutation(oldTable, materializedView, timestamp, mutation);

        // newly created materialized views
        for (MaterializedViewDefinition materializedView : materializedViewDiff.entriesOnlyOnRight().values())
            addMaterializedViewToSchemaMutation(oldTable, materializedView, timestamp, mutation);

        // updated materialized views need to be updated
        for (MapDifference.ValueDifference<MaterializedViewDefinition> diff : materializedViewDiff.entriesDiffering().values())
        {
            addUpdatedMaterializedViewDefinitionToSchemaMutation(oldTable, diff.rightValue(), timestamp, mutation);
        }

        return mutation;
    }

    private static MapDifference<String, TriggerMetadata> triggersDiff(Triggers before, Triggers after)
    {
        Map<String, TriggerMetadata> beforeMap = new HashMap<>();
        before.forEach(t -> beforeMap.put(t.name, t));

        Map<String, TriggerMetadata> afterMap = new HashMap<>();
        after.forEach(t -> afterMap.put(t.name, t));

        return Maps.difference(beforeMap, afterMap);
    }

    private static MapDifference<String, MaterializedViewDefinition> materializedViewsDiff(MaterializedViews before, MaterializedViews after)
    {
        Map<String, MaterializedViewDefinition> beforeMap = new HashMap<>();
        before.forEach(v -> beforeMap.put(v.viewName, v));

        Map<String, MaterializedViewDefinition> afterMap = new HashMap<>();
        after.forEach(v -> afterMap.put(v.viewName, v));

        return Maps.difference(beforeMap, afterMap);
    }

    public static Mutation makeDropTableMutation(KeyspaceMetadata keyspace, CFMetaData table, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);

        RowUpdateBuilder.deleteRow(Tables, timestamp, mutation, table.cfName);

        for (ColumnDefinition column : table.allColumns())
            dropColumnFromSchemaMutation(table, column, timestamp, mutation);

        for (TriggerMetadata trigger : table.getTriggers())
            dropTriggerFromSchemaMutation(table, trigger, timestamp, mutation);

        for (MaterializedViewDefinition materializedView : table.getMaterializedViews())
            dropMaterializedViewFromSchemaMutation(table, materializedView, timestamp, mutation);

        return mutation;
    }

    public static CFMetaData createTableFromName(String keyspace, String table)
    {
        return readSchemaPartitionForTableAndApply(TABLES, keyspace, table, partition ->
        {
            if (partition.isEmpty())
                throw new RuntimeException(String.format("%s:%s not found in the schema definitions keyspace.", keyspace, table));

            return createTableFromTablePartition(partition);
        });
    }

    /**
     * Deserialize tables from low-level schema representation, all of them belong to the same keyspace
     */
    private static Tables createTablesFromTablesPartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, TABLES);
        Tables.Builder tables = org.apache.cassandra.schema.Tables.builder();
        QueryProcessor.resultify(query, partition).forEach(row -> tables.add(createTableFromTableRow(row)));
        return tables.build();
    }

    private static List<ColumnDefinition> createColumnsFromColumnsPartition(RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, COLUMNS);
        return createColumnsFromColumnRows(QueryProcessor.resultify(query, serializedColumns));
    }

    private static CFMetaData createTableFromTablePartition(RowIterator partition)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, TABLES);
        return createTableFromTableRow(QueryProcessor.resultify(query, partition).one());
    }

    public static CFMetaData createTableFromTablePartitionAndColumnsPartition(RowIterator tablePartition,
                                                                              RowIterator columnsPartition)
    {
        List<ColumnDefinition> columns = createColumnsFromColumnsPartition(columnsPartition);
        String query = String.format("SELECT * FROM %s.%s", NAME, TABLES);
        return createTableFromTableRowAndColumns(QueryProcessor.resultify(query, tablePartition).one(), columns);
    }

    /**
     * Deserialize table metadata from low-level representation
     *
     * @return Metadata deserialized from schema
     */
    private static CFMetaData createTableFromTableRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");

        List<ColumnDefinition> columns =
            readSchemaPartitionForTableAndApply(COLUMNS, keyspace, table, SchemaKeyspace::createColumnsFromColumnsPartition);

        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns =
            readSchemaPartitionForTableAndApply(DROPPED_COLUMNS, keyspace, table, SchemaKeyspace::createDroppedColumnsFromDroppedColumnsPartition);

        Triggers triggers =
            readSchemaPartitionForTableAndApply(TRIGGERS, keyspace, table, SchemaKeyspace::createTriggersFromTriggersPartition);

        MaterializedViews views =
            readSchemaPartitionForTableAndApply(MATERIALIZED_VIEWS, keyspace, table, SchemaKeyspace::createMaterializedViewsFromMaterializedViewsPartition);

        return createTableFromTableRowAndColumns(row, columns).droppedColumns(droppedColumns)
                                                              .triggers(triggers)
                                                              .materializedViews(views);
    }

    public static CFMetaData createTableFromTableRowAndColumns(UntypedResultSet.Row row, List<ColumnDefinition> columns)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");
        UUID id = row.getUUID("id");

        Set<CFMetaData.Flag> flags = row.has("flags")
                                   ? flagsFromStrings(row.getSet("flags", UTF8Type.instance))
                                   : Collections.emptySet();

        boolean isSuper = flags.contains(CFMetaData.Flag.SUPER);
        boolean isCounter = flags.contains(CFMetaData.Flag.COUNTER);
        boolean isDense = flags.contains(CFMetaData.Flag.DENSE);
        boolean isCompound = flags.contains(CFMetaData.Flag.COMPOUND);
        boolean isMaterializedView = flags.contains(CFMetaData.Flag.MATERIALIZEDVIEW);

        CFMetaData cfm = CFMetaData.create(keyspace, table, id, isDense, isCompound, isSuper, isCounter, isMaterializedView, columns);

        Map<String, String> compaction = new HashMap<>(row.getTextMap("compaction"));
        Class<? extends AbstractCompactionStrategy> compactionStrategyClass =
            CFMetaData.createCompactionStrategy(compaction.remove("class"));

        int minCompactionThreshold = compaction.containsKey(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD)
                                   ? Integer.parseInt(compaction.get(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD))
                                   : CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD;

        int maxCompactionThreshold = compaction.containsKey(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD)
                                   ? Integer.parseInt(compaction.get(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD))
                                   : CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD;

        cfm.bloomFilterFpChance(row.getDouble("bloom_filter_fp_chance"))
           .caching(CachingOptions.fromMap(row.getTextMap("caching")))
           .comment(row.getString("comment"))
           .compactionStrategyClass(compactionStrategyClass)
           .compactionStrategyOptions(compaction)
           .compressionParameters(CompressionParameters.fromMap(row.getTextMap("compression")))
           .dcLocalReadRepairChance(row.getDouble("dclocal_read_repair_chance"))
           .defaultTimeToLive(row.getInt("default_time_to_live"))
           .gcGraceSeconds(row.getInt("gc_grace_seconds"))
           .maxCompactionThreshold(maxCompactionThreshold)
           .maxIndexInterval(row.getInt("max_index_interval"))
           .memtableFlushPeriod(row.getInt("memtable_flush_period_in_ms"))
           .minCompactionThreshold(minCompactionThreshold)
           .minIndexInterval(row.getInt("min_index_interval"))
           .readRepairChance(row.getDouble("read_repair_chance"))
           .speculativeRetry(CFMetaData.SpeculativeRetry.fromString(row.getString("speculative_retry")));

        return cfm;
    }

    public static Set<CFMetaData.Flag> flagsFromStrings(Set<String> strings)
    {
        return strings.stream()
                      .map(String::toUpperCase)
                      .map(CFMetaData.Flag::valueOf)
                      .collect(toSet());
    }

    private static Set<String> flagsToStrings(Set<CFMetaData.Flag> flags)
    {
        return flags.stream()
                    .map(CFMetaData.Flag::toString)
                    .map(String::toLowerCase)
                    .collect(toSet());
    }

    /*
     * Column metadata serialization/deserialization.
     */

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Columns, timestamp, mutation).clustering(table.cfName, column.name.toString());

        adder.add("column_name_bytes", column.name.bytes)
             .add("validator", column.type.toString())
             .add("type", column.kind.toString().toLowerCase())
             .add("component_index", column.isOnAllComponents() ? null : column.position())
             .add("index_name", column.getIndexName())
             .add("index_type", column.getIndexType() == null ? null : column.getIndexType().toString())
             .add("index_options", json(column.getIndexOptions()))
             .build();
    }

    private static void dropColumnFromSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        // Note: we do want to use name.toString(), not name.bytes directly for backward compatibility (For CQL3, this won't make a difference).
        RowUpdateBuilder.deleteRow(Columns, timestamp, mutation, table.cfName, column.name.toString());
    }

    private static List<ColumnDefinition> createColumnsFromColumnRows(UntypedResultSet rows)
{
        List<ColumnDefinition> columns = new ArrayList<>(rows.size());
        rows.forEach(row -> columns.add(createColumnFromColumnRow(row)));
        return columns;
    }

    private static ColumnDefinition createColumnFromColumnRow(UntypedResultSet.Row row)
    {
        String keyspace = row.getString("keyspace_name");
        String table = row.getString("table_name");

        ColumnIdentifier name = ColumnIdentifier.getInterned(row.getBytes("column_name_bytes"), row.getString("column_name"));

        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(row.getString("type").toUpperCase());

        Integer componentIndex = null;
        if (row.has("component_index"))
            componentIndex = row.getInt("component_index");

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
     * Dropped column metadata serialization/deserialization.
     */

    private static void addDroppedColumnToSchemaMutation(CFMetaData table, CFMetaData.DroppedColumn column, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(DroppedColumns, timestamp, mutation).clustering(table.cfName, column.name);

        adder.add("dropped_time", new Date(TimeUnit.MICROSECONDS.toMillis(column.droppedTime)))
             .add("type", column.type.toString())
             .build();
    }

    private static Map<ByteBuffer, CFMetaData.DroppedColumn> createDroppedColumnsFromDroppedColumnsPartition(RowIterator serializedColumns)
    {
        String query = String.format("SELECT * FROM %s.%s", NAME, DROPPED_COLUMNS);
        Map<ByteBuffer, CFMetaData.DroppedColumn> columns = new HashMap<>();
        for (CFMetaData.DroppedColumn column : createDroppedColumnsFromDroppedColumnRows(QueryProcessor.resultify(query, serializedColumns)))
            columns.put(UTF8Type.instance.decompose(column.name), column);
        return columns;
    }

    private static List<CFMetaData.DroppedColumn> createDroppedColumnsFromDroppedColumnRows(UntypedResultSet rows)
    {
        List<CFMetaData.DroppedColumn> columns = new ArrayList<>(rows.size());
        rows.forEach(row -> columns.add(createDroppedColumnFromDroppedColumnRow(row)));
        return columns;
    }

    private static CFMetaData.DroppedColumn createDroppedColumnFromDroppedColumnRow(UntypedResultSet.Row row)
    {
        String name = row.getString("column_name");
        AbstractType type = TypeParser.parse(row.getString("type"));
        long droppedTime = TimeUnit.MILLISECONDS.toMicros(row.getLong("dropped_time"));

        return new CFMetaData.DroppedColumn(name, type, droppedTime);
    }

    /*
     * Trigger metadata serialization/deserialization.
     */

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerMetadata trigger, long timestamp, Mutation mutation)
    {
        new RowUpdateBuilder(Triggers, timestamp, mutation)
            .clustering(table.cfName, trigger.name)
            .addMapEntry("trigger_options", "class", trigger.classOption)
            .build();
    }

    private static void dropTriggerFromSchemaMutation(CFMetaData table, TriggerMetadata trigger, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder.deleteRow(Triggers, timestamp, mutation, table.cfName, trigger.name);
    }

    /**
     * Deserialize triggers from storage-level representation.
     *
     * @param partition storage-level partition containing the trigger definitions
     * @return the list of processed TriggerDefinitions
     */
    private static Triggers createTriggersFromTriggersPartition(RowIterator partition)
    {
        Triggers.Builder triggers = org.apache.cassandra.schema.Triggers.builder();
        String query = String.format("SELECT * FROM %s.%s", NAME, TRIGGERS);
        QueryProcessor.resultify(query, partition).forEach(row -> triggers.add(createTriggerFromTriggerRow(row)));
        return triggers.build();
    }

    private static TriggerMetadata createTriggerFromTriggerRow(UntypedResultSet.Row row)
    {
        String name = row.getString("trigger_name");
        String classOption = row.getTextMap("trigger_options").get("class");
        return new TriggerMetadata(name, classOption);
    }

    /*
     * Global Index metadata serialization/deserialization.
     */

    private static void addMaterializedViewToSchemaMutation(CFMetaData table, MaterializedViewDefinition materializedView, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder builder = new RowUpdateBuilder(MaterializedViews, timestamp, mutation)
            .clustering(table.cfName, materializedView.viewName);

        for (ColumnIdentifier partitionColumn : materializedView.partitionColumns)
            builder.addListEntry("target_columns", partitionColumn.toString());
        for (ColumnIdentifier clusteringColumn : materializedView.clusteringColumns)
            builder = builder.addListEntry("clustering_columns", clusteringColumn.toString());
        for (ColumnIdentifier includedColumn : materializedView.included)
            builder = builder.addListEntry("included_columns", includedColumn.toString());

        builder.build();
    }

    private static void dropMaterializedViewFromSchemaMutation(CFMetaData table, MaterializedViewDefinition materializedView, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder.deleteRow(MaterializedViews, timestamp, mutation, table.cfName, materializedView.viewName);
    }

    private static void addUpdatedMaterializedViewDefinitionToSchemaMutation(CFMetaData table, MaterializedViewDefinition materializedView, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder builder = new RowUpdateBuilder(MaterializedViews, timestamp, mutation)
                                   .clustering(table.cfName, materializedView.viewName);

        builder.resetCollection("target_columns");
        for (ColumnIdentifier partitionColumn : materializedView.partitionColumns)
            builder.addListEntry("target_columns", partitionColumn.toString());

        builder.resetCollection("clustering_columns");
        for (ColumnIdentifier clusteringColumn : materializedView.clusteringColumns)
            builder = builder.addListEntry("clustering_columns", clusteringColumn.toString());

        builder.resetCollection("included_columns");
        for (ColumnIdentifier includedColumn : materializedView.included)
            builder = builder.addListEntry("included_columns", includedColumn.toString());

        builder.build();
    }

    /**
     * Deserialize materialized views from storage-level representation.
     *
     * @param partition storage-level partition containing the materialized view definitions
     * @return the list of processed MaterializedViewDefinitions
     */
    private static MaterializedViews createMaterializedViewsFromMaterializedViewsPartition(RowIterator partition)
    {
        MaterializedViews.Builder views = org.apache.cassandra.schema.MaterializedViews.builder();
        String query = String.format("SELECT * FROM %s.%s", NAME, MATERIALIZED_VIEWS);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, partition))
        {
            MaterializedViewDefinition mv = createMaterializedViewFromMaterializedViewRow(row);
            views.add(mv);
        }
        return views.build();
    }

    private static MaterializedViewDefinition createMaterializedViewFromMaterializedViewRow(UntypedResultSet.Row row)
    {
        String name = row.getString("view_name");
        List<String> partitionColumnNames = row.getList("target_columns", UTF8Type.instance);

        String cfName = row.getString("columnfamily_name");
        List<String> clusteringColumnNames = row.getList("clustering_columns", UTF8Type.instance);

        List<ColumnIdentifier> partitionColumns = new ArrayList<>();
        for (String columnName : partitionColumnNames)
        {
            partitionColumns.add(ColumnIdentifier.getInterned(columnName, true));
        }

        List<ColumnIdentifier> clusteringColumns = new ArrayList<>();
        for (String columnName : clusteringColumnNames)
        {
            clusteringColumns.add(ColumnIdentifier.getInterned(columnName, true));
        }

        List<String> includedColumnNames = row.getList("included_columns", UTF8Type.instance);
        Set<ColumnIdentifier> includedColumns = new HashSet<>();
        if (includedColumnNames != null)
        {
            for (String columnName : includedColumnNames)
                includedColumns.add(ColumnIdentifier.getInterned(columnName, true));
        }

        return new MaterializedViewDefinition(cfName,
                                              name,
                                              partitionColumns,
                                              clusteringColumns,
                                              includedColumns);
    }

    /*
     * UDF metadata serialization/deserialization.
     */

    public static Mutation makeCreateFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addFunctionToSchemaMutation(function, timestamp, mutation);
        return mutation;
    }

    static void addFunctionToSchemaMutation(UDFunction function, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Functions, timestamp, mutation)
                                 .clustering(function.name().name, functionSignatureWithTypes(function));

        adder.add("body", function.body())
             .add("language", function.language())
             .add("return_type", function.returnType().toString())
             .add("called_on_null_input", function.isCalledOnNullInput());

        adder.resetCollection("argument_names")
             .resetCollection("argument_types");

        for (int i = 0; i < function.argNames().size(); i++)
        {
            adder.addListEntry("argument_names", function.argNames().get(i).bytes);
            adder.addListEntry("argument_types", function.argTypes().get(i).toString());
        }

        adder.build();
    }

    public static Mutation makeDropFunctionMutation(KeyspaceMetadata keyspace, UDFunction function, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        return RowUpdateBuilder.deleteRow(Functions, timestamp, mutation, function.name().name, functionSignatureWithTypes(function));
    }

    private static Collection<UDFunction> createFunctionsFromFunctionsPartition(RowIterator partition)
    {
        List<UDFunction> functions = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", NAME, FUNCTIONS);
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

    public static Mutation makeCreateAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
        addAggregateToSchemaMutation(aggregate, timestamp, mutation);
        return mutation;
    }

    static void addAggregateToSchemaMutation(UDAggregate aggregate, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Aggregates, timestamp, mutation)
                                 .clustering(aggregate.name().name, functionSignatureWithTypes(aggregate));

        adder.resetCollection("argument_types");

        adder.add("return_type", aggregate.returnType().toString())
             .add("state_func", aggregate.stateFunction().name().name);

        if (aggregate.stateType() != null)
            adder.add("state_type", aggregate.stateType().toString());
        if (aggregate.finalFunction() != null)
            adder.add("final_func", aggregate.finalFunction().name().name);
        if (aggregate.initialCondition() != null)
            adder.add("initcond", aggregate.initialCondition());

        for (AbstractType<?> argType : aggregate.argTypes())
            adder.addListEntry("argument_types", argType.toString());

        adder.build();
    }

    private static Collection<UDAggregate> createAggregatesFromAggregatesPartition(RowIterator partition)
    {
        List<UDAggregate> aggregates = new ArrayList<>();
        String query = String.format("SELECT * FROM %s.%s", NAME, AGGREGATES);
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

        FunctionName stateFunc = new FunctionName(ksName, (row.getString("state_func")));
        FunctionName finalFunc = row.has("final_func") ? new FunctionName(ksName, row.getString("final_func")) : null;
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

    public static Mutation makeDropAggregateMutation(KeyspaceMetadata keyspace, UDAggregate aggregate, long timestamp)
    {
        // Include the serialized keyspace in case the target node missed a CREATE KEYSPACE migration (see CASSANDRA-5631).
        Mutation mutation = makeCreateKeyspaceMutation(keyspace.name, keyspace.params, timestamp);
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
