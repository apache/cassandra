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
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.thrift.ThriftConversion;

import static java.lang.String.format;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.FBUtilities.json;

@SuppressWarnings("deprecation")
public class LegacySchemaMigratorTest
{
    private static final long TIMESTAMP = 1435908994000000L;

    private static final String KEYSPACE_PREFIX = "LegacySchemaMigratorTest";

    /*
     * 1. Write a variety of different keyspaces/tables/types/function in the legacy manner, using legacy schema tables
     * 2. Run the migrator
     * 3. Read all the keyspaces from the new schema tables
     * 4. Make sure that we've read *exactly* the same set of keyspaces/tables/types/functions
     * 5. Validate that the legacy schema tables are now empty
     */
    @Test
    public void testMigrate() throws IOException
    {
        CQLTester.cleanupAndLeaveDirs();

        Keyspaces expected = keyspacesToMigrate();

        // write the keyspaces into the legacy tables
        expected.forEach(LegacySchemaMigratorTest::legacySerializeKeyspace);

        // run the migration
        LegacySchemaMigrator.migrate();

        // read back all the metadata from the new schema tables
        Keyspaces actual = SchemaKeyspace.fetchNonSystemKeyspaces();

        // need to load back CFMetaData of those tables (CFS instances will still be loaded)
        loadLegacySchemaTables();

        // verify that nothing's left in the old schema tables
        for (CFMetaData table : LegacySchemaMigrator.LegacySchemaTables)
        {
            String query = format("SELECT * FROM %s.%s", SystemKeyspace.NAME, table.cfName);
            //noinspection ConstantConditions
            assertTrue(executeOnceInternal(query).isEmpty());
        }

        // make sure that we've read *exactly* the same set of keyspaces/tables/types/functions
        assertEquals(expected.diff(actual).toString(), expected, actual);

        // check that the build status of all indexes has been updated to use the new
        // format of index name: the index_name column of system.IndexInfo used to
        // contain table_name.index_name. Now it should contain just the index_name.
        expected.forEach(LegacySchemaMigratorTest::verifyIndexBuildStatus);
    }

    private static void loadLegacySchemaTables()
    {
        KeyspaceMetadata systemKeyspace = Schema.instance.getKSMetaData(SystemKeyspace.NAME);

        Tables systemTables = systemKeyspace.tables;
        for (CFMetaData table : LegacySchemaMigrator.LegacySchemaTables)
            systemTables = systemTables.with(table);

        LegacySchemaMigrator.LegacySchemaTables.forEach(Schema.instance::load);

        Schema.instance.setKeyspaceMetadata(systemKeyspace.withSwapped(systemTables));
    }

    private static Keyspaces keyspacesToMigrate()
    {
        Keyspaces.Builder keyspaces = Keyspaces.builder();

        // A whole bucket of shorthand
        String ks1 = KEYSPACE_PREFIX + "Keyspace1";
        String ks2 = KEYSPACE_PREFIX + "Keyspace2";
        String ks3 = KEYSPACE_PREFIX + "Keyspace3";
        String ks4 = KEYSPACE_PREFIX + "Keyspace4";
        String ks5 = KEYSPACE_PREFIX + "Keyspace5";
        String ks6 = KEYSPACE_PREFIX + "Keyspace6";
        String ks_rcs = KEYSPACE_PREFIX + "RowCacheSpace";
        String ks_nocommit = KEYSPACE_PREFIX + "NoCommitlogSpace";
        String ks_prsi = KEYSPACE_PREFIX + "PerRowSecondaryIndex";
        String ks_cql = KEYSPACE_PREFIX + "cql_keyspace";

        // Make it easy to test compaction
        Map<String, String> compactionOptions = new HashMap<>();
        compactionOptions.put("tombstone_compaction_interval", "1");

        Map<String, String> leveledOptions = new HashMap<>();
        leveledOptions.put("sstable_size_in_mb", "1");

        keyspaces.add(KeyspaceMetadata.create(ks1,
                                              KeyspaceParams.simple(1),
                                              Tables.of(SchemaLoader.standardCFMD(ks1, "Standard1")
                                                                    .compaction(CompactionParams.scts(compactionOptions)),
                                                        SchemaLoader.standardCFMD(ks1, "StandardGCGS0").gcGraceSeconds(0),
                                                        SchemaLoader.standardCFMD(ks1, "StandardLong1"),
                                                        SchemaLoader.superCFMD(ks1, "Super1", LongType.instance),
                                                        SchemaLoader.superCFMD(ks1, "Super2", UTF8Type.instance),
                                                        SchemaLoader.superCFMD(ks1, "Super5", BytesType.instance),
                                                        SchemaLoader.superCFMD(ks1, "Super6", LexicalUUIDType.instance, UTF8Type.instance),
                                                        SchemaLoader.keysIndexCFMD(ks1, "Indexed1", true),
                                                        SchemaLoader.keysIndexCFMD(ks1, "Indexed2", false),
                                                        SchemaLoader.superCFMD(ks1, "SuperDirectGC", BytesType.instance)
                                                                    .gcGraceSeconds(0),
                                                        SchemaLoader.jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance)
                                                                    .addColumnDefinition(SchemaLoader.utf8Column(ks1, "JdbcUtf8")),
                                                        SchemaLoader.jdbcCFMD(ks1, "JdbcLong", LongType.instance),
                                                        SchemaLoader.jdbcCFMD(ks1, "JdbcBytes", BytesType.instance),
                                                        SchemaLoader.jdbcCFMD(ks1, "JdbcAscii", AsciiType.instance),
                                                        SchemaLoader.standardCFMD(ks1, "StandardLeveled")
                                                                    .compaction(CompactionParams.lcs(leveledOptions)),
                                                        SchemaLoader.standardCFMD(ks1, "legacyleveled")
                                                                    .compaction(CompactionParams.lcs(leveledOptions)),
                                                        SchemaLoader.standardCFMD(ks1, "StandardLowIndexInterval")
                                                                    .minIndexInterval(8)
                                                                    .maxIndexInterval(256)
                                                                    .caching(CachingParams.CACHE_NOTHING))));

        // Keyspace 2
        keyspaces.add(KeyspaceMetadata.create(ks2,
                                              KeyspaceParams.simple(1),
                                              Tables.of(SchemaLoader.standardCFMD(ks2, "Standard1"),
                                                        SchemaLoader.superCFMD(ks2, "Super3", BytesType.instance),
                                                        SchemaLoader.superCFMD(ks2, "Super4", TimeUUIDType.instance),
                                                        SchemaLoader.keysIndexCFMD(ks2, "Indexed1", true),
                                                        SchemaLoader.compositeIndexCFMD(ks2, "Indexed2", true),
                                                        SchemaLoader.compositeIndexCFMD(ks2, "Indexed3", true)
                                                                    .gcGraceSeconds(0))));

        // Keyspace 3
        keyspaces.add(KeyspaceMetadata.create(ks3,
                                              KeyspaceParams.simple(5),
                                              Tables.of(SchemaLoader.standardCFMD(ks3, "Standard1"),
                                                        SchemaLoader.keysIndexCFMD(ks3, "Indexed1", true))));

        // Keyspace 4
        keyspaces.add(KeyspaceMetadata.create(ks4,
                                              KeyspaceParams.simple(3),
                                              Tables.of(SchemaLoader.standardCFMD(ks4, "Standard1"),
                                                        SchemaLoader.superCFMD(ks4, "Super3", BytesType.instance),
                                                        SchemaLoader.superCFMD(ks4, "Super4", TimeUUIDType.instance),
                                                        SchemaLoader.superCFMD(ks4, "Super5", TimeUUIDType.instance, BytesType.instance))));

        // Keyspace 5
        keyspaces.add(KeyspaceMetadata.create(ks5,
                                              KeyspaceParams.simple(2),
                                              Tables.of(SchemaLoader.standardCFMD(ks5, "Standard1"))));

        // Keyspace 6
        keyspaces.add(KeyspaceMetadata.create(ks6,
                                              KeyspaceParams.simple(1),
                                              Tables.of(SchemaLoader.keysIndexCFMD(ks6, "Indexed1", true))));

        // RowCacheSpace
        keyspaces.add(KeyspaceMetadata.create(ks_rcs,
                                              KeyspaceParams.simple(1),
                                              Tables.of(SchemaLoader.standardCFMD(ks_rcs, "CFWithoutCache")
                                                                    .caching(CachingParams.CACHE_NOTHING),
                                                        SchemaLoader.standardCFMD(ks_rcs, "CachedCF")
                                                                    .caching(CachingParams.CACHE_EVERYTHING),
                                                        SchemaLoader.standardCFMD(ks_rcs, "CachedIntCF")
                                                                    .caching(new CachingParams(true, 100)))));

        keyspaces.add(KeyspaceMetadata.create(ks_nocommit,
                                              KeyspaceParams.simpleTransient(1),
                                              Tables.of(SchemaLoader.standardCFMD(ks_nocommit, "Standard1"))));

        // PerRowSecondaryIndexTest
        keyspaces.add(KeyspaceMetadata.create(ks_prsi,
                                              KeyspaceParams.simple(1),
                                              Tables.of(SchemaLoader.perRowIndexedCFMD(ks_prsi, "Indexed1"))));

        // CQLKeyspace
        keyspaces.add(KeyspaceMetadata.create(ks_cql,
                                              KeyspaceParams.simple(1),
                                              Tables.of(CFMetaData.compile("CREATE TABLE table1 ("
                                                                           + "k int PRIMARY KEY,"
                                                                           + "v1 text,"
                                                                           + "v2 int"
                                                                           + ')', ks_cql),

                                                        CFMetaData.compile("CREATE TABLE table2 ("
                                                                           + "k text,"
                                                                           + "c text,"
                                                                           + "v text,"
                                                                           + "PRIMARY KEY (k, c))", ks_cql),

                                                        CFMetaData.compile("CREATE TABLE foo ("
                                                                           + "bar text, "
                                                                           + "baz text, "
                                                                           + "qux text, "
                                                                           + "PRIMARY KEY(bar, baz) ) "
                                                                           + "WITH COMPACT STORAGE", ks_cql),

                                                        CFMetaData.compile("CREATE TABLE compact_pkonly ("
                                                                           + "k int, "
                                                                           + "c int, "
                                                                           + "PRIMARY KEY (k, c)) "
                                                                           + "WITH COMPACT STORAGE",
                                                                           ks_cql),

                                                        CFMetaData.compile("CREATE TABLE foofoo ("
                                                                           + "bar text, "
                                                                           + "baz text, "
                                                                           + "qux text, "
                                                                           + "quz text, "
                                                                           + "foo text, "
                                                                           + "PRIMARY KEY((bar, baz), qux, quz) ) "
                                                                           + "WITH COMPACT STORAGE", ks_cql))));

        // NTS keyspace
        keyspaces.add(KeyspaceMetadata.create("nts", KeyspaceParams.nts("dc1", 1, "dc2", 2)));

        keyspaces.add(keyspaceWithDroppedCollections());
        keyspaces.add(keyspaceWithTriggers());
        keyspaces.add(keyspaceWithUDTs());
        keyspaces.add(keyspaceWithUDFs());
        keyspaces.add(keyspaceWithUDFsAndUDTs());
        keyspaces.add(keyspaceWithUDAs());
        keyspaces.add(keyspaceWithUDAsAndUDTs());

        return keyspaces.build();
    }

    private static KeyspaceMetadata keyspaceWithDroppedCollections()
    {
        String keyspace = KEYSPACE_PREFIX + "DroppedCollections";

        CFMetaData table =
            CFMetaData.compile("CREATE TABLE dropped_columns ("
                               + "foo text,"
                               + "bar text,"
                               + "map1 map<text, text>,"
                               + "map2 map<int, int>,"
                               + "set1 set<ascii>,"
                               + "list1 list<blob>,"
                               + "PRIMARY KEY ((foo), bar))",
                               keyspace);

        String[] collectionColumnNames = { "map1", "map2", "set1", "list1" };
        for (String name : collectionColumnNames)
        {
            ColumnDefinition column = table.getColumnDefinition(bytes(name));
            table.recordColumnDrop(column);
            table.removeColumnDefinition(column);
        }

        return KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1), Tables.of(table));
    }

    private static KeyspaceMetadata keyspaceWithTriggers()
    {
        String keyspace = KEYSPACE_PREFIX + "Triggers";

        Triggers.Builder triggers = Triggers.builder();
        CFMetaData table = SchemaLoader.standardCFMD(keyspace, "WithTriggers");
        for (int i = 0; i < 10; i++)
            triggers.add(new TriggerMetadata("trigger" + i, "DummyTrigger" + i));
        table.triggers(triggers.build());

        return KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1), Tables.of(table));
    }

    private static KeyspaceMetadata keyspaceWithUDTs()
    {
        String keyspace = KEYSPACE_PREFIX + "UDTs";

        UserType udt1 = new UserType(keyspace,
                                     bytes("udt1"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col1")); add(bytes("col2")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(UTF8Type.instance); add(Int32Type.instance); }},
                                     true);

        UserType udt2 = new UserType(keyspace,
                                     bytes("udt2"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col3")); add(bytes("col4")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(BytesType.instance); add(BooleanType.instance); }},
                                     true);

        UserType udt3 = new UserType(keyspace,
                                     bytes("udt3"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col5")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(AsciiType.instance); }},
                                     true);

        return KeyspaceMetadata.create(keyspace,
                                       KeyspaceParams.simple(1),
                                       Tables.none(),
                                       Views.none(),
                                       Types.of(udt1, udt2, udt3),
                                       Functions.none());
    }

    private static KeyspaceMetadata keyspaceWithUDFs()
    {
        String keyspace = KEYSPACE_PREFIX + "UDFs";

        UDFunction udf1 = UDFunction.create(new FunctionName(keyspace, "udf"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(BytesType.instance, Int32Type.instance),
                                            LongType.instance,
                                            false,
                                            "java",
                                            "return 42L;");

        // an overload with the same name, not a typo
        UDFunction udf2 = UDFunction.create(new FunctionName(keyspace, "udf"),
                                            ImmutableList.of(new ColumnIdentifier("col3", false), new ColumnIdentifier("col4", false)),
                                            ImmutableList.of(AsciiType.instance, LongType.instance),
                                            Int32Type.instance,
                                            true,
                                            "java",
                                            "return 42;");

        UDFunction udf3 = UDFunction.create(new FunctionName(keyspace, "udf3"),
                                            ImmutableList.of(new ColumnIdentifier("col4", false)),
                                            ImmutableList.of(UTF8Type.instance),
                                            BooleanType.instance,
                                            false,
                                            "java",
                                            "return true;");

        return KeyspaceMetadata.create(keyspace,
                                       KeyspaceParams.simple(1),
                                       Tables.none(),
                                       Views.none(),
                                       Types.none(),
                                       Functions.of(udf1, udf2, udf3));
    }

    private static KeyspaceMetadata keyspaceWithUDAs()
    {
        String keyspace = KEYSPACE_PREFIX + "UDAs";

        UDFunction udf1 = UDFunction.create(new FunctionName(keyspace, "udf1"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(Int32Type.instance, Int32Type.instance),
                                            Int32Type.instance,
                                            false,
                                            "java",
                                            "return 42;");

        UDFunction udf2 = UDFunction.create(new FunctionName(keyspace, "udf2"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(LongType.instance, Int32Type.instance),
                                            LongType.instance,
                                            false,
                                            "java",
                                            "return 42L;");

        UDFunction udf3 = UDFunction.create(new FunctionName(keyspace, "udf3"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false)),
                                            ImmutableList.of(LongType.instance),
                                            DoubleType.instance,
                                            false,
                                            "java",
                                            "return 42d;");

        Functions udfs = Functions.builder().add(udf1).add(udf2).add(udf3).build();

        UDAggregate uda1 = UDAggregate.create(udfs, new FunctionName(keyspace, "uda1"),
                                              ImmutableList.of(udf1.argTypes().get(1)),
                                              udf1.returnType(),
                                              udf1.name(),
                                              null,
                                              udf1.argTypes().get(0),
                                              null
        );

        UDAggregate uda2 = UDAggregate.create(udfs, new FunctionName(keyspace, "uda2"),
                                              ImmutableList.of(udf2.argTypes().get(1)),
                                              udf3.returnType(),
                                              udf2.name(),
                                              udf3.name(),
                                              udf2.argTypes().get(0),
                                              LongType.instance.decompose(0L)
        );

        return KeyspaceMetadata.create(keyspace,
                                       KeyspaceParams.simple(1),
                                       Tables.none(),
                                       Views.none(),
                                       Types.none(),
                                       Functions.of(udf1, udf2, udf3, uda1, uda2));
    }

    private static KeyspaceMetadata keyspaceWithUDFsAndUDTs()
    {
        String keyspace = KEYSPACE_PREFIX + "UDFUDTs";

        UserType udt1 = new UserType(keyspace,
                                     bytes("udt1"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col1")); add(bytes("col2")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(UTF8Type.instance); add(Int32Type.instance); }},
                                     true);

        UserType udt2 = new UserType(keyspace,
                                     bytes("udt2"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col1")); add(bytes("col2")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(ListType.getInstance(udt1, false)); add(Int32Type.instance); }},
                                     true);

        UDFunction udf1 = UDFunction.create(new FunctionName(keyspace, "udf"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(udt1, udt2),
                                            LongType.instance,
                                            false,
                                            "java",
                                            "return 42L;");

        // an overload with the same name, not a typo
        UDFunction udf2 = UDFunction.create(new FunctionName(keyspace, "udf"),
                                            ImmutableList.of(new ColumnIdentifier("col3", false), new ColumnIdentifier("col4", false)),
                                            ImmutableList.of(AsciiType.instance, LongType.instance),
                                            Int32Type.instance,
                                            true,
                                            "java",
                                            "return 42;");

        UDFunction udf3 = UDFunction.create(new FunctionName(keyspace, "udf3"),
                                            ImmutableList.of(new ColumnIdentifier("col4", false)),
                                            ImmutableList.of(new TupleType(Arrays.asList(udt1, udt2))),
                                            BooleanType.instance,
                                            false,
                                            "java",
                                            "return true;");

        return KeyspaceMetadata.create(keyspace,
                                       KeyspaceParams.simple(1),
                                       Tables.none(),
                                       Views.none(),
                                       Types.of(udt1, udt2),
                                       Functions.of(udf1, udf2, udf3));
    }

    private static KeyspaceMetadata keyspaceWithUDAsAndUDTs()
    {
        String keyspace = KEYSPACE_PREFIX + "UDAUDTs";

        UserType udt1 = new UserType(keyspace,
                                     bytes("udt1"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col1")); add(bytes("col2")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(UTF8Type.instance); add(Int32Type.instance); }},
                                     true);

        UserType udt2 = new UserType(keyspace,
                                     bytes("udt2"),
                                     new ArrayList<ByteBuffer>() {{ add(bytes("col1")); add(bytes("col2")); }},
                                     new ArrayList<AbstractType<?>>() {{ add(ListType.getInstance(udt1, false)); add(Int32Type.instance); }},
                                     true);

        UDFunction udf1 = UDFunction.create(new FunctionName(keyspace, "udf1"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(udt1, udt2),
                                            udt1,
                                            false,
                                            "java",
                                            "return null;");

        UDFunction udf2 = UDFunction.create(new FunctionName(keyspace, "udf2"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false), new ColumnIdentifier("col2", false)),
                                            ImmutableList.of(udt2, udt1),
                                            udt2,
                                            false,
                                            "java",
                                            "return null;");

        UDFunction udf3 = UDFunction.create(new FunctionName(keyspace, "udf3"),
                                            ImmutableList.of(new ColumnIdentifier("col1", false)),
                                            ImmutableList.of(udt2),
                                            DoubleType.instance,
                                            false,
                                            "java",
                                            "return 42d;");

        Functions udfs = Functions.builder().add(udf1).add(udf2).add(udf3).build();

        UDAggregate uda1 = UDAggregate.create(udfs, new FunctionName(keyspace, "uda1"),
                                              ImmutableList.of(udf1.argTypes().get(1)),
                                              udf1.returnType(),
                                              udf1.name(),
                                              null,
                                              udf1.argTypes().get(0),
                                              null
        );

        ByteBuffer twoNullEntries = ByteBuffer.allocate(8);
        twoNullEntries.putInt(-1);
        twoNullEntries.putInt(-1);
        twoNullEntries.flip();
        UDAggregate uda2 = UDAggregate.create(udfs, new FunctionName(keyspace, "uda2"),
                                              ImmutableList.of(udf2.argTypes().get(1)),
                                              udf3.returnType(),
                                              udf2.name(),
                                              udf3.name(),
                                              udf2.argTypes().get(0),
                                              twoNullEntries
        );

        return KeyspaceMetadata.create(keyspace,
                                       KeyspaceParams.simple(1),
                                       Tables.none(),
                                       Views.none(),
                                       Types.of(udt1, udt2),
                                       Functions.of(udf1, udf2, udf3, uda1, uda2));
    }

    /*
     * Serializing keyspaces
     */

    private static void legacySerializeKeyspace(KeyspaceMetadata keyspace)
    {
        makeLegacyCreateKeyspaceMutation(keyspace, TIMESTAMP).apply();
        setLegacyIndexStatus(keyspace);
    }

    private static Mutation makeLegacyCreateKeyspaceMutation(KeyspaceMetadata keyspace, long timestamp)
    {
        // Note that because Keyspaces is a COMPACT TABLE, we're really only setting static columns internally and shouldn't set any clustering.
        RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyKeyspaces, timestamp, keyspace.name);

        adder.add("durable_writes", keyspace.params.durableWrites)
             .add("strategy_class", keyspace.params.replication.klass.getName())
             .add("strategy_options", json(keyspace.params.replication.options));

        Mutation mutation = adder.build();

        keyspace.tables.forEach(table -> addTableToSchemaMutation(table, timestamp, true, mutation));
        keyspace.types.forEach(type -> addTypeToSchemaMutation(type, timestamp, mutation));
        keyspace.functions.udfs().forEach(udf -> addFunctionToSchemaMutation(udf, timestamp, mutation));
        keyspace.functions.udas().forEach(uda -> addAggregateToSchemaMutation(uda, timestamp, mutation));

        return mutation;
    }

    /*
     * Serializing tables
     */

    private static void addTableToSchemaMutation(CFMetaData table, long timestamp, boolean withColumnsAndTriggers, Mutation mutation)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyColumnfamilies, timestamp, mutation)
                                 .clustering(table.cfName);

        adder.add("cf_id", table.cfId)
             .add("type", table.isSuper() ? "Super" : "Standard");

        if (table.isSuper())
        {
            adder.add("comparator", table.comparator.subtype(0).toString())
                 .add("subcomparator", ((MapType)table.compactValueColumn().type).getKeysType().toString());
        }
        else
        {
            adder.add("comparator", LegacyLayout.makeLegacyComparator(table).toString());
        }

        adder.add("bloom_filter_fp_chance", table.params.bloomFilterFpChance)
             .add("caching", cachingToString(table.params.caching))
             .add("comment", table.params.comment)
             .add("compaction_strategy_class", table.params.compaction.klass().getName())
             .add("compaction_strategy_options", json(table.params.compaction.options()))
             .add("compression_parameters", json(ThriftConversion.compressionParametersToThrift(table.params.compression)))
             .add("default_time_to_live", table.params.defaultTimeToLive)
             .add("gc_grace_seconds", table.params.gcGraceSeconds)
             .add("key_validator", table.getKeyValidator().toString())
             .add("local_read_repair_chance", table.params.dcLocalReadRepairChance)
             .add("max_compaction_threshold", table.params.compaction.maxCompactionThreshold())
             .add("max_index_interval", table.params.maxIndexInterval)
             .add("memtable_flush_period_in_ms", table.params.memtableFlushPeriodInMs)
             .add("min_compaction_threshold", table.params.compaction.minCompactionThreshold())
             .add("min_index_interval", table.params.minIndexInterval)
             .add("read_repair_chance", table.params.readRepairChance)
             .add("speculative_retry", table.params.speculativeRetry.toString());

        for (Map.Entry<ByteBuffer, CFMetaData.DroppedColumn> entry : table.getDroppedColumns().entrySet())
        {
            String name = UTF8Type.instance.getString(entry.getKey());
            CFMetaData.DroppedColumn column = entry.getValue();
            adder.addMapEntry("dropped_columns", name, column.droppedTime);
        }

        adder.add("is_dense", table.isDense());

        adder.add("default_validator", table.makeLegacyDefaultValidator().toString());

        if (withColumnsAndTriggers)
        {
            for (ColumnDefinition column : table.allColumns())
                addColumnToSchemaMutation(table, column, timestamp, mutation);

            for (TriggerMetadata trigger : table.getTriggers())
                addTriggerToSchemaMutation(table, trigger, timestamp, mutation);
        }

        adder.build();
    }

    private static String cachingToString(CachingParams caching)
    {
        return format("{\"keys\":\"%s\", \"rows_per_partition\":\"%s\"}",
                      caching.keysAsString(),
                      caching.rowsPerPartitionAsString());
    }

    private static void addColumnToSchemaMutation(CFMetaData table, ColumnDefinition column, long timestamp, Mutation mutation)
    {
        // We need to special case pk-only dense tables. See CASSANDRA-9874.
        String name = table.isDense() && column.kind == ColumnDefinition.Kind.REGULAR && column.type instanceof EmptyType
                    ? ""
                    : column.name.toString();

        final RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyColumns, timestamp, mutation).clustering(table.cfName, name);

        adder.add("validator", column.type.toString())
             .add("type", serializeKind(column.kind, table.isDense()))
             .add("component_index", column.position());

        Optional<IndexMetadata> index = findIndexForColumn(table.getIndexes(), table, column);
        if (index.isPresent())
        {
            IndexMetadata i = index.get();
            adder.add("index_name", i.name);
            adder.add("index_type", i.kind.toString());
            adder.add("index_options", json(i.options));
        }
        else
        {
            adder.add("index_name", null);
            adder.add("index_type", null);
            adder.add("index_options", null);
        }

        adder.build();
    }

    private static Optional<IndexMetadata> findIndexForColumn(Indexes indexes,
                                                              CFMetaData table,
                                                              ColumnDefinition column)
    {
        // makes the assumptions that the string option denoting the
        // index targets can be parsed by CassandraIndex.parseTarget
        // which should be true for any pre-3.0 index
        for (IndexMetadata index : indexes)
          if (TargetParser.parse(table, index).left.equals(column))
                return Optional.of(index);

        return Optional.empty();
    }

    private static String serializeKind(ColumnDefinition.Kind kind, boolean isDense)
    {
        // For backward compatibility, we special case CLUSTERING and the case where the table is dense.
        if (kind == ColumnDefinition.Kind.CLUSTERING)
            return "clustering_key";

        if (kind == ColumnDefinition.Kind.REGULAR && isDense)
            return "compact_value";

        return kind.toString().toLowerCase();
    }

    private static void addTriggerToSchemaMutation(CFMetaData table, TriggerMetadata trigger, long timestamp, Mutation mutation)
    {
        new RowUpdateBuilder(SystemKeyspace.LegacyTriggers, timestamp, mutation)
            .clustering(table.cfName, trigger.name)
            .addMapEntry("trigger_options", "class", trigger.classOption)
            .build();
    }

    /*
     * Serializing types
     */

    private static void addTypeToSchemaMutation(UserType type, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyUsertypes, timestamp, mutation)
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

    /*
     * Serializing functions
     */

    private static void addFunctionToSchemaMutation(UDFunction function, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyFunctions, timestamp, mutation)
                                 .clustering(function.name().name, functionSignatureWithTypes(function));

        adder.add("body", function.body())
             .add("language", function.language())
             .add("return_type", function.returnType().toString())
             .add("called_on_null_input", function.isCalledOnNullInput());

        adder.resetCollection("argument_names")
             .resetCollection("argument_types");

        for (int i = 0; i < function.argNames().size(); i++)
        {
            adder.addListEntry("argument_names", function.argNames().get(i).bytes)
                 .addListEntry("argument_types", function.argTypes().get(i).toString());
        }

        adder.build();
    }

    /*
     * Serializing aggregates
     */

    private static void addAggregateToSchemaMutation(UDAggregate aggregate, long timestamp, Mutation mutation)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(SystemKeyspace.LegacyAggregates, timestamp, mutation)
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

    // We allow method overloads, so a function is not uniquely identified by its name only, but
    // also by its argument types. To distinguish overloads of given function name in the schema
    // we use a "signature" which is just a list of it's CQL argument types.
    public static ByteBuffer functionSignatureWithTypes(AbstractFunction fun)
    {
        List<String> arguments =
            fun.argTypes()
               .stream()
               .map(argType -> argType.asCQL3Type().toString())
               .collect(Collectors.toList());

        return ListType.getInstance(UTF8Type.instance, false).decompose(arguments);
    }

    private static void setLegacyIndexStatus(KeyspaceMetadata keyspace)
    {
        keyspace.tables.forEach(LegacySchemaMigratorTest::setLegacyIndexStatus);
    }

    private static void setLegacyIndexStatus(CFMetaData table)
    {
        table.getIndexes().forEach((index) -> setLegacyIndexStatus(table.ksName, table.cfName, index));
    }

    private static void setLegacyIndexStatus(String keyspace, String table, IndexMetadata index)
    {
        SystemKeyspace.setIndexBuilt(keyspace, table + '.' + index.name);
    }

    private static void verifyIndexBuildStatus(KeyspaceMetadata keyspace)
    {
        keyspace.tables.forEach(LegacySchemaMigratorTest::verifyIndexBuildStatus);
    }

    private static void verifyIndexBuildStatus(CFMetaData table)
    {
        table.getIndexes().forEach(index -> verifyIndexBuildStatus(table.ksName, table.cfName, index));
    }

    private static void verifyIndexBuildStatus(String keyspace, String table, IndexMetadata index)
    {
        assertFalse(SystemKeyspace.isIndexBuilt(keyspace, table + '.' + index.name));
        assertTrue(SystemKeyspace.isIndexBuilt(keyspace, index.name));
    }

}
