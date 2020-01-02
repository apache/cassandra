/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SchemaLoader
{
    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        prepareServer();

        // Migrations aren't happy if gossiper is not started.  Even if we don't use migrations though,
        // some tests now expect us to start gossip for them.
        startGossiper();
    }

    @After
    public void leakDetect() throws InterruptedException
    {
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(10);
    }

    public static void prepareServer()
    {
       CQLTester.prepareServer();
    }

    public static void startGossiper()
    {
        // skip shadow round and endpoint collision check in tests
        System.setProperty("cassandra.allow_unsafe_join", "true");
        if (!Gossiper.instance.isEnabled())
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
    }

    public static void schemaDefinition(String testName) throws ConfigurationException
    {
        List<KeyspaceMetadata> schema = new ArrayList<KeyspaceMetadata>();

        // A whole bucket of shorthand
        String ks1 = testName + "Keyspace1";
        String ks2 = testName + "Keyspace2";
        String ks3 = testName + "Keyspace3";
        String ks4 = testName + "Keyspace4";
        String ks5 = testName + "Keyspace5";
        String ks6 = testName + "Keyspace6";
        String ks7 = testName + "Keyspace7";
        String ks_kcs = testName + "KeyCacheSpace";
        String ks_rcs = testName + "RowCacheSpace";
        String ks_ccs = testName + "CounterCacheSpace";
        String ks_nocommit = testName + "NoCommitlogSpace";
        String ks_prsi = testName + "PerRowSecondaryIndex";
        String ks_cql = testName + "cql_keyspace";

        AbstractType bytes = BytesType.instance;

        AbstractType<?> composite = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, TimeUUIDType.instance, IntegerType.instance}));
        AbstractType<?> compositeMaxMin = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, IntegerType.instance}));
        Map<Byte, AbstractType<?>> aliases = new HashMap<Byte, AbstractType<?>>();
        aliases.put((byte)'b', BytesType.instance);
        aliases.put((byte)'t', TimeUUIDType.instance);
        aliases.put((byte)'B', ReversedType.getInstance(BytesType.instance));
        aliases.put((byte)'T', ReversedType.getInstance(TimeUUIDType.instance));
        AbstractType<?> dynamicComposite = DynamicCompositeType.getInstance(aliases);

        // Make it easy to test compaction
        Map<String, String> compactionOptions = new HashMap<String, String>();
        compactionOptions.put("tombstone_compaction_interval", "1");
        Map<String, String> leveledOptions = new HashMap<String, String>();
        leveledOptions.put("sstable_size_in_mb", "1");
        leveledOptions.put("fanout_size", "5");

        // Keyspace 1
        schema.add(KeyspaceMetadata.create(ks1,
                KeyspaceParams.simple(1),
                Tables.of(
                // Column Families
                standardCFMD(ks1, "Standard1").compaction(CompactionParams.scts(compactionOptions)),
                standardCFMD(ks1, "Standard2"),
                standardCFMD(ks1, "Standard3"),
                standardCFMD(ks1, "Standard4"),
                standardCFMD(ks1, "StandardGCGS0").gcGraceSeconds(0),
                standardCFMD(ks1, "StandardLong1"),
                standardCFMD(ks1, "StandardLong2"),
                //CFMetaData.Builder.create(ks1, "ValuesWithQuotes").build(),
                superCFMD(ks1, "Super1", LongType.instance),
                superCFMD(ks1, "Super2", LongType.instance),
                superCFMD(ks1, "Super3", LongType.instance),
                superCFMD(ks1, "Super4", UTF8Type.instance),
                superCFMD(ks1, "Super5", bytes),
                superCFMD(ks1, "Super6", LexicalUUIDType.instance, UTF8Type.instance),
                keysIndexCFMD(ks1, "Indexed1", true),
                keysIndexCFMD(ks1, "Indexed2", false),
                //CFMetaData.Builder.create(ks1, "StandardInteger1").withColumnNameComparator(IntegerType.instance).build(),
                //CFMetaData.Builder.create(ks1, "StandardLong3").withColumnNameComparator(IntegerType.instance).build(),
                //CFMetaData.Builder.create(ks1, "Counter1", false, false, true).build(),
                //CFMetaData.Builder.create(ks1, "SuperCounter1", false, false, true, true).build(),
                superCFMD(ks1, "SuperDirectGC", BytesType.instance).gcGraceSeconds(0),
//                jdbcCFMD(ks1, "JdbcInteger", IntegerType.instance).addColumnDefinition(integerColumn(ks1, "JdbcInteger")),
                jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance).addColumnDefinition(utf8Column(ks1, "JdbcUtf8")),
                jdbcCFMD(ks1, "JdbcLong", LongType.instance),
                jdbcCFMD(ks1, "JdbcBytes", bytes),
                jdbcCFMD(ks1, "JdbcAscii", AsciiType.instance),
                //CFMetaData.Builder.create(ks1, "StandardComposite", false, true, false).withColumnNameComparator(composite).build(),
                //CFMetaData.Builder.create(ks1, "StandardComposite2", false, true, false).withColumnNameComparator(compositeMaxMin).build(),
                //CFMetaData.Builder.create(ks1, "StandardDynamicComposite", false, true, false).withColumnNameComparator(dynamicComposite).build(),
                standardCFMD(ks1, "StandardLeveled").compaction(CompactionParams.lcs(leveledOptions)),
                standardCFMD(ks1, "legacyleveled").compaction(CompactionParams.lcs(leveledOptions)),
                standardCFMD(ks1, "StandardLowIndexInterval").minIndexInterval(8)
                                                             .maxIndexInterval(256)
                                                             .caching(CachingParams.CACHE_NOTHING)
                //CFMetaData.Builder.create(ks1, "UUIDKeys").addPartitionKey("key",UUIDType.instance).build(),
                //CFMetaData.Builder.create(ks1, "MixedTypes").withColumnNameComparator(LongType.instance).addPartitionKey("key", UUIDType.instance).build(),
                //CFMetaData.Builder.create(ks1, "MixedTypesComposite", false, true, false).withColumnNameComparator(composite).addPartitionKey("key", composite).build(),
                //CFMetaData.Builder.create(ks1, "AsciiKeys").addPartitionKey("key", AsciiType.instance).build()
        )));

        // Keyspace 2
        schema.add(KeyspaceMetadata.create(ks2,
                KeyspaceParams.simple(1),
                Tables.of(
                // Column Families
                standardCFMD(ks2, "Standard1"),
                standardCFMD(ks2, "Standard3"),
                superCFMD(ks2, "Super3", bytes),
                superCFMD(ks2, "Super4", TimeUUIDType.instance),
                keysIndexCFMD(ks2, "Indexed1", true),
                compositeIndexCFMD(ks2, "Indexed2", true),
                compositeIndexCFMD(ks2, "Indexed3", true).gcGraceSeconds(0))));

        // Keyspace 3
        schema.add(KeyspaceMetadata.create(ks3,
                KeyspaceParams.simple(5),
                Tables.of(
                standardCFMD(ks3, "Standard1"),
                keysIndexCFMD(ks3, "Indexed1", true))));

        // Keyspace 4
        schema.add(KeyspaceMetadata.create(ks4,
                KeyspaceParams.simple(3),
                Tables.of(
                standardCFMD(ks4, "Standard1"),
                standardCFMD(ks4, "Standard3"),
                superCFMD(ks4, "Super3", bytes),
                superCFMD(ks4, "Super4", TimeUUIDType.instance),
                superCFMD(ks4, "Super5", TimeUUIDType.instance, BytesType.instance))));

        // Keyspace 5
        schema.add(KeyspaceMetadata.create(ks5,
                KeyspaceParams.simple(2),
                Tables.of(standardCFMD(ks5, "Standard1"))));

        // Keyspace 6
        schema.add(KeyspaceMetadata.create(ks6,
                KeyspaceParams.simple(1),
                Tables.of(keysIndexCFMD(ks6, "Indexed1", true))));

        // Keyspace 7
        schema.add(KeyspaceMetadata.create(ks7,
                KeyspaceParams.simple(1),
                Tables.of(customIndexCFMD(ks7, "Indexed1"))));

        // KeyCacheSpace
        schema.add(KeyspaceMetadata.create(ks_kcs,
                KeyspaceParams.simple(1),
                Tables.of(
                standardCFMD(ks_kcs, "Standard1"),
                standardCFMD(ks_kcs, "Standard2"),
                standardCFMD(ks_kcs, "Standard3"))));

        // RowCacheSpace
        schema.add(KeyspaceMetadata.create(ks_rcs,
                KeyspaceParams.simple(1),
                Tables.of(
                standardCFMD(ks_rcs, "CFWithoutCache").caching(CachingParams.CACHE_NOTHING),
                standardCFMD(ks_rcs, "CachedCF").caching(CachingParams.CACHE_EVERYTHING),
                standardCFMD(ks_rcs, "CachedNoClustering", 1, IntegerType.instance, IntegerType.instance, null).caching(CachingParams.CACHE_EVERYTHING),
                standardCFMD(ks_rcs, "CachedIntCF").
                        caching(new CachingParams(true, 100)))));

        // CounterCacheSpace
        /*schema.add(KeyspaceMetadata.testMetadata(ks_ccs,
                simple,
                opts_rf1,
                CFMetaData.Builder.create(ks_ccs, "Counter1", false, false, true).build(),
                CFMetaData.Builder.create(ks_ccs, "Counter1", false, false, true).build()));*/

        schema.add(KeyspaceMetadata.create(ks_nocommit, KeyspaceParams.simpleTransient(1), Tables.of(
                standardCFMD(ks_nocommit, "Standard1"))));

        // CQLKeyspace
        schema.add(KeyspaceMetadata.create(ks_cql, KeyspaceParams.simple(1), Tables.of(

                // Column Families
                CFMetaData.compile("CREATE TABLE table1 ("
                        + "k int PRIMARY KEY,"
                        + "v1 text,"
                        + "v2 int"
                        + ")", ks_cql),

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
                CFMetaData.compile("CREATE TABLE foofoo ("
                        + "bar text, "
                        + "baz text, "
                        + "qux text, "
                        + "quz text, "
                        + "foo text, "
                        + "PRIMARY KEY((bar, baz), qux, quz) ) "
                        + "WITH COMPACT STORAGE", ks_cql)
        )));

        if (DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner)
            schema.add(KeyspaceMetadata.create("sasi", KeyspaceParams.simpleTransient(1), Tables.of(sasiCFMD("sasi", "test_cf"), clusteringSASICFMD("sasi", "clustering_test_cf"))));

        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            useCompression(schema);

        // if you're messing with low-level sstable stuff, it can be useful to inject the schema directly
        // Schema.instance.load(schemaDefinition());
        for (KeyspaceMetadata ksm : schema)
            MigrationManager.announceNewKeyspace(ksm, false);
    }

    public static void createKeyspace(String name, KeyspaceParams params, CFMetaData... tables)
    {
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, Tables.of(tables)), true);
    }

    public static void createKeyspace(String name, KeyspaceParams params, Tables tables, Types types)
    {
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, tables, Views.none(), types, Functions.none()), true);
    }

    public static ColumnDefinition integerColumn(String ksName, String cfName)
    {
        return new ColumnDefinition(ksName,
                                    cfName,
                                    ColumnIdentifier.getInterned(IntegerType.instance.fromString("42"), IntegerType.instance),
                                    UTF8Type.instance,
                                    ColumnDefinition.NO_POSITION,
                                    ColumnDefinition.Kind.REGULAR);
    }

    public static ColumnDefinition utf8Column(String ksName, String cfName)
    {
        return new ColumnDefinition(ksName,
                                    cfName,
                                    ColumnIdentifier.getInterned("fortytwo", true),
                                    UTF8Type.instance,
                                    ColumnDefinition.NO_POSITION,
                                    ColumnDefinition.Kind.REGULAR);
    }

    public static CFMetaData perRowIndexedCFMD(String ksName, String cfName)
    {
        final Map<String, String> indexOptions = Collections.singletonMap(
                                                      IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                      StubIndex.class.getName());

        CFMetaData cfm =  CFMetaData.Builder.create(ksName, cfName)
                .addPartitionKey("key", AsciiType.instance)
                .build();

        ColumnDefinition indexedColumn = ColumnDefinition.regularDef(ksName, cfName, "indexed", AsciiType.instance);
        cfm.addOrReplaceColumnDefinition(indexedColumn);

        cfm.indexes(
            cfm.getIndexes()
               .with(IndexMetadata.fromIndexTargets(cfm,
                                                    Collections.singletonList(new IndexTarget(indexedColumn.name,
                                                                                              IndexTarget.Type.VALUES)),
                                                    "indexe1",
                                                    IndexMetadata.Kind.CUSTOM,
                                                    indexOptions)));
        return cfm;
    }

    private static void useCompression(List<KeyspaceMetadata> schema)
    {
        for (KeyspaceMetadata ksm : schema)
            for (CFMetaData cfm : ksm.tablesAndViews())
                cfm.compression(CompressionParams.snappy());
    }

    public static CFMetaData counterCFMD(String ksName, String cfName)
    {
        return CFMetaData.Builder.create(ksName, cfName, false, true, true)
                .addPartitionKey("key", AsciiType.instance)
                .addClusteringColumn("name", AsciiType.instance)
                .addRegularColumn("val", CounterColumnType.instance)
                .addRegularColumn("val2", CounterColumnType.instance)
                .build()
                .compression(getCompressionParameters());
    }

    public static CFMetaData standardCFMD(String ksName, String cfName)
    {
        return standardCFMD(ksName, cfName, 1, AsciiType.instance);
    }

    public static CFMetaData standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, AsciiType.instance);
    }

    public static CFMetaData standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, valType, AsciiType.instance);
    }

    public static CFMetaData standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType, AbstractType<?> clusteringType)
    {
        CFMetaData.Builder builder;
        builder = CFMetaData.Builder.create(ksName, cfName)
                                    .addPartitionKey("key", keyType)
                                    .addRegularColumn("val", valType);

        if(clusteringType != null)
            builder = builder.addClusteringColumn("name", clusteringType);

        for (int i = 0; i < columnCount; i++)
            builder.addRegularColumn("val" + i, AsciiType.instance);

        return builder.build()
                      .compression(getCompressionParameters());
    }

    public static CFMetaData staticCFMD(String ksName, String cfName)
    {
        return CFMetaData.Builder.create(ksName, cfName)
                                 .addPartitionKey("key", AsciiType.instance)
                                 .addClusteringColumn("cols", AsciiType.instance)
                                 .addStaticColumn("val", AsciiType.instance)
                                 .addRegularColumn("val2", AsciiType.instance)
                                 .build();
    }


    public static CFMetaData denseCFMD(String ksName, String cfName)
    {
        return denseCFMD(ksName, cfName, AsciiType.instance);
    }
    public static CFMetaData denseCFMD(String ksName, String cfName, AbstractType cc)
    {
        return denseCFMD(ksName, cfName, cc, null);
    }
    public static CFMetaData denseCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        AbstractType comp = cc;
        if (subcc != null)
            comp = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{cc, subcc}));

        return CFMetaData.Builder.createDense(ksName, cfName, subcc != null, false)
            .addPartitionKey("key", AsciiType.instance)
            .addClusteringColumn("cols", comp)
            .addRegularColumn("val", AsciiType.instance)
            .build()
            .compression(getCompressionParameters());
    }

    public static CFMetaData superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, BytesType.instance, subcc);
    }

    public static CFMetaData superCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        return CFMetaData.Builder.createSuper(ksName, cfName, false)
                                 .addPartitionKey("key", BytesType.instance)
                                 .addClusteringColumn("column1", cc)
                                 .addRegularColumn("", MapType.getInstance(AsciiType.instance, subcc, true))
                                 .build();

    }
    public static CFMetaData compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex) throws ConfigurationException
    {
        return compositeIndexCFMD(ksName, cfName, withRegularIndex, false);
    }

    public static CFMetaData compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex, boolean withStaticIndex) throws ConfigurationException
    {
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                .addPartitionKey("key", AsciiType.instance)
                .addClusteringColumn("c1", AsciiType.instance)
                .addRegularColumn("birthdate", LongType.instance)
                .addRegularColumn("notbirthdate", LongType.instance)
                .addStaticColumn("static", LongType.instance)
                .build();

        if (withRegularIndex)
        {
            cfm.indexes(
                cfm.getIndexes()
                   .with(IndexMetadata.fromIndexTargets(cfm,
                                                        Collections.singletonList(
                                                            new IndexTarget(new ColumnIdentifier("birthdate", true),
                                                                            IndexTarget.Type.VALUES)),
                                                        "birthdate_key_index",
                                                        IndexMetadata.Kind.COMPOSITES,
                                                        Collections.EMPTY_MAP)));
        }

        if (withStaticIndex)
        {
            cfm.indexes(
                    cfm.getIndexes()
                       .with(IndexMetadata.fromIndexTargets(cfm,
                                                            Collections.singletonList(
                                                                new IndexTarget(new ColumnIdentifier("static", true),
                                                                                IndexTarget.Type.VALUES)),
                                                            "static_index",
                                                            IndexMetadata.Kind.COMPOSITES,
                                                            Collections.EMPTY_MAP)));
        }

        return cfm.compression(getCompressionParameters());
    }

    public static CFMetaData compositeMultipleIndexCFMD(String ksName, String cfName) throws ConfigurationException
    {
        // the withIndex flag exists to allow tests index creation
        // on existing columns
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                                           .addPartitionKey("key", AsciiType.instance)
                                           .addClusteringColumn("c1", AsciiType.instance)
                                           .addRegularColumn("birthdate", LongType.instance)
                                           .addRegularColumn("notbirthdate", LongType.instance)
                                           .build();

        cfm.indexes(
        cfm.getIndexes()
           .with(IndexMetadata.fromIndexTargets(cfm,
                                                Collections.singletonList(
                                                new IndexTarget(new ColumnIdentifier("birthdate", true),
                                                                IndexTarget.Type.VALUES)),
                                                "birthdate_key_index",
                                                IndexMetadata.Kind.COMPOSITES,
                                                Collections.EMPTY_MAP))
           .with(IndexMetadata.fromIndexTargets(cfm,
                                                Collections.singletonList(
                                                new IndexTarget(new ColumnIdentifier("notbirthdate", true),
                                                                IndexTarget.Type.VALUES)),
                                                "notbirthdate_key_index",
                                                IndexMetadata.Kind.COMPOSITES,
                                                Collections.EMPTY_MAP))
        );


        return cfm.compression(getCompressionParameters());
    }

    public static CFMetaData keysIndexCFMD(String ksName, String cfName, boolean withIndex) throws ConfigurationException
    {
        CFMetaData cfm = CFMetaData.Builder.createDense(ksName, cfName, false, false)
                                           .addPartitionKey("key", AsciiType.instance)
                                           .addClusteringColumn("c1", AsciiType.instance)
                                           .addStaticColumn("birthdate", LongType.instance)
                                           .addStaticColumn("notbirthdate", LongType.instance)
                                           .addRegularColumn("value", LongType.instance)
                                           .build();

        if (withIndex)
            cfm.indexes(
                cfm.getIndexes()
                   .with(IndexMetadata.fromIndexTargets(cfm,
                                                        Collections.singletonList(
                                                            new IndexTarget(new ColumnIdentifier("birthdate", true),
                                                                            IndexTarget.Type.VALUES)),
                                                         "birthdate_composite_index",
                                                         IndexMetadata.Kind.KEYS,
                                                         Collections.EMPTY_MAP)));


        return cfm.compression(getCompressionParameters());
    }

    public static CFMetaData customIndexCFMD(String ksName, String cfName) throws ConfigurationException
    {
        CFMetaData cfm = CFMetaData.Builder.createDense(ksName, cfName, false, false)
                                           .addPartitionKey("key", AsciiType.instance)
                                           .addClusteringColumn("c1", AsciiType.instance)
                                           .addRegularColumn("value", LongType.instance)
                                           .build();

            cfm.indexes(
                cfm.getIndexes()
                .with(IndexMetadata.fromIndexTargets(cfm,
                                                     Collections.singletonList(
                                                             new IndexTarget(new ColumnIdentifier("value", true),
                                                                             IndexTarget.Type.VALUES)),
                                                     "value_index",
                                                     IndexMetadata.Kind.CUSTOM,
                                                     Collections.singletonMap(
                                                             IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                             StubIndex.class.getName()))));


        return cfm.compression(getCompressionParameters());
    }

    public static CFMetaData jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return CFMetaData.Builder.create(ksName, cfName).addPartitionKey("key", BytesType.instance)
                                                        .build()
                                                        .compression(getCompressionParameters());
    }

    public static CFMetaData sasiCFMD(String ksName, String cfName)
    {
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                                           .addPartitionKey("id", UTF8Type.instance)
                                           .addRegularColumn("first_name", UTF8Type.instance)
                                           .addRegularColumn("last_name", UTF8Type.instance)
                                           .addRegularColumn("age", Int32Type.instance)
                                           .addRegularColumn("height", Int32Type.instance)
                                           .addRegularColumn("timestamp", LongType.instance)
                                           .addRegularColumn("address", UTF8Type.instance)
                                           .addRegularColumn("score", DoubleType.instance)
                                           .addRegularColumn("comment", UTF8Type.instance)
                                           .addRegularColumn("comment_suffix_split", UTF8Type.instance)
                                           .addRegularColumn("/output/full-name/", UTF8Type.instance)
                                           .addRegularColumn("/data/output/id", UTF8Type.instance)
                                           .addRegularColumn("first_name_prefix", UTF8Type.instance)
                                           .build();

        cfm.indexes(cfm.getIndexes()
                        .with(IndexMetadata.fromSchemaMetadata("first_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "first_name");
                            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("last_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "last_name");
                            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("age", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "age");

                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("timestamp", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "timestamp");
                            put("mode", OnDiskIndexBuilder.Mode.SPARSE.toString());

                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("address", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "address");
                            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
                            put("case_sensitive", "false");
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("score", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "score");
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("comment", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "comment");
                            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                            put("analyzed", "true");
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("comment_suffix_split", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "comment_suffix_split");
                            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                            put("analyzed", "false");
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("output_full_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "/output/full-name/");
                            put("analyzed", "true");
                            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
                            put("case_sensitive", "false");
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("data_output_id", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "/data/output/id");
                            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                        }}))
                        .with(IndexMetadata.fromSchemaMetadata("first_name_prefix", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                        {{
                            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                            put(IndexTarget.TARGET_OPTION_NAME, "first_name_prefix");
                            put("analyzed", "true");
                            put("tokenization_normalize_lowercase", "true");
                        }})));

        return cfm;
    }

    public static CFMetaData clusteringSASICFMD(String ksName, String cfName)
    {
        return clusteringSASICFMD(ksName, cfName, "location", "age", "height", "score");
    }

    public static CFMetaData clusteringSASICFMD(String ksName, String cfName, String...indexedColumns)
    {
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                                           .addPartitionKey("name", UTF8Type.instance)
                                           .addClusteringColumn("location", UTF8Type.instance)
                                           .addClusteringColumn("age", Int32Type.instance)
                                           .addRegularColumn("height", Int32Type.instance)
                                           .addRegularColumn("score", DoubleType.instance)
                                           .addStaticColumn("nickname", UTF8Type.instance)
                                           .build();

        Indexes indexes = cfm.getIndexes();
        for (String indexedColumn : indexedColumns)
        {
            indexes = indexes.with(IndexMetadata.fromSchemaMetadata(indexedColumn, IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
            {{
                put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                put(IndexTarget.TARGET_OPTION_NAME, indexedColumn);
                put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
            }}));
        }
        cfm.indexes(indexes);
        return cfm;
    }

    public static CFMetaData staticSASICFMD(String ksName, String cfName)
    {
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                                           .addPartitionKey("sensor_id", Int32Type.instance)
                                           .addStaticColumn("sensor_type", UTF8Type.instance)
                                           .addClusteringColumn("date", LongType.instance)
                                           .addRegularColumn("value", DoubleType.instance)
                                           .addRegularColumn("variance", Int32Type.instance)
                                           .build();

        Indexes indexes = cfm.getIndexes();
        indexes = indexes.with(IndexMetadata.fromSchemaMetadata("sensor_type", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "sensor_type");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
            put("case_sensitive", "false");
        }}));

        indexes = indexes.with(IndexMetadata.fromSchemaMetadata("value", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "value");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
        }}));

        indexes = indexes.with(IndexMetadata.fromSchemaMetadata("variance", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "variance");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
        }}));

        cfm.indexes(indexes);
        return cfm;
    }

    public static CFMetaData fullTextSearchSASICFMD(String ksName, String cfName)
    {
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                                           .addPartitionKey("song_id", UUIDType.instance)
                                           .addRegularColumn("title", UTF8Type.instance)
                                           .addRegularColumn("artist", UTF8Type.instance)
                                           .build();

        Indexes indexes = cfm.getIndexes();
        indexes = indexes.with(IndexMetadata.fromSchemaMetadata("title", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "title");
            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer");
            put("tokenization_enable_stemming", "true");
            put("tokenization_locale", "en");
            put("tokenization_skip_stop_words", "true");
            put("tokenization_normalize_lowercase", "true");
        }}));

        indexes = indexes.with(IndexMetadata.fromSchemaMetadata("artist", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "artist");
            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
            put("case_sensitive", "false");

        }}));

        cfm.indexes(indexes);
        return cfm;
    }

    public static CompressionParams getCompressionParameters()
    {
        return getCompressionParameters(null);
    }

    public static CompressionParams getCompressionParameters(Integer chunkSize)
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            return CompressionParams.snappy(chunkSize);

        return CompressionParams.noCompression();
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        CommitLog.instance.stopUnsafe(true);
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.restartUnsafe();
    }

    public static void cleanup()
    {
        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());

            // Leave the folder around as Windows will complain about directory deletion w/handles open to children files
            String[] children = dir.list();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            String[] children = dir.list();
            for (String child : children)
                FileUtils.deleteRecursive(new File(dir, child));
        }
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void insertData(String keyspace, String columnFamily, int offset, int numberOfRows)
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace, columnFamily);

        for (int i = offset; i < offset + numberOfRows; i++)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), ByteBufferUtil.bytes("key"+i));
            if (cfm.clusteringColumns() != null && !cfm.clusteringColumns().isEmpty())
                builder.clustering(ByteBufferUtil.bytes("col"+ i)).add("val", ByteBufferUtil.bytes("val" + i));
            else
                builder.add("val", ByteBufferUtil.bytes("val"+i));
            builder.build().apply();
        }
    }


    public static void cleanupSavedCaches()
    {
        File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }
}
