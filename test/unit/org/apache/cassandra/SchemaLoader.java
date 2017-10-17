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

import org.apache.cassandra.cql3.statements.CreateTableStatement;
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
import org.apache.cassandra.schema.MigrationManager;
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
                standardCFMD(ks1, "Standard1").compaction(CompactionParams.scts(compactionOptions)).build(),
                standardCFMD(ks1, "Standard2").build(),
                standardCFMD(ks1, "Standard3").build(),
                standardCFMD(ks1, "Standard4").build(),
                standardCFMD(ks1, "StandardGCGS0").gcGraceSeconds(0).build(),
                standardCFMD(ks1, "StandardLong1").build(),
                standardCFMD(ks1, "StandardLong2").build(),
                superCFMD(ks1, "Super1", LongType.instance).build(),
                superCFMD(ks1, "Super2", LongType.instance).build(),
                superCFMD(ks1, "Super3", LongType.instance).build(),
                superCFMD(ks1, "Super4", UTF8Type.instance).build(),
                superCFMD(ks1, "Super5", bytes).build(),
                superCFMD(ks1, "Super6", LexicalUUIDType.instance, UTF8Type.instance).build(),
                keysIndexCFMD(ks1, "Indexed1", true).build(),
                keysIndexCFMD(ks1, "Indexed2", false).build(),
                superCFMD(ks1, "SuperDirectGC", BytesType.instance).gcGraceSeconds(0).build(),
                jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance).addColumn(utf8Column(ks1, "JdbcUtf8")).build(),
                jdbcCFMD(ks1, "JdbcLong", LongType.instance).build(),
                jdbcCFMD(ks1, "JdbcBytes", bytes).build(),
                jdbcCFMD(ks1, "JdbcAscii", AsciiType.instance).build(),
                standardCFMD(ks1, "StandardLeveled").compaction(CompactionParams.lcs(leveledOptions)).build(),
                standardCFMD(ks1, "legacyleveled").compaction(CompactionParams.lcs(leveledOptions)).build(),
                standardCFMD(ks1, "StandardLowIndexInterval").minIndexInterval(8)
                                                             .maxIndexInterval(256)
                                                             .caching(CachingParams.CACHE_NOTHING).build()
        )));

        // Keyspace 2
        schema.add(KeyspaceMetadata.create(ks2,
                KeyspaceParams.simple(1),
                Tables.of(
                // Column Families
                standardCFMD(ks2, "Standard1").build(),
                standardCFMD(ks2, "Standard3").build(),
                superCFMD(ks2, "Super3", bytes).build(),
                superCFMD(ks2, "Super4", TimeUUIDType.instance).build(),
                keysIndexCFMD(ks2, "Indexed1", true).build(),
                compositeIndexCFMD(ks2, "Indexed2", true).build(),
                compositeIndexCFMD(ks2, "Indexed3", true).gcGraceSeconds(0).build())));

        // Keyspace 3
        schema.add(KeyspaceMetadata.create(ks3,
                KeyspaceParams.simple(5),
                Tables.of(
                standardCFMD(ks3, "Standard1").build(),
                keysIndexCFMD(ks3, "Indexed1", true).build())));

        // Keyspace 4
        schema.add(KeyspaceMetadata.create(ks4,
                KeyspaceParams.simple(3),
                Tables.of(
                standardCFMD(ks4, "Standard1").build(),
                standardCFMD(ks4, "Standard3").build(),
                superCFMD(ks4, "Super3", bytes).build(),
                superCFMD(ks4, "Super4", TimeUUIDType.instance).build(),
                superCFMD(ks4, "Super5", TimeUUIDType.instance, BytesType.instance).build())));

        // Keyspace 5
        schema.add(KeyspaceMetadata.create(ks5,
                KeyspaceParams.simple(2),
                Tables.of(standardCFMD(ks5, "Standard1").build())));

        // Keyspace 6
        schema.add(KeyspaceMetadata.create(ks6,
                KeyspaceParams.simple(1),
                Tables.of(keysIndexCFMD(ks6, "Indexed1", true).build())));

        // Keyspace 7
        schema.add(KeyspaceMetadata.create(ks7,
                KeyspaceParams.simple(1),
                Tables.of(customIndexCFMD(ks7, "Indexed1").build())));

        // KeyCacheSpace
        schema.add(KeyspaceMetadata.create(ks_kcs,
                KeyspaceParams.simple(1),
                Tables.of(
                standardCFMD(ks_kcs, "Standard1").build(),
                standardCFMD(ks_kcs, "Standard2").build(),
                standardCFMD(ks_kcs, "Standard3").build())));

        // RowCacheSpace
        schema.add(KeyspaceMetadata.create(ks_rcs,
                KeyspaceParams.simple(1),
                Tables.of(
                standardCFMD(ks_rcs, "CFWithoutCache").caching(CachingParams.CACHE_NOTHING).build(),
                standardCFMD(ks_rcs, "CachedCF").caching(CachingParams.CACHE_EVERYTHING).build(),
                standardCFMD(ks_rcs, "CachedNoClustering", 1, IntegerType.instance, IntegerType.instance, null).caching(CachingParams.CACHE_EVERYTHING).build(),
                standardCFMD(ks_rcs, "CachedIntCF").caching(new CachingParams(true, 100)).build())));

        schema.add(KeyspaceMetadata.create(ks_nocommit, KeyspaceParams.simpleTransient(1), Tables.of(
                standardCFMD(ks_nocommit, "Standard1").build())));

        // CQLKeyspace
        schema.add(KeyspaceMetadata.create(ks_cql, KeyspaceParams.simple(1), Tables.of(

        // Column Families
        CreateTableStatement.parse("CREATE TABLE table1 ("
                                   + "k int PRIMARY KEY,"
                                   + "v1 text,"
                                   + "v2 int"
                                   + ")", ks_cql)
                            .build(),

        CreateTableStatement.parse("CREATE TABLE table2 ("
                                   + "k text,"
                                   + "c text,"
                                   + "v text,"
                                   + "PRIMARY KEY (k, c))", ks_cql)
                            .build()
        )));

        if (DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner)
        {
            schema.add(KeyspaceMetadata.create("sasi",
                                               KeyspaceParams.simpleTransient(1),
                                               Tables.of(sasiCFMD("sasi", "test_cf").build(),
                                                         clusteringSASICFMD("sasi", "clustering_test_cf").build())));
        }

        // if you're messing with low-level sstable stuff, it can be useful to inject the schema directly
        // Schema.instance.load(schemaDefinition());
        for (KeyspaceMetadata ksm : schema)
            MigrationManager.announceNewKeyspace(ksm, false);

        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            useCompression(schema);
    }

    public static void createKeyspace(String name, KeyspaceParams params)
    {
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, Tables.of()), true);
    }

    public static void createKeyspace(String name, KeyspaceParams params, TableMetadata.Builder... builders)
    {
        Tables.Builder tables = Tables.builder();
        for (TableMetadata.Builder builder : builders)
            tables.add(builder.build());

        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, tables.build()), true);
    }

    public static void createKeyspace(String name, KeyspaceParams params, TableMetadata... tables)
    {
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, Tables.of(tables)), true);
    }

    public static void createKeyspace(String name, KeyspaceParams params, Tables tables, Types types)
    {
        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(name, params, tables, Views.none(), types, Functions.none()), true);
    }

    public static ColumnMetadata integerColumn(String ksName, String cfName)
    {
        return new ColumnMetadata(ksName,
                                  cfName,
                                  ColumnIdentifier.getInterned(IntegerType.instance.fromString("42"), IntegerType.instance),
                                  UTF8Type.instance,
                                  ColumnMetadata.NO_POSITION,
                                  ColumnMetadata.Kind.REGULAR);
    }

    public static ColumnMetadata utf8Column(String ksName, String cfName)
    {
        return new ColumnMetadata(ksName,
                                  cfName,
                                  ColumnIdentifier.getInterned("fortytwo", true),
                                  UTF8Type.instance,
                                  ColumnMetadata.NO_POSITION,
                                  ColumnMetadata.Kind.REGULAR);
    }

    public static TableMetadata perRowIndexedCFMD(String ksName, String cfName)
    {
        ColumnMetadata indexedColumn = ColumnMetadata.regularColumn(ksName, cfName, "indexed", AsciiType.instance);

        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addColumn(indexedColumn);

        final Map<String, String> indexOptions = Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName());
        builder.indexes(Indexes.of(IndexMetadata.fromIndexTargets(
        Collections.singletonList(new IndexTarget(indexedColumn.name,
                                                                                                            IndexTarget.Type.VALUES)),
                                                                  "indexe1",
                                                                  IndexMetadata.Kind.CUSTOM,
                                                                  indexOptions)));

        return builder.build();
    }

    private static void useCompression(List<KeyspaceMetadata> schema)
    {
        for (KeyspaceMetadata ksm : schema)
            for (TableMetadata cfm : ksm.tablesAndViews())
                MigrationManager.announceTableUpdate(cfm.unbuild().compression(CompressionParams.snappy()).build(), true);
    }

    public static TableMetadata.Builder counterCFMD(String ksName, String cfName)
    {
        return TableMetadata.builder(ksName, cfName)
                            .isCounter(true)
                            .addPartitionKeyColumn("key", AsciiType.instance)
                            .addClusteringColumn("name", AsciiType.instance)
                            .addRegularColumn("val", CounterColumnType.instance)
                            .addRegularColumn("val2", CounterColumnType.instance)
                            .compression(getCompressionParameters());
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName)
    {
        return standardCFMD(ksName, cfName, 1, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType)
    {
        return standardCFMD(ksName, cfName, columnCount, keyType, valType, AsciiType.instance);
    }

    public static TableMetadata.Builder standardCFMD(String ksName, String cfName, int columnCount, AbstractType<?> keyType, AbstractType<?> valType, AbstractType<?> clusteringType)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("key", keyType)
                         .addRegularColumn("val", valType)
                         .compression(getCompressionParameters());

        if (clusteringType != null)
            builder.addClusteringColumn("name", clusteringType);

        for (int i = 0; i < columnCount; i++)
            builder.addRegularColumn("val" + i, AsciiType.instance);

        return builder;
    }


    public static TableMetadata.Builder denseCFMD(String ksName, String cfName)
    {
        return denseCFMD(ksName, cfName, AsciiType.instance);
    }
    public static TableMetadata.Builder denseCFMD(String ksName, String cfName, AbstractType cc)
    {
        return denseCFMD(ksName, cfName, cc, null);
    }
    public static TableMetadata.Builder denseCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        AbstractType comp = cc;
        if (subcc != null)
            comp = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{cc, subcc}));

        return TableMetadata.builder(ksName, cfName)
                            .isDense(true)
                            .isCompound(subcc != null)
                            .addPartitionKeyColumn("key", AsciiType.instance)
                            .addClusteringColumn("cols", comp)
                            .addRegularColumn("val", AsciiType.instance)
                            .compression(getCompressionParameters());
    }

    // TODO: Fix superCFMD failing on legacy table creation. Seems to be applying composite comparator to partition key
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, BytesType.instance, subcc);
    }
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, "cols", cc, subcc);
    }
    public static TableMetadata.Builder superCFMD(String ksName, String cfName, String ccName, AbstractType cc, AbstractType subcc)
    {
        return standardCFMD(ksName, cfName);

    }
    public static TableMetadata.Builder compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex) throws ConfigurationException
    {
        return compositeIndexCFMD(ksName, cfName, withRegularIndex, false);
    }

    public static TableMetadata.Builder compositeIndexCFMD(String ksName, String cfName, boolean withRegularIndex, boolean withStaticIndex) throws ConfigurationException
    {
        // the withIndex flag exists to allow tests index creation
        // on existing columns
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addClusteringColumn("c1", AsciiType.instance)
                         .addRegularColumn("birthdate", LongType.instance)
                         .addRegularColumn("notbirthdate", LongType.instance)
                         .addStaticColumn("static", LongType.instance)
                         .compression(getCompressionParameters());

        Indexes.Builder indexes = Indexes.builder();

        if (withRegularIndex)
        {
            indexes.add(IndexMetadata.fromIndexTargets(
            Collections.singletonList(
                                                           new IndexTarget(new ColumnIdentifier("birthdate", true),
                                                                           IndexTarget.Type.VALUES)),
                                                       cfName + "_birthdate_key_index",
                                                       IndexMetadata.Kind.COMPOSITES,
                                                       Collections.EMPTY_MAP));
        }

        if (withStaticIndex)
        {
            indexes.add(IndexMetadata.fromIndexTargets(
            Collections.singletonList(
                                                           new IndexTarget(new ColumnIdentifier("static", true),
                                                                           IndexTarget.Type.VALUES)),
                                                       cfName + "_static_index",
                                                       IndexMetadata.Kind.COMPOSITES,
                                                       Collections.EMPTY_MAP));
        }

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder keysIndexCFMD(String ksName, String cfName, boolean withIndex)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .isCompound(false)
                         .isDense(true)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addClusteringColumn("c1", AsciiType.instance)
                         .addStaticColumn("birthdate", LongType.instance)
                         .addStaticColumn("notbirthdate", LongType.instance)
                         .addRegularColumn("value", LongType.instance)
                         .compression(getCompressionParameters());

        if (withIndex)
        {
            IndexMetadata index =
                IndexMetadata.fromIndexTargets(
                Collections.singletonList(new IndexTarget(new ColumnIdentifier("birthdate", true),
                                                                                         IndexTarget.Type.VALUES)),
                                                                                         cfName + "_birthdate_composite_index",
                                                                                         IndexMetadata.Kind.KEYS,
                                                                                         Collections.EMPTY_MAP);
            builder.indexes(Indexes.builder().add(index).build());
        }

        return builder;
    }

    public static TableMetadata.Builder customIndexCFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder  =
            TableMetadata.builder(ksName, cfName)
                         .isCompound(false)
                         .isDense(true)
                         .addPartitionKeyColumn("key", AsciiType.instance)
                         .addClusteringColumn("c1", AsciiType.instance)
                         .addRegularColumn("value", LongType.instance)
                         .compression(getCompressionParameters());

        IndexMetadata index =
            IndexMetadata.fromIndexTargets(
            Collections.singletonList(new IndexTarget(new ColumnIdentifier("value", true), IndexTarget.Type.VALUES)),
                                           cfName + "_value_index",
                                           IndexMetadata.Kind.CUSTOM,
                                           Collections.singletonMap(IndexTarget.CUSTOM_INDEX_OPTION_NAME, StubIndex.class.getName()));

        builder.indexes(Indexes.of(index));

        return builder;
    }

    public static TableMetadata.Builder jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return TableMetadata.builder(ksName, cfName)
                            .addPartitionKeyColumn("key", BytesType.instance)
                            .compression(getCompressionParameters());
    }

    public static TableMetadata.Builder sasiCFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("id", UTF8Type.instance)
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
                         .addRegularColumn("first_name_prefix", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "first_name");
                        put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_last_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "last_name");
                        put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_age", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "age");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_timestamp", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "timestamp");
                        put("mode", OnDiskIndexBuilder.Mode.SPARSE.toString());

                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_address", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "address");
                        put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
                        put("case_sensitive", "false");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_score", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "score");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "comment");
                        put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                        put("analyzed", "true");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_comment_suffix_split", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "comment_suffix_split");
                        put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                        put("analyzed", "false");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_output_full_name", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "/output/full-name/");
                        put("analyzed", "true");
                        put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
                        put("case_sensitive", "false");
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_data_output_id", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "/data/output/id");
                        put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
                    }}))
               .add(IndexMetadata.fromSchemaMetadata(cfName + "_first_name_prefix", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
                    {{
                        put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                        put(IndexTarget.TARGET_OPTION_NAME, "first_name_prefix");
                        put("analyzed", "true");
                        put("tokenization_normalize_lowercase", "true");
                    }}));

    return builder.indexes(indexes.build());
}

public static TableMetadata.Builder clusteringSASICFMD(String ksName, String cfName)
{
    return clusteringSASICFMD(ksName, cfName, "location", "age", "height", "score");
}

    public static TableMetadata.Builder clusteringSASICFMD(String ksName, String cfName, String...indexedColumns)
    {
        Indexes.Builder indexes = Indexes.builder();
        for (String indexedColumn : indexedColumns)
        {
            indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_" + indexedColumn, IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
            {{
                put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
                put(IndexTarget.TARGET_OPTION_NAME, indexedColumn);
                put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
            }}));
        }

        return TableMetadata.builder(ksName, cfName)
                            .addPartitionKeyColumn("name", UTF8Type.instance)
                            .addClusteringColumn("location", UTF8Type.instance)
                            .addClusteringColumn("age", Int32Type.instance)
                            .addRegularColumn("height", Int32Type.instance)
                            .addRegularColumn("score", DoubleType.instance)
                            .addStaticColumn("nickname", UTF8Type.instance)
                            .indexes(indexes.build());
    }

    public static TableMetadata.Builder staticSASICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("sensor_id", Int32Type.instance)
                         .addStaticColumn("sensor_type", UTF8Type.instance)
                         .addClusteringColumn("date", LongType.instance)
                         .addRegularColumn("value", DoubleType.instance)
                         .addRegularColumn("variance", Int32Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_sensor_type", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "sensor_type");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
            put("case_sensitive", "false");
        }}));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_value", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "value");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
        }}));

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_variance", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "variance");
            put("mode", OnDiskIndexBuilder.Mode.PREFIX.toString());
        }}));

        return builder.indexes(indexes.build());
    }

    public static TableMetadata.Builder fullTextSearchSASICFMD(String ksName, String cfName)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ksName, cfName)
                         .addPartitionKeyColumn("song_id", UUIDType.instance)
                         .addRegularColumn("title", UTF8Type.instance)
                         .addRegularColumn("artist", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_title", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
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

        indexes.add(IndexMetadata.fromSchemaMetadata(cfName + "_artist", IndexMetadata.Kind.CUSTOM, new HashMap<String, String>()
        {{
            put(IndexTarget.CUSTOM_INDEX_OPTION_NAME, SASIIndex.class.getName());
            put(IndexTarget.TARGET_OPTION_NAME, "artist");
            put("mode", OnDiskIndexBuilder.Mode.CONTAINS.toString());
            put("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
            put("case_sensitive", "false");

        }}));

        return builder.indexes(indexes.build());
    }

    public static CompressionParams getCompressionParameters()
    {
        return getCompressionParameters(null);
    }

    public static CompressionParams getCompressionParameters(Integer chunkSize)
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            return chunkSize != null ? CompressionParams.snappy(chunkSize) : CompressionParams.snappy();

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
        TableMetadata cfm = Schema.instance.getTableMetadata(keyspace, columnFamily);

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
