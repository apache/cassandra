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

import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.index.PerRowSecondaryIndexTest;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SchemaLoader
{
    private static Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

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
       CQLTester.prepareServer(false);
    }

    public static void startGossiper()
    {
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

        // Keyspace 1
        schema.add(KeyspaceMetadata.create(ks1,
                KeyspaceParams.simple(1),
                Tables.of(
                // Column Families
                standardCFMD(ks1, "Standard1").compactionStrategyOptions(compactionOptions),
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
                standardCFMD(ks1, "StandardLeveled")
                        .compactionStrategyClass(LeveledCompactionStrategy.class)
                        .compactionStrategyOptions(leveledOptions),
                standardCFMD(ks1, "legacyleveled")
                        .compactionStrategyClass(LeveledCompactionStrategy.class)
                        .compactionStrategyOptions(leveledOptions),
                standardCFMD(ks1, "StandardLowIndexInterval").minIndexInterval(8)
                        .maxIndexInterval(256)
                        .caching(CachingOptions.NONE)
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
                standardCFMD(ks_rcs, "CFWithoutCache").caching(CachingOptions.NONE),
                standardCFMD(ks_rcs, "CachedCF").caching(CachingOptions.ALL),
                standardCFMD(ks_rcs, "CachedIntCF").
                        caching(new CachingOptions(new CachingOptions.KeyCache(CachingOptions.KeyCache.Type.ALL),
                                new CachingOptions.RowCache(CachingOptions.RowCache.Type.HEAD, 100))))));

        // CounterCacheSpace
        /*schema.add(KeyspaceMetadata.testMetadata(ks_ccs,
                simple,
                opts_rf1,
                CFMetaData.Builder.create(ks_ccs, "Counter1", false, false, true).build(),
                CFMetaData.Builder.create(ks_ccs, "Counter1", false, false, true).build()));*/

        schema.add(KeyspaceMetadata.create(ks_nocommit, KeyspaceParams.simpleTransient(1), Tables.of(
                standardCFMD(ks_nocommit, "Standard1"))));

        // PerRowSecondaryIndexTest
        schema.add(KeyspaceMetadata.create(ks_prsi, KeyspaceParams.simple(1), Tables.of(
                perRowIndexedCFMD(ks_prsi, "Indexed1"))));

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

    public static ColumnDefinition integerColumn(String ksName, String cfName)
    {
        return new ColumnDefinition(ksName,
                                    cfName,
                                    ColumnIdentifier.getInterned(IntegerType.instance.fromString("42"), IntegerType.instance),
                                    UTF8Type.instance,
                                    null,
                                    null,
                                    null,
                                    null,
                                    ColumnDefinition.Kind.REGULAR);
    }

    public static ColumnDefinition utf8Column(String ksName, String cfName)
    {
        return new ColumnDefinition(ksName,
                                    cfName,
                                    ColumnIdentifier.getInterned("fortytwo", true),
                                    UTF8Type.instance,
                                    null,
                                    null,
                                    null,
                                    null,
                                    ColumnDefinition.Kind.REGULAR);
    }

    public static CFMetaData perRowIndexedCFMD(String ksName, String cfName)
    {
        final Map<String, String> indexOptions = Collections.singletonMap(
                                                      SecondaryIndex.CUSTOM_INDEX_OPTION_NAME,
                                                      PerRowSecondaryIndexTest.TestIndex.class.getName());

        CFMetaData cfm =  CFMetaData.Builder.create(ksName, cfName)
                .addPartitionKey("key", AsciiType.instance)
                .build();

        return cfm.addOrReplaceColumnDefinition(ColumnDefinition.regularDef(ksName, cfName, "indexed", AsciiType.instance)
                                                                .setIndex("indexe1", IndexType.CUSTOM, indexOptions));
    }

    private static void useCompression(List<KeyspaceMetadata> schema)
    {
        for (KeyspaceMetadata ksm : schema)
            for (CFMetaData cfm : ksm.tables)
                cfm.compressionParameters(CompressionParameters.snappy());
    }

    public static CFMetaData counterCFMD(String ksName, String cfName)
    {
        return CFMetaData.Builder.create(ksName, cfName, false, true, true)
                .addPartitionKey("key", AsciiType.instance)
                .addClusteringColumn("name", AsciiType.instance)
                .addRegularColumn("val", CounterColumnType.instance)
                .addRegularColumn("val2", CounterColumnType.instance)
                .build()
                .compressionParameters(getCompressionParameters());
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
        CFMetaData.Builder builder = CFMetaData.Builder.create(ksName, cfName)
                .addPartitionKey("key", keyType)
                .addClusteringColumn("name", clusteringType)
                .addRegularColumn("val", valType);

        for (int i = 0; i < columnCount; i++)
            builder.addRegularColumn("val" + i, AsciiType.instance);

        return builder.build()
               .compressionParameters(getCompressionParameters());
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
            .compressionParameters(getCompressionParameters());
    }

    // TODO: Fix superCFMD failing on legacy table creation. Seems to be applying composite comparator to partition key
    public static CFMetaData superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, BytesType.instance, subcc);
    }
    public static CFMetaData superCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, "cols", cc, subcc);
    }
    public static CFMetaData superCFMD(String ksName, String cfName, String ccName, AbstractType cc, AbstractType subcc)
    {
        //This is busted
//        return CFMetaData.Builder.createSuper(ksName, cfName, false)
//            .addPartitionKey("0", BytesType.instance)
//            .addClusteringColumn("1", cc)
//            .addClusteringColumn("2", subcc)
//            .addRegularColumn("3", AsciiType.instance)
//            .build();
        return standardCFMD(ksName, cfName);

    }
    public static CFMetaData compositeIndexCFMD(String ksName, String cfName, boolean withIndex) throws ConfigurationException
    {
        // the withIndex flag exists to allow tests index creation
        // on existing columns
        CFMetaData cfm = CFMetaData.Builder.create(ksName, cfName)
                .addPartitionKey("key", AsciiType.instance)
                .addClusteringColumn("c1", AsciiType.instance)
                .addRegularColumn("birthdate", LongType.instance)
                .addRegularColumn("notbirthdate", LongType.instance)
                .build();

        if (withIndex)
            cfm.getColumnDefinition(new ColumnIdentifier("birthdate", true))
               .setIndex("birthdate_key_index", IndexType.COMPOSITES, Collections.EMPTY_MAP);

        return cfm.compressionParameters(getCompressionParameters());
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
            cfm.getColumnDefinition(new ColumnIdentifier("birthdate", true))
               .setIndex("birthdate_composite_index", IndexType.KEYS, Collections.EMPTY_MAP);

        return cfm.compressionParameters(getCompressionParameters());
    }
    
    public static CFMetaData jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return CFMetaData.Builder.create(ksName, cfName).addPartitionKey("key", BytesType.instance)
                                                        .build()
                                                        .compressionParameters(getCompressionParameters());
    }

    public static CompressionParameters getCompressionParameters()
    {
        return getCompressionParameters(null);
    }

    public static CompressionParameters getCompressionParameters(Integer chunkSize)
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            return CompressionParameters.snappy(chunkSize);

        return CompressionParameters.noCompression();
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        CommitLog.instance.stopUnsafe(true);
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.startUnsafe();
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
            builder.clustering(ByteBufferUtil.bytes("col"+ i)).add("val", ByteBufferUtil.bytes("val" + i));
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
