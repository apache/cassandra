/**
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.cassandra.db.index.PerRowSecondaryIndexTest;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SchemaLoader
{
    private static Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

    @BeforeClass
    public static void loadSchema() throws IOException, ConfigurationException
    {
        loadSchema(false);
    }

    public static void loadSchema(boolean withOldCfIds) throws IOException, ConfigurationException
    {
        // Cleanup first
        cleanupAndLeaveDirs();

        CommitLog.instance.allocator.enableReserveSegmentCreation();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                logger.error("Fatal exception in thread " + t, e);
            }
        });

        // Migrations aren't happy if gossiper is not started.  Even if we don't use migrations though,
        // some tests now expect us to start gossip for them.
        startGossiper();
        // if you're messing with low-level sstable stuff, it can be useful to inject the schema directly
        // Schema.instance.load(schemaDefinition(withOldCfIds));
        for (KSMetaData ksm : schemaDefinition(withOldCfIds))
            MigrationManager.announceNewKeyspace(ksm);
    }

    public static void startGossiper()
    {
        Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
    }

    @AfterClass
    public static void stopGossiper()
    {
        Gossiper.instance.stop();
    }

    public static Collection<KSMetaData> schemaDefinition(boolean withOldCfIds) throws ConfigurationException
    {
        List<KSMetaData> schema = new ArrayList<KSMetaData>();

        // A whole bucket of shorthand
        String ks1 = "Keyspace1";
        String ks2 = "Keyspace2";
        String ks3 = "Keyspace3";
        String ks4 = "Keyspace4";
        String ks5 = "Keyspace5";
        String ks6 = "Keyspace6";
        String ks_kcs = "KeyCacheSpace";
        String ks_rcs = "RowCacheSpace";
        String ks_nocommit = "NoCommitlogSpace";
        String ks_prsi = "PerRowSecondaryIndex";
        String ks_cql = "cql_keyspace";

        Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

        Map<String, String> opts_rf1 = KSMetaData.optsWithRF(1);
        Map<String, String> opts_rf2 = KSMetaData.optsWithRF(2);
        Map<String, String> opts_rf3 = KSMetaData.optsWithRF(3);
        Map<String, String> opts_rf5 = KSMetaData.optsWithRF(5);

        ColumnFamilyType st = ColumnFamilyType.Standard;
        ColumnFamilyType su = ColumnFamilyType.Super;
        AbstractType bytes = BytesType.instance;

        AbstractType<?> composite = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, TimeUUIDType.instance, IntegerType.instance}));
        AbstractType<?> compositeMaxMin = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{BytesType.instance, IntegerType.instance}));
        Map<Byte, AbstractType<?>> aliases = new HashMap<Byte, AbstractType<?>>();
        aliases.put((byte)'b', BytesType.instance);
        aliases.put((byte)'t', TimeUUIDType.instance);
        aliases.put((byte)'B', ReversedType.getInstance(BytesType.instance));
        aliases.put((byte)'T', ReversedType.getInstance(TimeUUIDType.instance));
        AbstractType<?> dynamicComposite = DynamicCompositeType.getInstance(aliases);

        // these column definitions will will be applied to the jdbc utf and integer column familes respectively.
        Map<ByteBuffer, ColumnDefinition> integerColumn = new HashMap<ByteBuffer, ColumnDefinition>();
        integerColumn.put(IntegerType.instance.fromString("42"), ColumnDefinition.regularDef(IntegerType.instance.fromString("42"), UTF8Type.instance, null));
        Map<ByteBuffer, ColumnDefinition> utf8Column = new HashMap<ByteBuffer, ColumnDefinition>();
        utf8Column.put(UTF8Type.instance.fromString("fortytwo"), ColumnDefinition.regularDef(UTF8Type.instance.fromString("fortytwo"), IntegerType.instance, null));

        // Make it easy to test compaction
        Map<String, String> compactionOptions = new HashMap<String, String>();
        compactionOptions.put("tombstone_compaction_interval", "1");
        Map<String, String> leveledOptions = new HashMap<String, String>();
        leveledOptions.put("sstable_size_in_mb", "1");

        // Keyspace 1
        schema.add(KSMetaData.testMetadata(ks1,
                                           simple,
                                           opts_rf1,

                                           // Column Families
                                           standardCFMD(ks1, "Standard1").compactionStrategyOptions(compactionOptions),
                                           standardCFMD(ks1, "Standard2"),
                                           standardCFMD(ks1, "Standard3"),
                                           standardCFMD(ks1, "Standard4"),
                                           standardCFMD(ks1, "StandardGCGS0").gcGraceSeconds(0),
                                           standardCFMD(ks1, "StandardLong1"),
                                           standardCFMD(ks1, "StandardLong2"),
                                           new CFMetaData(ks1,
                                                          "ValuesWithQuotes",
                                                          st,
                                                          BytesType.instance,
                                                          null)
                                                   .defaultValidator(UTF8Type.instance),
                                           superCFMD(ks1, "Super1", LongType.instance),
                                           superCFMD(ks1, "Super2", LongType.instance),
                                           superCFMD(ks1, "Super3", LongType.instance),
                                           superCFMD(ks1, "Super4", UTF8Type.instance),
                                           superCFMD(ks1, "Super5", bytes),
                                           superCFMD(ks1, "Super6", LexicalUUIDType.instance, UTF8Type.instance),
                                           indexCFMD(ks1, "Indexed1", true),
                                           indexCFMD(ks1, "Indexed2", false),
                                           new CFMetaData(ks1,
                                                          "StandardInteger1",
                                                          st,
                                                          IntegerType.instance,
                                                          null),
                                           new CFMetaData(ks1,
                                                          "StandardLong3",
                                                          st,
                                                          LongType.instance,
                                                          null),
                                           new CFMetaData(ks1,
                                                          "Counter1",
                                                          st,
                                                          bytes,
                                                          null)
                                                   .defaultValidator(CounterColumnType.instance),
                                           new CFMetaData(ks1,
                                                          "SuperCounter1",
                                                          su,
                                                          bytes,
                                                          bytes)
                                                   .defaultValidator(CounterColumnType.instance),
                                           superCFMD(ks1, "SuperDirectGC", BytesType.instance).gcGraceSeconds(0),
                                           jdbcCFMD(ks1, "JdbcInteger", IntegerType.instance).columnMetadata(integerColumn),
                                           jdbcCFMD(ks1, "JdbcUtf8", UTF8Type.instance).columnMetadata(utf8Column),
                                           jdbcCFMD(ks1, "JdbcLong", LongType.instance),
                                           jdbcCFMD(ks1, "JdbcBytes", bytes),
                                           jdbcCFMD(ks1, "JdbcAscii", AsciiType.instance),
                                           new CFMetaData(ks1,
                                                          "StandardComposite",
                                                          st,
                                                          composite,
                                                          null),
                                           new CFMetaData(ks1,
                                                          "StandardComposite2",
                                                          st,
                                                          compositeMaxMin,
                                                          null),
                                           new CFMetaData(ks1,
                                                          "StandardDynamicComposite",
                                                          st,
                                                          dynamicComposite,
                                                          null),
                                           standardCFMD(ks1, "StandardLeveled")
                                                                               .compactionStrategyClass(LeveledCompactionStrategy.class)
                                                                               .compactionStrategyOptions(leveledOptions),
                                           standardCFMD(ks1, "legacyleveled")
                                                                               .compactionStrategyClass(LeveledCompactionStrategy.class)
                                                                               .compactionStrategyOptions(leveledOptions),
                                           standardCFMD(ks1, "UUIDKeys").keyValidator(UUIDType.instance),
                                           new CFMetaData(ks1,
                                                          "MixedTypes",
                                                          st,
                                                          LongType.instance,
                                                          null).keyValidator(UUIDType.instance).defaultValidator(BooleanType.instance),
                                           new CFMetaData(ks1,
                                                          "MixedTypesComposite",
                                                          st,
                                                          composite,
                                                          null).keyValidator(composite).defaultValidator(BooleanType.instance)));

        // Keyspace 2
        schema.add(KSMetaData.testMetadata(ks2,
                                           simple,
                                           opts_rf1,

                                           // Column Families
                                           standardCFMD(ks2, "Standard1"),
                                           standardCFMD(ks2, "Standard3"),
                                           superCFMD(ks2, "Super3", bytes),
                                           superCFMD(ks2, "Super4", TimeUUIDType.instance),
                                           indexCFMD(ks2, "Indexed1", true),
                                           compositeIndexCFMD(ks2, "Indexed2", true, withOldCfIds),
                                           compositeIndexCFMD(ks2, "Indexed3", true, withOldCfIds).gcGraceSeconds(0)));

        // Keyspace 3
        schema.add(KSMetaData.testMetadata(ks3,
                                           simple,
                                           opts_rf5,

                                           // Column Families
                                           standardCFMD(ks3, "Standard1"),
                                           indexCFMD(ks3, "Indexed1", true)));

        // Keyspace 4
        schema.add(KSMetaData.testMetadata(ks4,
                                           simple,
                                           opts_rf3,

                                           // Column Families
                                           standardCFMD(ks4, "Standard1"),
                                           standardCFMD(ks4, "Standard3"),
                                           superCFMD(ks4, "Super3", bytes),
                                           superCFMD(ks4, "Super4", TimeUUIDType.instance),
                                           new CFMetaData(ks4,
                                                          "Super5",
                                                          su,
                                                          TimeUUIDType.instance,
                                                          bytes)));

        // Keyspace 5
        schema.add(KSMetaData.testMetadata(ks5,
                                           simple,
                                           opts_rf2,
                                           standardCFMD(ks5, "Standard1"),
                                           standardCFMD(ks5, "Counter1")
                                                   .defaultValidator(CounterColumnType.instance)));

        // Keyspace 6
        schema.add(KSMetaData.testMetadata(ks6,
                                           simple,
                                           opts_rf1,
                                           indexCFMD(ks6, "Indexed1", true)));

        // KeyCacheSpace
        schema.add(KSMetaData.testMetadata(ks_kcs,
                                           simple,
                                           opts_rf1,
                                           standardCFMD(ks_kcs, "Standard1"),
                                           standardCFMD(ks_kcs, "Standard2"),
                                           standardCFMD(ks_kcs, "Standard3")));

        // RowCacheSpace
        schema.add(KSMetaData.testMetadata(ks_rcs,
                                           simple,
                                           opts_rf1,
                                           standardCFMD(ks_rcs, "CFWithoutCache").caching(CFMetaData.Caching.NONE),
                                           standardCFMD(ks_rcs, "CachedCF").caching(CFMetaData.Caching.ALL)));

        schema.add(KSMetaData.testMetadataNotDurable(ks_nocommit,
                                                     simple,
                                                     opts_rf1,
                                                     standardCFMD(ks_nocommit, "Standard1")));

        // PerRowSecondaryIndexTest
        schema.add(KSMetaData.testMetadata(ks_prsi,
                                           simple,
                                           opts_rf1,
                                           perRowIndexedCFMD(ks_prsi, "Indexed1", withOldCfIds)));

        // CQLKeyspace
        schema.add(KSMetaData.testMetadata(ks_cql,
                                           simple,
                                           opts_rf1,

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
                                           ));


        if (Boolean.parseBoolean(System.getProperty("cassandra.test.compression", "false")))
            useCompression(schema);

        return schema;
    }

    private static CFMetaData perRowIndexedCFMD(String ksName, String cfName, boolean withOldCfIds)
    {
        final Map<String, String> indexOptions = Collections.singletonMap(
                                                      SecondaryIndex.CUSTOM_INDEX_OPTION_NAME,
                                                      PerRowSecondaryIndexTest.TestIndex.class.getName());
        return standardCFMD(ksName, cfName)
                .keyValidator(AsciiType.instance)
                .columnMetadata(new HashMap<ByteBuffer, ColumnDefinition>()
                {{
                        ByteBuffer cName = ByteBuffer.wrap("indexed".getBytes(StandardCharsets.UTF_8));
                        put(cName, new ColumnDefinition(cName,
                                AsciiType.instance,
                                IndexType.CUSTOM,
                                indexOptions,
                                ByteBufferUtil.bytesToHex(cName),
                                null, ColumnDefinition.Type.REGULAR));
                }});
    }

    private static void useCompression(List<KSMetaData> schema)
    {
        for (KSMetaData ksm : schema)
        {
            for (CFMetaData cfm : ksm.cfMetaData().values())
            {
                cfm.compressionParameters(new CompressionParameters(SnappyCompressor.instance));
            }
        }
    }

    private static CFMetaData standardCFMD(String ksName, String cfName)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, BytesType.instance, null);
    }
    private static CFMetaData superCFMD(String ksName, String cfName, AbstractType subcc)
    {
        return superCFMD(ksName, cfName, BytesType.instance, subcc);
    }
    private static CFMetaData superCFMD(String ksName, String cfName, AbstractType cc, AbstractType subcc)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Super, cc, subcc);
    }
    private static CFMetaData indexCFMD(String ksName, String cfName, final Boolean withIdxType) throws ConfigurationException
    {
        return standardCFMD(ksName, cfName)
               .keyValidator(AsciiType.instance)
               .columnMetadata(new HashMap<ByteBuffer, ColumnDefinition>()
                   {{
                        ByteBuffer cName = ByteBuffer.wrap("birthdate".getBytes(StandardCharsets.UTF_8));
                        IndexType keys = withIdxType ? IndexType.KEYS : null;
                        put(cName, ColumnDefinition.regularDef(cName, LongType.instance, null).setIndex(withIdxType ? ByteBufferUtil.bytesToHex(cName) : null, keys, null));
                    }});
    }
    private static CFMetaData compositeIndexCFMD(String ksName, String cfName, final Boolean withIdxType, boolean withOldCfIds) throws ConfigurationException
    {
        final CompositeType composite = CompositeType.getInstance(Arrays.asList(new AbstractType<?>[]{UTF8Type.instance, UTF8Type.instance})); 
        return new CFMetaData(ksName,
                cfName,
                ColumnFamilyType.Standard,
                composite,
                null)
               .columnMetadata(new HashMap<ByteBuffer, ColumnDefinition>()
                {{
                   ByteBuffer cName = ByteBuffer.wrap("col1".getBytes(StandardCharsets.UTF_8));
                   IndexType idxType = withIdxType ? IndexType.COMPOSITES : null;
                   put(cName, ColumnDefinition.regularDef(cName, UTF8Type.instance, 1)
                                              .setIndex(withIdxType ? "col1_idx" : null, idxType, Collections.<String, String>emptyMap()));
                }});
    }
    
    private static CFMetaData jdbcCFMD(String ksName, String cfName, AbstractType comp)
    {
        return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, comp, null).defaultValidator(comp);
    }

    public static void cleanupAndLeaveDirs()
    {
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
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
            FileUtils.deleteRecursive(dir);
        }

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    protected void insertData(String keyspace, String columnFamily, int offset, int numberOfRows) throws IOException
    {
        for (int i = offset; i < offset + numberOfRows; i++)
        {
            ByteBuffer key = ByteBufferUtil.bytes("key" + i);
            RowMutation rowMutation = new RowMutation(keyspace, key);
            rowMutation.add(columnFamily, ByteBufferUtil.bytes("col" + i), ByteBufferUtil.bytes("val" + i), System.currentTimeMillis());
            rowMutation.applyUnsafe();
        }
    }

    /* usually used to populate the cache */
    protected void readData(String keyspace, String columnFamily, int offset, int numberOfRows) throws IOException
    {
        ColumnFamilyStore store = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);
        for (int i = offset; i < offset + numberOfRows; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            store.getColumnFamily(QueryFilter.getNamesFilter(key, columnFamily, FBUtilities.singleton(ByteBufferUtil.bytes("col" + i), store.getComparator()), System.currentTimeMillis()));
        }
    }

    protected static void cleanupSavedCaches()
    {
        File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }
}
