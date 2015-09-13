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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";
    public static final String KSNAME = "Keyspace1";
    public static final String CFNAME = "Standard1";

    public static Set<String> TEST_DATA;
    public static File LEGACY_SSTABLE_ROOT;

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     */
    public static final String[] legacyVersions = {"ma", "la", "ka", "jb"};

    // 1200 chars
    static final String longString = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                                     "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        CFMetaData metadata = CFMetaData.Builder.createDense(KSNAME, CFNAME, false, false)
                                                .addPartitionKey("key", BytesType.instance)
                                                .addClusteringColumn("column", BytesType.instance)
                                                .addRegularColumn("value", BytesType.instance)
                                                .build();

        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    metadata);
        beforeClass();
    }

    public static void beforeClass()
    {
        Keyspace.setInitialized();
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();

        TEST_DATA = new HashSet<String>();
        for (int i = 100; i < 1000; ++i)
            TEST_DATA.add(Integer.toString(i));
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String ver)
    {
        File directory = new File(LEGACY_SSTABLE_ROOT + File.separator + ver + File.separator + KSNAME);
        return new Descriptor(ver, directory, KSNAME, CFNAME, 0, SSTableFormat.Type.LEGACY);
    }

    @Test
    public void testStreaming() throws Throwable
    {
        StorageService.instance.initServer();

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (!new File(LEGACY_SSTABLE_ROOT + File.separator + version.getName() + File.separator + KSNAME).isDirectory())
                continue;
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatibleForStreaming())
                testStreaming(version.getName());
        }
    }

    private void testStreaming(String version) throws Exception
    {
        SSTableReader sstable = SSTableReader.open(getDescriptor(version));
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        ArrayList<StreamSession.SSTableStreamingSections> details = new ArrayList<>();
        details.add(new StreamSession.SSTableStreamingSections(sstable.ref(),
                                                               sstable.getPositionsForRanges(ranges),
                                                               sstable.estimatedKeysForRanges(ranges), sstable.getSSTableMetadata().repairedAt));
        new StreamPlan("LegacyStreamingTest").transferFiles(FBUtilities.getBroadcastAddress(), details)
                                             .execute().get();

        ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);
        assert cfs.getLiveSSTables().size() == 1;
        sstable = cfs.getLiveSSTables().iterator().next();
        for (String keystring : TEST_DATA)
        {
            ByteBuffer key = bytes(keystring);

            UnfilteredRowIterator iter = sstable.iterator(Util.dk(key), Slices.ALL, ColumnFilter.selectionBuilder().add(cfs.metadata.getColumnDefinition(bytes("name"))).build(), false, false);

            // check not deleted (CASSANDRA-6527)
            assert iter.partitionLevelDeletion().equals(DeletionTime.LIVE);
            assert iter.next().clustering().get(0).equals(key);
        }
        sstable.selfRef().release();
    }

    @Test
    public void testVersions() throws Throwable
    {
        boolean notSkipped = false;

        for (File version : LEGACY_SSTABLE_ROOT.listFiles())
        {
            if (!new File(LEGACY_SSTABLE_ROOT + File.separator + version.getName() + File.separator + KSNAME).isDirectory())
                continue;
            if (Version.validate(version.getName()) && SSTableFormat.Type.LEGACY.info.getVersion(version.getName()).isCompatible())
            {
                notSkipped = true;
                testVersion(version.getName());
            }
        }

        assert notSkipped;
    }

    public void testVersion(String version) throws Throwable
    {
        try
        {
            ColumnFamilyStore cfs = Keyspace.open(KSNAME).getColumnFamilyStore(CFNAME);


            SSTableReader reader = SSTableReader.open(getDescriptor(version));
            for (String keystring : TEST_DATA)
            {

                ByteBuffer key = bytes(keystring);

                UnfilteredRowIterator iter = reader.iterator(Util.dk(key), Slices.ALL, ColumnFilter.selection(cfs.metadata.partitionColumns()), false, false);

                // check not deleted (CASSANDRA-6527)
                assert iter.partitionLevelDeletion().equals(DeletionTime.LIVE);
                assert iter.next().clustering().get(0).equals(key);
            }

            // TODO actually test some reads
        }
        catch (Throwable e)
        {
            System.err.println("Failed to read " + version);
            throw e;
        }
    }

    @Test
    public void testLegacyCqlTables() throws Exception
    {
        createKeyspace();

        loadLegacyTables();
    }

    private static void loadLegacyTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Preparing legacy version {}", legacyVersion);

            createTables(legacyVersion);

            loadLegacyTable("legacy_%s_simple", legacyVersion);
            loadLegacyTable("legacy_%s_simple_counter", legacyVersion);
            loadLegacyTable("legacy_%s_clust", legacyVersion);
            loadLegacyTable("legacy_%s_clust_counter", legacyVersion);

            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            for (int ck = 0; ck < 50; ck++)
            {
                String ckValue = Integer.toString(ck) + longString;
                for (int pk = 0; pk < 5; pk++)
                {
                    logger.debug("for pk={} ck={}", pk, ck);

                    String pkValue = Integer.toString(pk);
                    UntypedResultSet rs;
                    if (ck == 0)
                    {
                        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple WHERE pk=?", legacyVersion), pkValue);
                        Assert.assertNotNull(rs);
                        Assert.assertEquals(1, rs.size());
                        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
                        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter WHERE pk=?", legacyVersion), pkValue);
                        Assert.assertNotNull(rs);
                        Assert.assertEquals(1, rs.size());
                        Assert.assertEquals(1L, rs.one().getLong("val"));
                    }

                    rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
                    assertLegacyClustRows(1, rs);

                    String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
                    String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
                    rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion), pkValue, ckValue, ckValue2, ckValue3);
                    assertLegacyClustRows(3, rs);

                    rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
                    Assert.assertNotNull(rs);
                    Assert.assertEquals(1, rs.size());
                    Assert.assertEquals(1L, rs.one().getLong("val"));
                }
            }

            //For https://issues.apache.org/jira/browse/CASSANDRA-10778
            //Validate whether the key cache successfully saves in the presence of old keys as
            //well as loads the correct number of keys
            long endCount = CacheService.instance.keyCache.size();
            Assert.assertTrue(endCount > startCount);
            CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();
            CacheService.instance.invalidateKeyCache();
            Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
            CacheService.instance.keyCache.loadSaved();
            if (BigFormat.instance.getVersion(legacyVersion).storeRows())
                Assert.assertEquals(endCount, CacheService.instance.keyCache.size());
            else
                Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
        }
    }

    private void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", legacyVersion));
    }

    private static void assertLegacyClustRows(int count, UntypedResultSet rs)
    {
        Assert.assertNotNull(rs);
        Assert.assertEquals(count, rs.size());
        for (int i = 0; i < count; i++)
        {
            for (UntypedResultSet.Row r : rs)
            {
                Assert.assertEquals(128, r.getString("val").length());
            }
        }
    }

    private static void loadLegacyTable(String tablePattern, String legacyVersion) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for 4 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#getVersion() BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Test
    @Ignore
    public void testGenerateSstables() throws Throwable
    {
        createKeyspace();
        createTables(BigFormat.latestVersion.getVersion());

        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int pk = 0; pk < 5; pk++)
        {
            String valPk = Integer.toString(pk);
            QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple (pk, val) VALUES ('%s', '%s')",
                                                         BigFormat.latestVersion, valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         BigFormat.latestVersion, valPk));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             BigFormat.latestVersion, valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             BigFormat.latestVersion, valPk, valCk + longString));

            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", BigFormat.latestVersion));
        ksDir.mkdirs();
        copySstablesFromTestData(String.format("legacy_%s_simple", BigFormat.latestVersion), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_simple_counter", BigFormat.latestVersion), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_clust", BigFormat.latestVersion), ksDir);
        copySstablesFromTestData(String.format("legacy_%s_clust_counter", BigFormat.latestVersion), ksDir);
    }

    private void copySstablesFromTestData(String table, File ksDir) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.mkdir();

        for (File srcDir : Keyspace.open("legacy_tables").getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.listFiles())
            {
                copyFile(cfDir, file);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        for (File file : new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table)).listFiles())
        {
            copyFile(cfDir, file);
        }
    }

    private static void copyFile(File cfDir, File file) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            File target = new File(cfDir, file.getName());
            int rd;
            FileInputStream is = new FileInputStream(file);
            FileOutputStream os = new FileOutputStream(target);
            while ((rd = is.read(buf)) >= 0)
                os.write(buf, 0, rd);
        }
    }
}
