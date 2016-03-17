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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static final String LEGACY_SSTABLE_PROP = "legacy-sstable-root";

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
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }
        String scp = System.getProperty(LEGACY_SSTABLE_PROP);
        assert scp != null;
        LEGACY_SSTABLE_ROOT = new File(scp).getAbsoluteFile();
        assert LEGACY_SSTABLE_ROOT.isDirectory();
    }

    @After
    public void tearDown()
    {
        for (String legacyVersion : legacyVersions)
        {
            truncateTables(legacyVersion);
        }
    }

    /**
     * Get a descriptor for the legacy sstable at the given version.
     */
    protected Descriptor getDescriptor(String legacyVersion, String table)
    {
        return new Descriptor(legacyVersion, getTableDir(legacyVersion, table), "legacy_tables", table, 1,
                              BigFormat.instance.getVersion(legacyVersion).hasNewFileName()?
                              SSTableFormat.Type.BIG :SSTableFormat.Type.LEGACY);
    }

    @Test
    public void testLoadLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            verifyReads(legacyVersion);
            verifyCache(legacyVersion, startCount);
        }
    }

    @Test
    public void testStreamLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            streamLegacyTables(legacyVersion);
            verifyReads(legacyVersion);
        }
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Streaming legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            streamLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion, String compactNameSuffix) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion, compactNameSuffix);
        SSTableReader sstable = SSTableReader.open(getDescriptor(legacyVersion, table));
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
    }

    private static void loadLegacyTables(String legacyVersion) throws Exception
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            logger.info("Preparing legacy version {}{}", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact));
            loadLegacyTable("legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact));
        }
    }

    private static void verifyCache(String legacyVersion, long startCount) throws InterruptedException, java.util.concurrent.ExecutionException
    {
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

    private static void verifyReads(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
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
                        readSimpleTable(legacyVersion, getCompactNameSuffix(compact),  pkValue);
                        readSimpleCounterTable(legacyVersion, getCompactNameSuffix(compact), pkValue);
                    }

                    readClusteringTable(legacyVersion, getCompactNameSuffix(compact), ck, ckValue, pkValue);
                    readClusteringCounterTable(legacyVersion, getCompactNameSuffix(compact), ckValue, pkValue);
                }
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String compactSuffix, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, String compactSuffix, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck=?", legacyVersion, compactSuffix), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust%s WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion, compactSuffix), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String compactSuffix, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple{}", legacyVersion, compactSuffix);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple%s WHERE pk=?", legacyVersion, compactSuffix), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals("foo bar baz", rs.one().getString("val"));
    }

    private static void createKeyspace()
    {
        QueryProcessor.executeInternal("CREATE KEYSPACE legacy_tables WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    private static void createTables(String legacyVersion)
    {
        for (int i=0; i<=1; i++)
        {
            String compactSuffix = getCompactNameSuffix(i);
            String tableSuffix = i == 0? "" : " WITH COMPACT STORAGE";
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple%s (pk text PRIMARY KEY, val text)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter%s (pk text PRIMARY KEY, val counter)%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust%s (pk text, ck text, val text, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter%s (pk text, ck text, val counter, PRIMARY KEY (pk, ck))%s", legacyVersion, compactSuffix, tableSuffix));
        }
    }

    private static String getCompactNameSuffix(int i)
    {
        return i == 0? "" : "_compact";
    }

    private static void truncateTables(String legacyVersion)
    {
        for (int compact = 0; compact <= 1; compact++)
        {
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust%s", legacyVersion, getCompactNameSuffix(compact)));
            QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter%s", legacyVersion, getCompactNameSuffix(compact)));
        }
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
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

    private static void loadLegacyTable(String tablePattern, String legacyVersion, String compactSuffix) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion, compactSuffix);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open("legacy_tables").getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for 8 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#getVersion() BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore
    @Test
    public void testGenerateSstables() throws Throwable
    {
        Random rand = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 128; i++)
        {
            sb.append((char)('a' + rand.nextInt(26)));
        }
        String randomString = sb.toString();

        for (int compact = 0; compact <= 1; compact++)
        {
            for (int pk = 0; pk < 5; pk++)
            {
                String valPk = Integer.toString(pk);
                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_simple%s (pk, val) VALUES ('%s', '%s')",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, "foo bar baz"));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter%s SET val = val + 1 WHERE pk = '%s'",
                                                             BigFormat.latestVersion, getCompactNameSuffix(compact), valPk));

                for (int ck = 0; ck < 50; ck++)
                {
                    String valCk = Integer.toString(ck);

                    QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust%s (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString, randomString));

                    QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter%s SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                                 BigFormat.latestVersion, getCompactNameSuffix(compact), valPk, valCk + longString));

                }
            }
        }

        StorageService.instance.forceKeyspaceFlush("legacy_tables");

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", BigFormat.latestVersion));
        ksDir.mkdirs();
        for (int compact = 0; compact <= 1; compact++)
        {
            copySstablesFromTestData(String.format("legacy_%s_simple%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_simple_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
            copySstablesFromTestData(String.format("legacy_%s_clust_counter%s", BigFormat.latestVersion, getCompactNameSuffix(compact)), ksDir);
        }
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
        for (File file : getTableDir(legacyVersion, table).listFiles())
        {
            copyFile(cfDir, file);
        }
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
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
