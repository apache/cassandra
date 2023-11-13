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


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SinglePartitionSliceCommandTest;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.Collections.singleton;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_LEGACY_SSTABLE_ROOT;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests backwards compatibility for SSTables
 */
public class LegacySSTableTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacySSTableTest.class);

    public static File LEGACY_SSTABLE_ROOT;

    private static final String LEGACY_TABLES_KEYSPACE = "legacy_tables";

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     */
    public static String[] legacyVersions = null;

    // Get all versions up to the current one. Useful for testing in compatibility mode C18301
    private static String[] getValidLegacyVersions()
    {
        String[] versions = {"oa", "da", "nb", "na", "me", "md", "mc", "mb", "ma"};
        return Arrays.stream(versions).filter((v) -> v.compareTo(BigFormat.getInstance().getLatestVersion().toString()) <= 0).toArray(String[]::new);
    }

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
        String scp = TEST_LEGACY_SSTABLE_ROOT.getString();
        Assert.assertNotNull("System property " + TEST_LEGACY_SSTABLE_ROOT.getKey() + " not set", scp);

        LEGACY_SSTABLE_ROOT = new File(scp).toAbsolute();
        Assert.assertTrue("System property " + LEGACY_SSTABLE_ROOT + " does not specify a directory", LEGACY_SSTABLE_ROOT.isDirectory());

        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        Keyspace.setInitialized();
        createKeyspace();
        
        legacyVersions = getValidLegacyVersions();
        for (String legacyVersion : legacyVersions)
        {
            createTables(legacyVersion);
        }

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
    protected Descriptor getDescriptor(String legacyVersion, String table) throws IOException
    {
        File[] files = getTableDir(legacyVersion, table).list();
        Preconditions.checkArgument(files.length > 0, "No files for version=%s and table=%s", legacyVersion, table);
        return Descriptor.fromFileWithComponent(files[0]).left;
    }

    @Test
    public void testLoadLegacyCqlTables() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(99999);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testLoadLegacyCqlTablesShallow() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        CacheService.instance.invalidateKeyCache();
        doTestLegacyCqlTables();
    }

    @Test
    public void testMutateMetadata() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1234, NO_PENDING_REPAIR, false);
                    sstable.reloadSSTableMetadata();
                    assertEquals(1234, sstable.getRepairedAt());
                    if (sstable.descriptor.version.hasPendingRepair())
                        assertEquals(NO_PENDING_REPAIR, sstable.getPendingRepair());
                }

                boolean isTransient = false;
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    TimeUUID random = nextTimeUUID();
                    sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, UNREPAIRED_SSTABLE, random, isTransient);
                    sstable.reloadSSTableMetadata();
                    assertEquals(UNREPAIRED_SSTABLE, sstable.getRepairedAt());
                    if (sstable.descriptor.version.hasPendingRepair())
                        assertEquals(random, sstable.getPendingRepair());
                    if (sstable.descriptor.version.hasIsTransient())
                        assertEquals(isTransient, sstable.isTransient());

                    isTransient = !isTransient;
                }
            }
        }
    }

    @Test
    public void testMutateMetadataCSM() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            // Skip 2.0.1 sstables as it doesn't have repaired information
            if (legacyVersion.equals("jb"))
                continue;
            truncateTables(legacyVersion);
            loadLegacyTables(legacyVersion);

            for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
            {
                // set pending
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    TimeUUID random = nextTimeUUID();
                    try
                    {
                        cfs.getCompactionStrategyManager().mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, random, false);
                        if (!sstable.descriptor.version.hasPendingRepair())
                            fail("We should fail setting pending repair on unsupported sstables "+sstable);
                    }
                    catch (IllegalStateException e)
                    {
                        if (sstable.descriptor.version.hasPendingRepair())
                            fail("We should succeed setting pending repair on "+legacyVersion + " sstables, failed on "+sstable);
                    }
                }
                // set transient
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    try
                    {
                        cfs.getCompactionStrategyManager().mutateRepaired(Collections.singleton(sstable), UNREPAIRED_SSTABLE, nextTimeUUID(), true);
                        if (!sstable.descriptor.version.hasIsTransient())
                            fail("We should fail setting pending repair on unsupported sstables "+sstable);
                    }
                    catch (IllegalStateException e)
                    {
                        if (sstable.descriptor.version.hasIsTransient())
                            fail("We should succeed setting pending repair on "+legacyVersion + " sstables, failed on "+sstable);
                    }
                }
            }
        }
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        // we need to make sure we write old version metadata in the format for that version
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();

            for (ColumnFamilyStore cfs : Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStores())
            {
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1234);
                    sstable.reloadSSTableMetadata();
                    assertEquals(1234, sstable.getSSTableLevel());
                }
            }
        }
    }

    private void doTestLegacyCqlTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            CacheService.instance.invalidateKeyCache();
            long startCount = CacheService.instance.keyCache.size();
            verifyReads(legacyVersion);
            verifyCache(legacyVersion, startCount);
            compactLegacyTables(legacyVersion);
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

    @Test
    public void testInaccurateSSTableMinMax() throws Exception
    {
        QueryProcessor.executeInternal("CREATE TABLE legacy_tables.legacy_mc_inaccurate_min_max (k int, c1 int, c2 int, c3 int, v int, primary key (k, c1, c2, c3))");
        loadLegacyTable("legacy_%s_inaccurate_min_max", "mc");

        /*
         sstable has the following mutations:
            INSERT INTO legacy_tables.legacy_mc_inaccurate_min_max (k, c1, c2, c3, v) VALUES (100, 4, 4, 4, 4)
            DELETE FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1<3
         */

        String query = "SELECT * FROM legacy_tables.legacy_mc_inaccurate_min_max WHERE k=100 AND c1=1 AND c2=1";
        List<Unfiltered> unfiltereds = SinglePartitionSliceCommandTest.getUnfilteredsFromSinglePartition(query);
        Assert.assertEquals(2, unfiltereds.size());
        Assert.assertTrue(unfiltereds.get(0).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(0)).isOpen(false));
        Assert.assertTrue(unfiltereds.get(1).isRangeTombstoneMarker());
        Assert.assertTrue(((RangeTombstoneMarker) unfiltereds.get(1)).isClose(false));
    }

    @Test
    public void testVerifyOldSSTables() throws IOException
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            loadLegacyTable("legacy_%s_simple", legacyVersion);

            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(true).build()))
                {
                    verifier.verify();
                    if (!sstable.descriptor.version.isLatestVersion())
                        fail("Verify should throw RuntimeException for old sstables "+sstable);
                }
                catch (RuntimeException e)
                {}
            }
            // make sure we don't throw any exception if not checking version:
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkVersion(false).build()))
                {
                    verifier.verify();
                }
                catch (Throwable e)
                {
                    fail("Verify should throw RuntimeException for old sstables "+sstable);
                }
            }
        }
    }

    @Test
    public void testPendingAntiCompactionOldSSTables() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            loadLegacyTable("legacy_%s_simple", legacyVersion);

            boolean shouldFail = !cfs.getLiveSSTables().stream().allMatch(sstable -> sstable.descriptor.version.hasPendingRepair());
            IPartitioner p = Iterables.getFirst(cfs.getLiveSSTables(), null).getPartitioner();
            Range<Token> r = new Range<>(p.getMinimumToken(), p.getMinimumToken());
            PendingAntiCompaction.AcquisitionCallable acquisitionCallable = new PendingAntiCompaction.AcquisitionCallable(cfs, singleton(r), nextTimeUUID(), 0, 0);
            PendingAntiCompaction.AcquireResult res = acquisitionCallable.call();
            assertEquals(shouldFail, res == null);
            if (res != null)
                res.abort();
        }
    }

    @Test
    public void testAutomaticUpgrade() throws Exception
    {
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            AbstractCompactionTask act = cfs.getCompactionStrategyManager().getNextBackgroundTask(0);
            // there should be no compactions to run with auto upgrades disabled:
            assertEquals(null, act);
        }

        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        for (String legacyVersion : legacyVersions)
        {
            logger.info("Loading legacy version: {}", legacyVersion);
            truncateLegacyTables(legacyVersion);
            loadLegacyTables(legacyVersion);
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion));
            if (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
                assertTrue(cfs.metric.oldVersionSSTableCount.getValue() > 0);
            while (cfs.getLiveSSTables().stream().anyMatch(s -> !s.descriptor.version.isLatestVersion()))
            {
                CompactionManager.instance.submitBackground(cfs);
                Thread.sleep(100);
            }
            assertTrue(cfs.metric.oldVersionSSTableCount.getValue() == 0);
        }
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);
    }

    private void streamLegacyTables(String legacyVersion) throws Exception
    {
            logger.info("Streaming legacy version {}", legacyVersion);
            streamLegacyTable("legacy_%s_simple", legacyVersion);
            streamLegacyTable("legacy_%s_simple_counter", legacyVersion);
            streamLegacyTable("legacy_%s_clust", legacyVersion);
            streamLegacyTable("legacy_%s_clust_counter", legacyVersion);
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion);
        SSTableReader sstable = SSTableReader.open(null, getDescriptor(legacyVersion, table));
        IPartitioner p = sstable.getPartitioner();
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(p.getMinimumToken(), p.getToken(ByteBufferUtil.bytes("100"))));
        ranges.add(new Range<>(p.getToken(ByteBufferUtil.bytes("100")), p.getMinimumToken()));
        List<OutgoingStream> streams = Lists.newArrayList(new CassandraOutgoingFile(StreamOperation.OTHER,
                                                                                    sstable.ref(),
                                                                                    sstable.getPositionsForRanges(ranges),
                                                                                    ranges,
                                                                                    sstable.estimatedKeysForRanges(ranges)));
        new StreamPlan(StreamOperation.OTHER).transferStreams(FBUtilities.getBroadcastAddressAndPort(), streams).execute().get();
    }

    public static void truncateLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Truncating legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).truncateBlocking();
    }

    private static void compactLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Compacting legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).forceMajorCompaction();
    }

    public static void loadLegacyTables(String legacyVersion) throws Exception
    {
            logger.info("Preparing legacy version {}", legacyVersion);
            loadLegacyTable("legacy_%s_simple", legacyVersion);
            loadLegacyTable("legacy_%s_simple_counter", legacyVersion);
            loadLegacyTable("legacy_%s_clust", legacyVersion);
            loadLegacyTable("legacy_%s_clust_counter", legacyVersion);
    }

    private static void verifyCache(String legacyVersion, long startCount) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        // Only perform test if format uses cache.
        SSTableReader sstable = Iterables.getFirst(Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables(), null);
        if (!(sstable instanceof KeyCacheSupport))
            return;

        //For https://issues.apache.org/jira/browse/CASSANDRA-10778
        //Validate whether the key cache successfully saves in the presence of old keys as
        //well as loads the correct number of keys
        long endCount = CacheService.instance.keyCache.size();
        Assert.assertTrue(endCount > startCount);
        CacheService.instance.keyCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateKeyCache();
        Assert.assertEquals(startCount, CacheService.instance.keyCache.size());
        CacheService.instance.keyCache.loadSaved();
        Assert.assertEquals(endCount, CacheService.instance.keyCache.size());
    }

    private static void verifyReads(String legacyVersion)
    {
        for (int ck = 0; ck < 50; ck++)
        {
            String ckValue = Integer.toString(ck) + longString;
            for (int pk = 0; pk < 5; pk++)
            {
                logger.debug("for pk={} ck={}", pk, ck);

                String pkValue = Integer.toString(pk);
                if (ck == 0)
                {
                    readSimpleTable(legacyVersion, pkValue);
                    readSimpleCounterTable(legacyVersion, pkValue);
                }

                readClusteringTable(legacyVersion, ck, ckValue, pkValue);
                readClusteringCounterTable(legacyVersion, ckValue, pkValue);
            }
        }
    }

    private static void readClusteringCounterTable(String legacyVersion, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust_counter WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readClusteringTable(String legacyVersion, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_clust WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion), pkValue, ckValue, ckValue2, ckValue3);
        assertLegacyClustRows(3, rs);
    }

    private static void readSimpleCounterTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read legacy_{}_simple_counter", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple_counter WHERE pk=?", legacyVersion), pkValue);
        Assert.assertNotNull(rs);
        Assert.assertEquals(1, rs.size());
        Assert.assertEquals(1L, rs.one().getLong("val"));
    }

    private static void readSimpleTable(String legacyVersion, String pkValue)
    {
        logger.debug("Read simple: legacy_{}_simple", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables.legacy_%s_simple WHERE pk=?", legacyVersion), pkValue);
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
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple (pk text PRIMARY KEY, val text)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_simple_counter (pk text PRIMARY KEY, val counter)", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_counter (pk text, ck text, val counter, PRIMARY KEY (pk, ck))", legacyVersion));
    }

    private static void truncateTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter", legacyVersion));
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

    private static void loadLegacyTable(String tablePattern, String legacyVersion) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);

        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for 8 CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#version BigFormat.latestVersion.getVersion()}.
     * <p>
     * Run this test alone (e.g. from your IDE) when a new version is introduced or format changed
     * during development. I.e. remove the {@code @Ignore} annotation temporarily.
     * </p>
     */
    @Ignore // TODO: Currently this test needs to be ran alone to avoid unwanted compactions, flushes, etc to interfere
    @Test
    public void testGenerateSstables() throws Throwable
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
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
                                                         format.getLatestVersion(), valPk, "foo bar baz"));

            QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_simple_counter SET val = val + 1 WHERE pk = '%s'",
                                                         format.getLatestVersion(), valPk));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             format.getLatestVersion(), valPk, valCk + longString));
            }
        }

        StorageService.instance.forceKeyspaceFlush(LEGACY_TABLES_KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS);

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", format.getLatestVersion()));
        ksDir.tryCreateDirectories();
        copySstablesFromTestData(String.format("legacy_%s_simple", format.getLatestVersion()), ksDir, LEGACY_TABLES_KEYSPACE);
        copySstablesFromTestData(String.format("legacy_%s_simple_counter", format.getLatestVersion()), ksDir, LEGACY_TABLES_KEYSPACE);
        copySstablesFromTestData(String.format("legacy_%s_clust", format.getLatestVersion()), ksDir, LEGACY_TABLES_KEYSPACE);
        copySstablesFromTestData(String.format("legacy_%s_clust_counter", format.getLatestVersion()), ksDir, LEGACY_TABLES_KEYSPACE);
    }

    public static void copySstablesFromTestData(String table, File ksDir, String ks) throws IOException
    {
        File cfDir = new File(ksDir, table);
        cfDir.tryCreateDirectory();

        for (File srcDir : Keyspace.open(ks).getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File file : srcDir.tryList())
            {
                copyFile(cfDir, file);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File cfDir) throws IOException
    {
        File tableDir = getTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + tableDir + " was not found", tableDir.isDirectory());
        for (File file : tableDir.tryList())
        {
            copyFile(cfDir, file);
        }
    }

    private static File getTableDir(String legacyVersion, String table)
    {
        return new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    public static void copyFile(File cfDir, File file) throws IOException
    {
        byte[] buf = new byte[65536];
        if (file.isFile())
        {
            File target = new File(cfDir, file.name());
            int rd;
            try (FileInputStreamPlus is = new FileInputStreamPlus(file);
                 FileOutputStreamPlus os = new FileOutputStreamPlus(target);) {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
                }
        }
    }
}
