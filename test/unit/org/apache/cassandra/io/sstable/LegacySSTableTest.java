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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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
import org.apache.cassandra.exceptions.InvalidRequestException;
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

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    public static File LEGACY_SSTABLE_ROOT;

    private static final String LEGACY_TABLES_KEYSPACE = "legacy_tables";

    /**
     * When adding a new sstable version, add that one here.
     * See {@link #testGenerateSstables()} to generate sstables.
     * Take care on commit as you need to add the sstable files using {@code git add -f}
     *
     * There are two me sstables, where the sequence number indicates the C* version they come from.
     * For example:
     *     me-3025-big-* sstables are generated from 3.0.25
     *     me-31111-big-* sstables are generated from 3.11.11
     * Both exist because of differences introduced in 3.6 (and 3.11) in how frozen multi-cell headers are serialised
     *  without the sstable format `me` being bumped, ref CASSANDRA-15035
     *
     * Sequence numbers represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables()
     */
    public static String[] legacyVersions = null;

    // Get all versions up to the current one. Useful for testing in compatibility mode C18301
    private static String[] getValidLegacyVersions()
    {
        String[] versions = {"da", "oa", "nb", "na", "me", "md", "mc", "mb", "ma"};
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
    protected Descriptor getDescriptor(File dir) throws IOException
    {
        Path file = Files.list(dir.toPath())
                .findFirst()
                .orElseThrow(() -> new RuntimeException(String.format("No files for path=%s", dir.absolutePath())));

        return Descriptor.fromFile(new File(file));
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
        loadLegacyTable("mc", "inaccurate_min_max");

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
    public void testVerifyOldSimpleSSTables() throws IOException
    {
        verifyOldSSTables("simple");
    }

    @Test
    public void testVerifyOldTupleSSTables() throws IOException
    {
        verifyOldSSTables("tuple");
    }

    @Test
    public void testVerifyOldDroppedTupleSSTables() throws IOException
    {
        try {
            for (String legacyVersion : legacyVersions)
            {
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val2", legacyVersion));
                QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val3", legacyVersion));
                // dropping non-frozen UDTs disabled, see AlterTableStatement.DropColumns.dropColumn(..)
                //QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple DROP val4", legacyVersion));
            }

            verifyOldSSTables("tuple");
        }
        finally
        {
            for (String legacyVersion : legacyVersions)
            {
                alterTableAddColumn(legacyVersion, "val frozen<tuple<set<int>,set<text>>>");
                alterTableAddColumn(legacyVersion, "val2 tuple<set<int>,set<text>>");
                try
                {
                    alterTableAddColumn(legacyVersion, String.format("val3 frozen<legacy_%s_tuple_udt>", legacyVersion));
                    throw new AssertionError(String.format("Against legacyVersion %s expected InvalidRequestException: Cannot re-add previously dropped column 'val3' of type frozen<legacy_da_tuple_udt>, incompatible with previous type frozen<tuple<frozen<tuple<text, text>>>>", legacyVersion));
                }
                catch (InvalidRequestException ex)
                {
                    // expected
                    // InvalidRequestException: Cannot re-add previously dropped column 'val3' of type frozen<legacy_da_tuple_udt>, incompatible with previous type frozen<tuple<frozen<tuple<text, text>>>>
                }
                // dropping non-frozen UDTs disabled, see AlterTableStatement.DropColumns.dropColumn(..)
                //alterTableAddColumn(legacyVersion, String.format("val4 legacy_%s_tuple_udt", legacyVersion));
            }
        }
    }

    private static void alterTableAddColumn(String legacyVersion, String column_definition)
    {
        QueryProcessor.executeInternal(String.format("ALTER TABLE legacy_tables.legacy_%s_tuple ADD IF NOT EXISTS %s", legacyVersion, column_definition));
    }

    private void verifyOldSSTables(String tableSuffix) throws IOException
    {
        for (String legacyVersion : legacyVersions)
        {
            ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_%s", legacyVersion, tableSuffix));
            loadLegacyTable(legacyVersion, tableSuffix);

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
            loadLegacyTable(legacyVersion, "simple");

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
        streamLegacyTable("legacy_%s_tuple", legacyVersion);
        streamLegacyTable("legacy_%s_clust_be_index_summary", legacyVersion);
    }

    private void streamLegacyTable(String tablePattern, String legacyVersion) throws Exception
    {
        String table = String.format(tablePattern, legacyVersion);
        // streaming can mutate test data (rewrite IndexSummary, so we have to copy them)
        File testDataDir = new File(tempFolder.newFolder(LEGACY_TABLES_KEYSPACE, table));
        copySstablesToTestData(legacyVersion, table, testDataDir);
        Descriptor descriptor = getDescriptor(testDataDir);
        if (null != descriptor)
        {
            SSTableReader sstable = SSTableReader.open(null, descriptor);
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
    }

    public static void truncateLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Truncating legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).truncateBlocking();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_be_index_summary", legacyVersion)).truncateBlocking();
        CacheService.instance.invalidateCounterCache();
        CacheService.instance.invalidateKeyCache();
    }

    private static void compactLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Compacting legacy version {}", legacyVersion);
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_simple_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_counter", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_tuple", legacyVersion)).forceMajorCompaction();
        Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(String.format("legacy_%s_clust_be_index_summary", legacyVersion)).forceMajorCompaction();
    }

    public static void loadLegacyTables(String legacyVersion) throws Exception
    {
        logger.info("Preparing legacy version {}", legacyVersion);
        loadLegacyTable(legacyVersion, "simple");
        loadLegacyTable(legacyVersion, "simple_counter");
        loadLegacyTable(legacyVersion, "clust");
        loadLegacyTable(legacyVersion, "clust_counter");
        loadLegacyTable(legacyVersion, "tuple");
        loadLegacyTable(legacyVersion, "clust_be_index_summary");
    }

    private static void verifyCache(String legacyVersion, long startCount) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        // Only perform test if format uses cache.
        SSTableReader sstable = Iterables.getFirst(Keyspace.open("legacy_tables").getColumnFamilyStore(String.format("legacy_%s_simple", legacyVersion)).getLiveSSTables(), null);
        if (!(sstable instanceof KeyCacheSupport) || DatabaseDescriptor.getKeyCacheSizeInMiB() == 0)
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

                readClusteringTable("legacy_%s_clust", legacyVersion, ck, ckValue, pkValue);
                readClusteringTable("legacy_%s_clust_be_index_summary", legacyVersion, ck, ckValue, pkValue);
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

    private static void readClusteringTable(String tableName, String legacyVersion, int ck, String ckValue, String pkValue)
    {
        logger.debug("Read legacy_{}_clust", legacyVersion);
        UntypedResultSet rs;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables." + tableName + " WHERE pk=? AND ck=?", legacyVersion), pkValue, ckValue);
        assertLegacyClustRows(1, rs);

        String ckValue2 = Integer.toString(ck < 10 ? 40 : ck - 1) + longString;
        String ckValue3 = Integer.toString(ck > 39 ? 10 : ck + 1) + longString;
        rs = QueryProcessor.executeInternal(String.format("SELECT val FROM legacy_tables." + tableName + " WHERE pk=? AND ck IN (?, ?, ?)", legacyVersion), pkValue, ckValue, ckValue2, ckValue3);
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
        QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%s_clust_be_index_summary (pk text, ck text, val text, PRIMARY KEY (pk, ck))", legacyVersion));

        QueryProcessor.executeInternal(String.format("CREATE TYPE legacy_tables.legacy_%s_tuple_udt (name tuple<text,text>)", legacyVersion));

        if (legacyVersion.startsWith("m"))
        {
            // sstable formats possibly from 3.0.x would have had a schema with everything frozen
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                    "val frozen<tuple<set<int>,set<text>>>, val2 frozen<tuple<set<int>,set<text>>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 frozen<legacy_%1$s_tuple_udt>, extra text)", legacyVersion));
        }
        else
        {
            QueryProcessor.executeInternal(String.format("CREATE TABLE legacy_tables.legacy_%1$s_tuple (pk text PRIMARY KEY, " +
                "val frozen<tuple<set<int>,set<text>>>, val2 tuple<set<int>,set<text>>, val3 frozen<legacy_%1$s_tuple_udt>, val4 legacy_%1$s_tuple_udt, extra text)", legacyVersion));
        }
    }

    private static void truncateTables(String legacyVersion)
    {
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_simple_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_counter", legacyVersion));
        QueryProcessor.executeInternal(String.format("TRUNCATE legacy_tables.legacy_%s_clust_be_index_summary", legacyVersion));
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

    private static void loadLegacyTable(String legacyVersion, String tableSuffix) throws IOException
    {
        String table = String.format("legacy_%s_%s", legacyVersion, tableSuffix);
        logger.info("Loading legacy table {}", table);

        ColumnFamilyStore cfs = Keyspace.open(LEGACY_TABLES_KEYSPACE).getColumnFamilyStore(table);

        for (File cfDir : cfs.getDirectories().getCFDirectories())
        {
            copySstablesToTestData(legacyVersion, table, cfDir);
        }

        cfs.loadNewSSTables();
    }

    /**
     * Generates sstables for CQL tables (see {@link #createTables(String)}) in <i>current</i>
     * sstable format (version) into {@code test/data/legacy-sstables/VERSION}, where
     * {@code VERSION} matches {@link Version#version BigFormat.latestVersion.getVersion()}.
     *
     * Sequence numbers are changed to represent the C* version used when creating the SSTable.
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

            QueryProcessor.executeInternal(
                    String.format("INSERT INTO legacy_tables.legacy_%s_tuple (pk, val, val2, val3, val4, extra)"
                                    + " VALUES ('%s', ({1,2,3},{'a','b','c'}), ({1,2,3},{'a','b','c'}), {name: ('abc','def')}, {name: ('abc','def')}, '%s')",
                                  format.getLatestVersion(), valPk, randomString));

            for (int ck = 0; ck < 50; ck++)
            {
                String valCk = Integer.toString(ck);

                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

                QueryProcessor.executeInternal(String.format("UPDATE legacy_tables.legacy_%s_clust_counter SET val = val + 1 WHERE pk = '%s' AND ck='%s'",
                                                             format.getLatestVersion(), valPk, valCk + longString));

                // note: to emulate BE for offsets in Summary you can comment temporary the following line:
                // offset = Integer.reverseBytes(offset);
                // in org.apache.cassandra.io.sstable.indexsummary.IndexSummary.IndexSummarySerializer.serialize
                QueryProcessor.executeInternal(String.format("INSERT INTO legacy_tables.legacy_%s_clust_be_index_summary (pk, ck, val) VALUES ('%s', '%s', '%s')",
                                                             format.getLatestVersion(), valPk, valCk + longString, randomString));

            }
        }

        StorageService.instance.forceKeyspaceFlush(LEGACY_TABLES_KEYSPACE, ColumnFamilyStore.FlushReason.UNIT_TESTS);

        File ksDir = new File(LEGACY_SSTABLE_ROOT, String.format("%s/legacy_tables", format.getLatestVersion()));
        ksDir.tryCreateDirectories();
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_simple_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust_counter", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_tuple", ksDir);
        copySstablesFromTestData(format.getLatestVersion(), "legacy_%s_clust_be_index_summary", ksDir);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir) throws IOException
    {
        copySstablesFromTestData(legacyVersion, tablePattern, ksDir, LEGACY_TABLES_KEYSPACE);
    }

    public static void copySstablesFromTestData(Version legacyVersion, String tablePattern, File ksDir, String ks) throws IOException
    {
        String table = String.format(tablePattern, legacyVersion);
        File cfDir = new File(ksDir, table);
        cfDir.tryCreateDirectory();

        for (File srcDir : Keyspace.open(ks).getColumnFamilyStore(table).getDirectories().getCFDirectories())
        {
            for (File sourceFile : srcDir.tryList())
            {
                // Sequence IDs represent the C* version used when creating the SSTable, i.e. with #testGenerateSstables() (if not uuid based)
                String newSeqId = FBUtilities.getReleaseVersionString().split("-")[0].replaceAll("[^0-9]", "");
                File target = new File(cfDir, sourceFile.name().replace(legacyVersion + "-1-", legacyVersion + "-" + newSeqId + "-"));
                copyFile(sourceFile, target);
            }
        }
    }

    private static void copySstablesToTestData(String legacyVersion, String table, File targetDir) throws IOException
    {
        File testDataTableDir = getTestDataTableDir(legacyVersion, table);
        Assert.assertTrue("The table directory " + testDataTableDir + " was not found", testDataTableDir.isDirectory());
        for (File sourceTestFile : testDataTableDir.tryList())
            copyFileToDir(sourceTestFile, targetDir);
    }

    private static File getTestDataTableDir(File parentDir, String legacyVersion, String table)
    {
        return new File(parentDir, String.format("%s/legacy_tables/%s", legacyVersion, table));
    }

    private static File getTestDataTableDir(String legacyVersion, String table)
    {
        return getTestDataTableDir(LEGACY_SSTABLE_ROOT, legacyVersion, table);
    }

    public static void copyFileToDir(File sourceFile, File targetDir) throws IOException
    {
        copyFile(sourceFile,  new File(targetDir, sourceFile.name()));
    }

    public static void copyFile(File sourceFile, File targetFile) throws IOException
    {
        byte[] buf = new byte[65536];
        if (sourceFile.isFile())
        {
            int rd;
            try (FileInputStreamPlus is = new FileInputStreamPlus(sourceFile);
                 FileOutputStreamPlus os = new FileOutputStreamPlus(targetFile);)
            {
                while ((rd = is.read(buf)) >= 0)
                    os.write(buf, 0, rd);
            }
        }
    }
}
