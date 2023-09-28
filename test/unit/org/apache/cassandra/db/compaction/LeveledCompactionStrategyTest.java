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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableRepairStatusChanged;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.ValidationManager;
import org.apache.cassandra.repair.Validator;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.awaitility.Awaitility;

import static java.util.Collections.singleton;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LeveledCompactionStrategyTest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategyTest.class);

    private static final String KEYSPACE1 = "LeveledCompactionStrategyTest";
    private static final String CF_STANDARDDLEVELED = "StandardLeveled";
    private Keyspace keyspace;
    private ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        CassandraRelevantProperties.STREAMING_HISTOGRAM_ROUND_SECONDS.setInt(1);

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARDDLEVELED)
                                                .compaction(CompactionParams.lcs(Collections.singletonMap("sstable_size_in_mb", "1"))));
        }

    @Before
    public void enableCompaction()
    {
        keyspace = Keyspace.open(KEYSPACE1);
        cfs = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED);
        cfs.enableAutoCompaction();
    }

    /**
     * Since we use StandardLeveled CF for every test, we want to clean up after the test.
     */
    @After
    public void truncateSTandardLeveled()
    {
        cfs.truncateBlocking();
    }

    /**
     * Ensure that the grouping operation preserves the levels of grouped tables
     */
    @Test
    public void testGrouperLevels() throws Exception{
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KiB value, make it easy to have multiple files

        //Need entropy to prevent compression so size is predictable with compression enabled/disabled
        new Random().nextBytes(value.array());

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math

        int l1Count = strategyManager.getSSTableCountPerLevel()[1];
        int l2Count = strategyManager.getSSTableCountPerLevel()[2];
        if (l1Count == 0 || l2Count == 0)
        {
            logger.error("L1 or L2 has 0 sstables. Expected > 0 on both.");
            logger.error("L1: " + l1Count);
            logger.error("L2: " + l2Count);
            Assert.fail();
        }

        Collection<Collection<SSTableReader>> groupedSSTables = cfs.getCompactionStrategyManager().groupSSTablesForAntiCompaction(cfs.getLiveSSTables());
        for (Collection<SSTableReader> sstableGroup : groupedSSTables)
        {
            int groupLevel = -1;
            Iterator<SSTableReader> it = sstableGroup.iterator();
            while (it.hasNext())
            {

                SSTableReader sstable = it.next();
                int tableLevel = sstable.getSSTableLevel();
                if (groupLevel == -1)
                    groupLevel = tableLevel;
                assert groupLevel == tableLevel;
            }
        }
    }

    /*
     * This exercises in particular the code of #4142
     */
    @Test
    public void testValidationMultipleSSTablePerLevel() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KiB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }

        waitForLeveling(cfs);
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        // Checking we're not completely bad at math
        assertTrue(strategyManager.getSSTableCountPerLevel()[1] > 0);
        assertTrue(strategyManager.getSSTableCountPerLevel()[2] > 0);

        Range<Token> range = new Range<>(Util.token(""), Util.token(""));
        long gcBefore = keyspace.getColumnFamilyStore(CF_STANDARDDLEVELED).gcBefore(FBUtilities.nowInSeconds());
        TimeUUID parentRepSession = nextTimeUUID();
        ActiveRepairService.instance().registerParentRepairSession(parentRepSession,
                                                                   FBUtilities.getBroadcastAddressAndPort(),
                                                                   Arrays.asList(cfs),
                                                                   Arrays.asList(range),
                                                                   false,
                                                                   ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                   true,
                                                                   PreviewKind.NONE);
        RepairJobDesc desc = new RepairJobDesc(parentRepSession, nextTimeUUID(), KEYSPACE1, CF_STANDARDDLEVELED, Arrays.asList(range));
        Validator validator = new Validator(new ValidationState(Clock.Global.clock(), desc, FBUtilities.getBroadcastAddressAndPort()), gcBefore, PreviewKind.NONE);

        ValidationManager.instance.submitValidation(cfs, validator).get();
    }

    /**
     * wait for leveled compaction to quiesce on the given columnfamily
     */
    public static void waitForLeveling(ColumnFamilyStore cfs) throws InterruptedException
    {
        CompactionStrategyManager strategyManager = cfs.getCompactionStrategyManager();
        while (true)
        {
            // since we run several compaction strategies we wait until L0 in all strategies is empty and
            // atleast one L1+ is non-empty. In these tests we always run a single data directory with only unrepaired data
            // so it should be good enough
            boolean allL0Empty = true;
            boolean anyL1NonEmpty = false;
            for (List<AbstractCompactionStrategy> strategies : strategyManager.getStrategies())
            {
                for (AbstractCompactionStrategy strategy : strategies)
                {
                    if (!(strategy instanceof LeveledCompactionStrategy))
                        return;
                    // note that we check > 1 here, if there is too little data in L0, we don't compact it up to L1
                    if (((LeveledCompactionStrategy)strategy).getLevelSize(0) > 1)
                        allL0Empty = false;
                    for (int i = 1; i < 5; i++)
                        if (((LeveledCompactionStrategy)strategy).getLevelSize(i) > 0)
                            anyL1NonEmpty = true;
                }
            }
            if (allL0Empty && anyL1NonEmpty)
                return;
            Thread.sleep(100);
        }
    }

    @Test
    public void testCompactionProgress() throws Exception
    {
        // make sure we have SSTables in L1
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 2;
        int columns = 10;
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }

        waitForLeveling(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);
        assert strategy.getLevelSize(1) > 0;

        // get LeveledScanner for level 1 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(1);
        List<ISSTableScanner> scanners = strategy.getScanners(sstables).scanners;
        assertEquals(1, scanners.size()); // should be one per level
        ISSTableScanner scanner = scanners.get(0);
        // scan through to the end
        while (scanner.hasNext())
            scanner.next();

        // scanner.getCurrentPosition should be equal to total bytes of L1 sstables
        assertEquals(scanner.getCurrentPosition(), SSTableReader.getTotalUncompressedBytes(sstables));
    }

    @Test
    public void testMutateLevel() throws Exception
    {
        cfs.disableAutoCompaction();
        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KiB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }
        Util.flush(cfs);
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(1).get(0);
        cfs.forceMajorCompaction();

        for (SSTableReader s : cfs.getLiveSSTables())
        {
            assertTrue(s.getSSTableLevel() != 6 && s.getSSTableLevel() > 0);
            strategy.manifest.remove(s);
            s.descriptor.getMetadataSerializer().mutateLevel(s.descriptor, 6);
            s.reloadSSTableMetadata();
            strategy.manifest.addSSTables(Collections.singleton(s));
        }
        // verify that all sstables in the changed set is level 6
        for (SSTableReader s : cfs.getLiveSSTables())
            assertEquals(6, s.getSSTableLevel());

        int[] levels = strategy.manifest.getAllLevelSize();
        // verify that the manifest has correct amount of sstables
        assertEquals(cfs.getLiveSSTables().size(), levels[6]);
    }

    @Test
    public void testNewRepairedSSTable() throws Exception
    {
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b); // 100 KiB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 40;
        int columns = 20;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }
        waitForLeveling(cfs);
        cfs.disableAutoCompaction();

        while(CompactionManager.instance.isCompacting(Arrays.asList(cfs), (sstable) -> true))
            Thread.sleep(100);

        CompactionStrategyManager manager = cfs.getCompactionStrategyManager();
        List<List<AbstractCompactionStrategy>> strategies = manager.getStrategies();
        LeveledCompactionStrategy repaired = (LeveledCompactionStrategy) strategies.get(0).get(0);
        LeveledCompactionStrategy unrepaired = (LeveledCompactionStrategy) strategies.get(1).get(0);
        assertEquals(0, repaired.manifest.getLevelCount() );
        assertEquals(2, unrepaired.manifest.getLevelCount());
        assertTrue(manager.getSSTableCountPerLevel()[1] > 0);
        assertTrue(manager.getSSTableCountPerLevel()[2] > 0);

        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertFalse(sstable.isRepaired());

        int sstableCount = unrepaired.manifest.getSSTables().size();
        // we only have unrepaired sstables:
        assertEquals(sstableCount, cfs.getLiveSSTables().size());

        SSTableReader sstable1 = unrepaired.manifest.getLevel(2).iterator().next();
        SSTableReader sstable2 = unrepaired.manifest.getLevel(1).iterator().next();

        sstable1.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable1.descriptor, System.currentTimeMillis(), null, false);
        sstable1.reloadSSTableMetadata();
        assertTrue(sstable1.isRepaired());

        manager.handleNotification(new SSTableRepairStatusChanged(Arrays.asList(sstable1)), this);

        int repairedSSTableCount = repaired.manifest.getSSTables().size();
        assertEquals(1, repairedSSTableCount);
        // make sure the repaired sstable ends up in the same level in the repaired manifest:
        assertTrue(repaired.manifest.getLevel(2).contains(sstable1));
        // and that it is gone from unrepaired
        assertFalse(unrepaired.manifest.getLevel(2).contains(sstable1));

        unrepaired.removeSSTable(sstable2);
        manager.handleNotification(new SSTableAddedNotification(singleton(sstable2), null), this);
        assertTrue(unrepaired.manifest.getLevel(1).contains(sstable2));
        assertFalse(repaired.manifest.getLevel(1).contains(sstable2));
    }

    @Test
    public void testTokenRangeCompaction() throws Exception
    {
        // Remove any existing data, so we can start out clean with predictable number of sstables
        cfs.truncateBlocking();

        // Disable auto compaction so cassandra does not compact
        CompactionManager.instance.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KiB value, make it easy to have multiple files

        DecoratedKey key1 = Util.dk(String.valueOf(1));
        DecoratedKey key2 = Util.dk(String.valueOf(2));
        List<DecoratedKey> keys = Arrays.asList(key1, key2);
        int numIterations = 10;
        int columns = 2;

        // Add enough data to trigger multiple sstables.

        // create 10 sstables that contain data for both key1 and key2
        for (int i = 0; i < numIterations; i++)
        {
            for (DecoratedKey key : keys)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
            }
            Util.flush(cfs);
        }

        // create 20 more sstables with 10 containing data for key1 and other 10 containing data for key2
        for (int i = 0; i < numIterations; i++) {
            for (DecoratedKey key : keys) {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
                Util.flush(cfs);
            }
        }

        // We should have a total of 30 sstables by now
        assertEquals(30, cfs.getLiveSSTables().size());

        // Compact just the tables with key2. The token ranges for compaction are interpreted as closed intervals,
        // so we can use [token, token] to select a single token.
        Range<Token> tokenRange = new Range<>(key2.getToken(), key2.getToken());
        Collection<Range<Token>> tokenRanges = singleton(tokenRange);
        cfs.forceCompactionForTokenRange(tokenRanges);

        Awaitility.await().until(() -> !CompactionManager.instance.isCompacting(singleton(cfs), sstable -> true));

        // 20 tables that have key2 should have been compacted in to 1 table resulting in 11 (30-20+1)
        assertEquals(11, cfs.getLiveSSTables().size());

        // Compact just the tables with key1. At this point all 11 tables should have key1
        Range<Token> tokenRange2 = new Range<>(key1.getToken(), key1.getToken());
        Collection<Range<Token>> tokenRanges2 = singleton(tokenRange2);
        cfs.forceCompactionForTokenRange(tokenRanges2);

        Awaitility.await().until(() -> !CompactionManager.instance.isCompacting(singleton(cfs), sstable -> true));

        // the 11 tables containing key1 should all compact to 1 table
        assertEquals(1, cfs.getLiveSSTables().size());
        // Set it up again
        cfs.truncateBlocking();

        // create 10 sstables that contain data for both key1 and key2
        for (int i = 0; i < numIterations; i++)
        {
            for (DecoratedKey key : keys)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
            }
            Util.flush(cfs);
        }

        // create 20 more sstables with 10 containing data for key1 and other 10 containing data for key2
        for (int i = 0; i < numIterations; i++)
        {
            for (DecoratedKey key : keys)
            {
                UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), key);
                for (int c = 0; c < columns; c++)
                    update.newRow("column" + c).add("val", value);
                update.applyUnsafe();
                Util.flush(cfs);
            }
        }

        // We should have a total of 30 sstables again
        assertEquals(30, cfs.getLiveSSTables().size());

        // This time, we're going to make sure the token range wraps around, to cover the full range.
        // Note that the ranges used by compaction are interpreted as closed intervals, so it will be [32, 31].
        Range<Token> wrappingRange;
        if (key1.getToken().compareTo(key2.getToken()) < 0)
        {
            wrappingRange = new Range<>(key2.getToken(), key1.getToken());
        }
        else
        {
            wrappingRange = new Range<>(key1.getToken(), key2.getToken());
        }
        Collection<Range<Token>> wrappingRanges = singleton(wrappingRange);
        cfs.forceCompactionForTokenRange(wrappingRanges);

        Awaitility.await().until(() -> !CompactionManager.instance.isCompacting(singleton(cfs), sstable -> true));

        // should all compact to 1 table
        assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void testCompactionCandidateOrdering() throws Exception
    {
        // add some data
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 4;
        int columns = 10;
        // Just keep sstables in L0 for this test
        cfs.disableAutoCompaction();
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
            Util.flush(cfs);
        }
        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) (cfs.getCompactionStrategyManager()).getStrategies().get(1).get(0);
        // get readers for level 0 sstables
        Collection<SSTableReader> sstables = strategy.manifest.getLevel(0);
        Collection<SSTableReader> sortedCandidates = strategy.manifest.ageSortedSSTables(sstables);
        assertTrue(String.format("More than 1 sstable required for test, found: %d .", sortedCandidates.size()), sortedCandidates.size() > 1);
        long lastMaxTimeStamp = Long.MIN_VALUE;
        for (SSTableReader sstable : sortedCandidates)
        {
            assertTrue(String.format("SStables not sorted into oldest to newest by maxTimestamp. Current sstable: %d , last sstable: %d", sstable.getMaxTimestamp(), lastMaxTimeStamp),
                       sstable.getMaxTimestamp() > lastMaxTimeStamp);
            lastMaxTimeStamp = sstable.getMaxTimestamp();
        }
    }

    @Test
    public void testDisableSTCSInL0() throws IOException
    {
        /*
        First creates a bunch of sstables in L1, then overloads L0 with 50 sstables. Now with STCS in L0 enabled
        we should get a compaction task where the target level is 0, then we disable STCS-in-L0 and make sure that
        the compaction task we get targets L1 or higher.
         */
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        cfs.setCompactionParameters(localOptions);
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0; i < 11; i++)
        {
            SSTableReader l1sstable = MockSchema.sstable(i, 1 * 1024 * 1024, cfs);
            l1sstable.descriptor.getMetadataSerializer().mutateLevel(l1sstable.descriptor, 1);
            l1sstable.reloadSSTableMetadata();
            sstables.add(l1sstable);
        }

        for (int i = 100; i < 150; i++)
            sstables.add(MockSchema.sstable(i, 1 * 1024 * 1024, cfs));

        cfs.disableAutoCompaction();
        cfs.addSSTables(sstables);
        assertEquals(0, getTaskLevel(cfs));

        try
        {
            CompactionManager.instance.setDisableSTCSInL0(true);
            assertTrue(getTaskLevel(cfs) > 0);
        }
        finally
        {
            CompactionManager.instance.setDisableSTCSInL0(false);
        }
    }

    private int getTaskLevel(ColumnFamilyStore cfs)
    {
        int level = -1;
        for (List<AbstractCompactionStrategy> strategies : cfs.getCompactionStrategyManager().getStrategies())
        {
            for (AbstractCompactionStrategy strategy : strategies)
            {
                AbstractCompactionTask task = strategy.getNextBackgroundTask(0);
                if (task != null)
                {
                    try
                    {
                        assertTrue(task instanceof LeveledCompactionTask);
                        LeveledCompactionTask lcsTask = (LeveledCompactionTask) task;
                        level = Math.max(level, lcsTask.getLevel());
                    }
                    finally
                    {
                        task.transaction.abort();
                    }
                }
            }
        }
        return level;
    }

    @Test
    public void testAddingOverlapping()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        List<SSTableReader> currentLevel = new ArrayList<>();
        int gen = 1;
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 10, 20, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 21, 30, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 51, 100, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 80, 120, 1, cfs));
        currentLevel.add(MockSchema.sstableWithLevel(gen++, 90, 150, 1, cfs));

        lm.addSSTables(currentLevel);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertLevelsEqual(lm.getLevel(0), currentLevel.subList(3, 5));

        List<SSTableReader> newSSTables = new ArrayList<>();
        // this sstable last token is the same as the first token of L1 above, should get sent to L0:
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 5, 10, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 30, 40, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 100, 140, 1, cfs));
        lm.addSSTables(newSSTables);
        assertLevelsEqual(lm.getLevel(1), currentLevel.subList(0, 3));
        assertEquals(0, newSSTables.get(0).getSSTableLevel());
        assertTrue(lm.getLevel(0).containsAll(newSSTables));

        newSSTables.clear();
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 100, 140, 1, cfs));
        newSSTables.add(MockSchema.sstableWithLevel(gen++, 120, 140, 1, cfs));
        lm.addSSTables(newSSTables);
        List<SSTableReader> newL1 = new ArrayList<>(currentLevel.subList(0, 3));
        newL1.add(newSSTables.get(1));
        assertLevelsEqual(lm.getLevel(1), newL1);
        newSSTables.remove(1);
        assertTrue(newSSTables.stream().allMatch(s -> s.getSSTableLevel() == 0));
        assertTrue(lm.getLevel(0).containsAll(newSSTables));
    }

    @Test
    public void singleTokenSSTableTest()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        List<SSTableReader> expectedL1 = new ArrayList<>();

        int gen = 1;
        // single sstable, single token (100)
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 100, 100, 1, cfs));
        lm.addSSTables(expectedL1);

        List<SSTableReader> expectedL0 = new ArrayList<>();

        // should get moved to L0:
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 99, 101, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 100, 101, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 99, 100, 1, cfs));
        expectedL0.add(MockSchema.sstableWithLevel(gen++, 100, 100, 1, cfs));
        lm.addSSTables(expectedL0);

        assertLevelsEqual(expectedL0, lm.getLevel(0));
        assertTrue(expectedL0.stream().allMatch(s -> s.getSSTableLevel() == 0));
        assertLevelsEqual(expectedL1, lm.getLevel(1));
        assertTrue(expectedL1.stream().allMatch(s -> s.getSSTableLevel() == 1));

        // should work:
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 98, 99, 1, cfs));
        expectedL1.add(MockSchema.sstableWithLevel(gen++, 101, 101, 1, cfs));
        lm.addSSTables(expectedL1.subList(1, expectedL1.size()));
        assertLevelsEqual(expectedL1, lm.getLevel(1));
    }

    @Test
    public void randomMultiLevelAddTest()
    {
        int iterations = 100;
        int levelCount = 9;

        ColumnFamilyStore cfs = MockSchema.newCFS();
        LeveledManifest lm = new LeveledManifest(cfs, 10, 10, new SizeTieredCompactionStrategyOptions());
        long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        List<SSTableReader> newLevels = generateNewRandomLevels(cfs, 40, levelCount, 0, r);

        int sstableCount = newLevels.size();
        lm.addSSTables(newLevels);

        int [] expectedLevelSizes = lm.getAllLevelSize();

        for (int j = 0; j < iterations; j++)
        {
            newLevels = generateNewRandomLevels(cfs, 20, levelCount, sstableCount, r);
            sstableCount += newLevels.size();

            int[] canAdd = canAdd(lm, newLevels, levelCount);
            for (int i = 0; i < levelCount; i++)
                expectedLevelSizes[i] += canAdd[i];
            lm.addSSTables(newLevels);
        }

        // and verify no levels overlap
        int actualSSTableCount = 0;
        for (int i = 0; i < levelCount; i++)
        {
            actualSSTableCount += lm.getLevelSize(i);
            List<SSTableReader> level = new ArrayList<>(lm.getLevel(i));
            int lvl = i;
            assertTrue(level.stream().allMatch(s -> s.getSSTableLevel() == lvl));
            if (i > 0)
            {
                level.sort(SSTableReader.firstKeyComparator);
                SSTableReader prev = null;
                for (SSTableReader sstable : level)
                {
                    if (prev != null && sstable.getFirst().compareTo(prev.getLast()) <= 0)
                    {
                        String levelStr = level.stream().map(s -> String.format("[%s, %s]", s.getFirst(), s.getLast())).collect(Collectors.joining(", "));
                        String overlap = String.format("sstable [%s, %s] overlaps with [%s, %s] in level %d (%s) ", sstable.getFirst(), sstable.getLast(), prev.getFirst(), prev.getLast(), i, levelStr);
                        Assert.fail("[seed = "+seed+"] overlap in level "+lvl+": " + overlap);
                    }
                    prev = sstable;
                }
            }
        }
        assertEquals(sstableCount, actualSSTableCount);
        for (int i = 0; i < levelCount; i++)
            assertEquals("[seed = " + seed + "] wrong sstable count in level = " + i, expectedLevelSizes[i], lm.getLevel(i).size());
    }

    private static List<SSTableReader> generateNewRandomLevels(ColumnFamilyStore cfs, int maxSSTableCountPerLevel, int levelCount, int startGen, Random r)
    {
        List<SSTableReader> newLevels = new ArrayList<>();
        for (int level = 0; level < levelCount; level++)
        {
            int numLevelSSTables = r.nextInt(maxSSTableCountPerLevel) + 1;
            List<Integer> tokens = new ArrayList<>(numLevelSSTables * 2);

            for (int i = 0; i < numLevelSSTables * 2; i++)
                tokens.add(r.nextInt(4000));
            Collections.sort(tokens);
            for (int i = 0; i < tokens.size() - 1; i += 2)
            {
                SSTableReader sstable = MockSchema.sstableWithLevel(++startGen, tokens.get(i), tokens.get(i + 1), level, cfs);
                newLevels.add(sstable);
            }
        }
        return newLevels;
    }
    @Test
    public void testPerLevelSizeBytes() throws IOException
    {
        byte [] b = new byte[100];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        int rows = 5;
        int columns = 5;

        cfs.disableAutoCompaction();
        for (int r = 0; r < rows; r++)
        {
            UpdateBuilder update = UpdateBuilder.create(cfs.metadata(), String.valueOf(r));
            for (int c = 0; c < columns; c++)
                update.newRow("column" + c).add("val", value);
            update.applyUnsafe();
        }
        Util.flush(cfs);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        long [] levelSizes = cfs.getPerLevelSizeBytes();
        for (int i = 0; i < levelSizes.length; i++)
        {
            if (i != 0)
                assertEquals(0, levelSizes[i]);
            else
                assertEquals(sstable.onDiskLength(), levelSizes[i]);
        }

        assertEquals(sstable.onDiskLength(), cfs.getPerLevelSizeBytes()[0]);

        LeveledCompactionStrategy strategy = (LeveledCompactionStrategy) ( cfs.getCompactionStrategyManager()).getStrategies().get(1).get(0);
        strategy.manifest.remove(sstable);
        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
        sstable.reloadSSTableMetadata();
        strategy.manifest.addSSTables(Collections.singleton(sstable));

        levelSizes = cfs.getPerLevelSizeBytes();
        for (int i = 0; i < levelSizes.length; i++)
        {
            if (i != 2)
                assertEquals(0, levelSizes[i]);
            else
                assertEquals(sstable.onDiskLength(), levelSizes[i]);
        }

    }

    /**
     * brute-force checks if the new sstables can be added to the correct level in manifest
     *
     * @return count of expected sstables to add to each level
     */
    private static int[] canAdd(LeveledManifest lm, List<SSTableReader> newSSTables, int levelCount)
    {
        Map<Integer, Collection<SSTableReader>> sstableGroups = new HashMap<>();
        newSSTables.forEach(s -> sstableGroups.computeIfAbsent(s.getSSTableLevel(), k -> new ArrayList<>()).add(s));

        int[] canAdd = new int[levelCount];
        for (Map.Entry<Integer, Collection<SSTableReader>> lvlGroup : sstableGroups.entrySet())
        {
            int level = lvlGroup.getKey();
            if (level == 0)
            {
                canAdd[0] += lvlGroup.getValue().size();
                continue;
            }

            List<SSTableReader> newLevel = new ArrayList<>(lm.getLevel(level));
            for (SSTableReader sstable : lvlGroup.getValue())
            {
                newLevel.add(sstable);
                newLevel.sort(SSTableReader.firstKeyComparator);

                SSTableReader prev = null;
                boolean kept = true;
                for (SSTableReader sst : newLevel)
                {
                    if (prev != null && prev.getLast().compareTo(sst.getFirst()) >= 0)
                    {
                        newLevel.remove(sstable);
                        kept = false;
                        break;
                    }
                    prev = sst;
                }
                if (kept)
                    canAdd[level] += 1;
                else
                    canAdd[0] += 1;
            }
        }
        return canAdd;
    }

    private static void assertLevelsEqual(Collection<SSTableReader> l1, Collection<SSTableReader> l2)
    {
        assertEquals(l1.size(), l2.size());
        assertEquals(new HashSet<>(l1), new HashSet<>(l2));
    }

    @Test
    public void testHighestLevelHasMoreDataThanSupported()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        int fanoutSize = 2; // to generate less sstables
        LeveledManifest lm = new LeveledManifest(cfs, 1, fanoutSize, new SizeTieredCompactionStrategyOptions());

        // generate data for L7 to trigger compaction
        int l7 = 7;
        int maxBytesForL7 = (int) (Math.pow(fanoutSize, l7) * 1024 * 1024);
        int sstablesSizeForL7 = (int) (maxBytesForL7 * 1.001) + 1;
        List<SSTableReader> sstablesOnL7 = Collections.singletonList(MockSchema.sstableWithLevel( 1, sstablesSizeForL7, l7, cfs));
        lm.addSSTables(sstablesOnL7);

        // generate data for L8 to trigger compaction
        int l8 = 8;
        int maxBytesForL8 = (int) (Math.pow(fanoutSize, l8) * 1024 * 1024);
        int sstablesSizeForL8 = (int) (maxBytesForL8 * 1.001) + 1;
        List<SSTableReader> sstablesOnL8 = Collections.singletonList(MockSchema.sstableWithLevel( 2, sstablesSizeForL8, l8, cfs));
        lm.addSSTables(sstablesOnL8);

        // compaction for L8 sstables is not supposed to be run because there is no upper level to promote sstables
        // that's why we expect compaction candidates for L7 only
        Collection<SSTableReader> compactionCandidates = lm.getCompactionCandidates().sstables;
        assertThat(compactionCandidates).containsAll(sstablesOnL7);
        assertThat(compactionCandidates).doesNotContainAnyElementsOf(sstablesOnL8);
    }

    @Test
    public void testReduceScopeL0L1() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "LeveledCompactionStrategy");
        localOptions.put("sstable_size_in_mb", "1");
        cfs.setCompactionParameters(localOptions);
        List<SSTableReader> l1sstables = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            SSTableReader l1sstable = MockSchema.sstable(i, 1 * 1024 * 1024, cfs);
            l1sstable.descriptor.getMetadataSerializer().mutateLevel(l1sstable.descriptor, 1);
            l1sstable.reloadSSTableMetadata();
            l1sstables.add(l1sstable);
        }
        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 10; i < 20; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs));
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, Iterables.concat(l0sstables, l1sstables)))
        {
            Set<SSTableReader> nonExpired = Sets.difference(txn.originals(), Collections.emptySet());
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 1, 0, 1024*1024, false);
            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(nonExpired, 0);
                SSTableReader removedSSTable = Iterables.getOnlyElement(Sets.difference(before, txn.originals()), null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l1after = sstables.right;

                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    Set<SSTableReader> l1after = sstables.right;
                    assertEquals(l1after, new HashSet<>(l1sstables)); // we don't touch L1
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertFalse(removed);
        }
    }

    @Test
    public void testReduceScopeL0()
    {

        List<SSTableReader> l0sstables = new ArrayList<>();
        for (int i = 10; i < 20; i++)
            l0sstables.add(MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs));

        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, l0sstables))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 0, 0, 1024*1024, false);

            SSTableReader lastRemoved = null;
            boolean removed = true;
            for (int i = 0; i < l0sstables.size(); i++)
            {
                Set<SSTableReader> before = new HashSet<>(txn.originals());
                removed = task.reduceScopeForLimitedSpace(before, 0);
                SSTableReader removedSSTable = Sets.difference(before, txn.originals()).stream().findFirst().orElse(null);
                if (removed)
                {
                    assertNotNull(removedSSTable);
                    assertTrue(lastRemoved == null || removedSSTable.onDiskLength() < lastRemoved.onDiskLength());
                    assertEquals(0, removedSSTable.getSSTableLevel());
                    assertEquals(before.size() - 1, txn.originals().size());
                    lastRemoved = removedSSTable;
                }
                else
                {
                    assertNull(removedSSTable);
                    Pair<Set<SSTableReader>, Set<SSTableReader>> sstables = groupByLevel(txn.originals());
                    Set<SSTableReader> l0after = sstables.left;
                    assertEquals(1, l0after.size()); // and we stop reducing once there is a single sstable left
                }
            }
            assertFalse(removed);
        }
    }

    @Test
    public void testNoHighLevelReduction() throws IOException
    {
        List<SSTableReader> sstables = new ArrayList<>();
        int i = 1;
        for (; i < 5; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs);
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
            sstable.reloadSSTableMetadata();
            sstables.add(sstable);
        }
        for (; i < 10; i++)
        {
            SSTableReader sstable = MockSchema.sstable(i, (i + 1) * 1024 * 1024, cfs);
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 2);
            sstable.reloadSSTableMetadata();
            sstables.add(sstable);
        }
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.COMPACTION, sstables))
        {
            CompactionTask task = new LeveledCompactionTask(cfs, txn, 0, 0, 1024 * 1024, false);
            assertFalse(task.reduceScopeForLimitedSpace(Sets.newHashSet(sstables), 0));
            assertEquals(Sets.newHashSet(sstables), txn.originals());
        }
    }

    private Pair<Set<SSTableReader>, Set<SSTableReader>> groupByLevel(Iterable<SSTableReader> sstables)
    {
        Set<SSTableReader> l1after = new HashSet<>();
        Set<SSTableReader> l0after = new HashSet<>();
        for (SSTableReader sstable : sstables)
        {
            switch (sstable.getSSTableLevel())
            {
                case 0:
                    l0after.add(sstable);
                    break;
                case 1:
                    l1after.add(sstable);
                    break;
                default:
                    throw new RuntimeException("only l0 & l1 sstables");
            }
        }
        return Pair.create(l0after, l1after);
    }
}
