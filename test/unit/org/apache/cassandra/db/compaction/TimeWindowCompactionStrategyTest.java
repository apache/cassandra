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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MockSchema;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.getBucketAggregates;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.getWindowBoundsInMillis;
import static org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy.validateOptions;
import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TimeWindowCompactionStrategyTest extends SchemaLoader
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "Standard1";
    private static final int TTL_SECONDS = 10;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");
        System.setProperty(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY, "true");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "0");
            validateOptions(options);
            fail(String.format("%s == 0 should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
            // expected exception
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "-1337");
            validateOptions(options);
            fail(String.format("Negative %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "1");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MONTHS");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "MINUTES");
        }

        try
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "not-a-boolean");
            validateOptions(options);
            fail(String.format("Invalid %s should be rejected", TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY));
        }
        catch (ConfigurationException e)
        {
            options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "true");
        }
        
        options.put(AbstractCompactionStrategy.UNCHECKED_TOMBSTONE_COMPACTION_OPTION, "true");
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        assertFalse(twcs.disableTombstoneCompactions);
        options.put(AbstractCompactionStrategy.UNCHECKED_TOMBSTONE_COMPACTION_OPTION, "false");
        twcs = new TimeWindowCompactionStrategy(cfs, options);
        assertTrue(twcs.disableTombstoneCompactions);

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }


    @Test
    public void testTimeWindows()
    {
        long tstamp1 = 1451001601000L; // 2015-12-25 @ 00:00:01, in milliseconds
        long tstamp2 = 1451088001000L; // 2015-12-26 @ 00:00:01, in milliseconds
        long lowHour = 1451001600000L; // 2015-12-25 @ 00:00:00, in milliseconds

        // A 1 hour window should round down to the beginning of the hour
        assertEquals(lowHour, getWindowBoundsInMillis(HOURS, 1, tstamp1));

        // A 1 minute window should round down to the beginning of the hour
        assertEquals(lowHour, getWindowBoundsInMillis(TimeUnit.MINUTES, 1, tstamp1));

        // A 1 day window should round down to the beginning of the hour
        assertEquals(lowHour, getWindowBoundsInMillis(TimeUnit.DAYS, 1, tstamp1));

        // The 2 day window of 2015-12-25 + 2015-12-26 should round down to the beginning of 2015-12-25
        assertEquals(lowHour, getWindowBoundsInMillis(TimeUnit.DAYS, 2, tstamp2));
    }

    @Test
    public void testPrepBucket()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);
        long tstamp = System.currentTimeMillis();
        long tstamp2 = tstamp - (2L * 3600L * 1000L);

        // create 5 sstables
        for (int r = 0; r < 3; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            new RowUpdateBuilder(cfs.metadata(), r, key.getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();

            cfs.forceBlockingFlush(UNIT_TESTS);
        }
        // Decrement the timestamp to simulate a timestamp in the past hour
        for (int r = 3; r < 5; r++)
        {
            // And add progressively more cells into each sstable
            DecoratedKey key = Util.dk(String.valueOf(r));
            new RowUpdateBuilder(cfs.metadata(), r, key.getKey())
                .clustering("column")
                .add("val", value).build().applyUnsafe();
            cfs.forceBlockingFlush(UNIT_TESTS);
        }

        cfs.forceBlockingFlush(UNIT_TESTS);

        TreeMap<Long, List<SSTableReader>> buckets = new TreeMap<>(Long::compare);
        List<SSTableReader> sstrs = new ArrayList<>(cfs.getLiveSSTables());

        // We'll put 3 sstables into the newest bucket
        for (int i = 0; i < 3; i++)
        {
            TimeWindowCompactionStrategy.addToBuckets(buckets, sstrs.get(i), tstamp, TimeUnit.HOURS, 1);
        }

        List<CompactionAggregate> aggregates = getBucketAggregates(buckets, 4, 32, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(HOURS, 1, System.currentTimeMillis()));
        Set<CompactionPick> compactions = toCompactions(aggregates);
        assertTrue("No selected compactions when fewer than min threshold SSTables in the newest bucket", CompactionAggregate.getSelected(aggregates).isEmpty());
        assertTrue("No compactions when fewer than min threshold SSTables in the newest bucket", compactions.isEmpty());

        aggregates = getBucketAggregates(buckets, 2, 32, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(HOURS, 1, System.currentTimeMillis()));
        compactions = toCompactions(aggregates);
        assertFalse("There should be one selected compaction when bucket is larger than the min but smaller than max threshold", CompactionAggregate.getSelected(aggregates).isEmpty());
        assertEquals("There should be one compaction when bucket is larger than the min but smaller than max threshold", 1,  compactions.size());

        // And 2 into the second bucket (1 hour back)
        for (int i = 3; i < 5; i++)
        {
            TimeWindowCompactionStrategy.addToBuckets(buckets, sstrs.get(i), tstamp2, TimeUnit.HOURS, 1);
        }

        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(0).getMinTimestamp(), sstrs.get(0).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(1).getMinTimestamp(), sstrs.get(1).getMaxTimestamp());
        assertEquals("an sstable with a single value should have equal min/max timestamps", sstrs.get(2).getMinTimestamp(), sstrs.get(2).getMaxTimestamp());

        // Test trim
        int numSSTables = 40;
        for (int r = 5; r < numSSTables; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            for (int i = 0; i < r; i++)
            {
                new RowUpdateBuilder(cfs.metadata(), tstamp + r, key.getKey())
                    .clustering("column")
                    .add("val", value).build().applyUnsafe();
            }
            cfs.forceBlockingFlush(UNIT_TESTS);
        }

        // Reset the buckets, overfill it now
        sstrs = new ArrayList<>(cfs.getLiveSSTables());
        for (int i = 0; i < 40; i++)
        {
            TimeWindowCompactionStrategy.addToBuckets(buckets, sstrs.get(i), sstrs.get(i).getMaxTimestamp(), TimeUnit.HOURS, 1);
        }

        aggregates = getBucketAggregates(buckets, 4, 32, new SizeTieredCompactionStrategyOptions(), getWindowBoundsInMillis(HOURS, 1, System.currentTimeMillis()));
        compactions = toCompactions(aggregates);
        assertEquals("new bucket should be split by max threshold of 32", buckets.keySet().size() + 1, compactions.size());

        CompactionPick selected = CompactionAggregate.getSelected(aggregates);
        assertEquals("first pick should be trimmed to max threshold of 32", 32, selected.sstables.size());
    }


    @Test
    public void testDropExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // Create a expiring sstable with a TTL
        DecoratedKey key = Util.dk("expired");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), TTL_SECONDS, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush(UNIT_TESTS);
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();
        Thread.sleep(10);

        // Create a second sstable without TTL
        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata(), System.currentTimeMillis(), key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush(UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 2);

        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "SECONDS");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();
        assertNull(twcs.getNextBackgroundTask(nowInSeconds()));

        // Wait for the expiration of the first sstable
        Thread.sleep(TimeUnit.SECONDS.toMillis(TTL_SECONDS + 1));
        AbstractCompactionTask t = twcs.getNextBackgroundTask(nowInSeconds());
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.transaction.originals()));
        SSTableReader sstable = t.transaction.originals().iterator().next();
        assertEquals(sstable, expiredSSTable);
        t.transaction.abort();
    }

    @Test
    public void testDropOverlappingExpiredSSTables() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF_STANDARD1);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        long timestamp = System.currentTimeMillis();
        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // Create a expiring sstable with a TTL
        DecoratedKey key = Util.dk("expired");
        new RowUpdateBuilder(cfs.metadata(), timestamp, TTL_SECONDS, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush(UNIT_TESTS);
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();
        Thread.sleep(10);

        // Create a second sstable without TTL and with a row superceded by the expiring row
        new RowUpdateBuilder(cfs.metadata(), timestamp - 1000, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();
        key = Util.dk("nonexpired");
        new RowUpdateBuilder(cfs.metadata(), timestamp, key.getKey())
            .clustering("column")
            .add("val", value).build().applyUnsafe();

        cfs.forceBlockingFlush(UNIT_TESTS);
        assertEquals(cfs.getLiveSSTables().size(), 2);

        Map<String, String> options = new HashMap<>();
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, "30");
        options.put(TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_UNIT_KEY, "SECONDS");
        options.put(TimeWindowCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY, "MILLISECONDS");
        options.put(TimeWindowCompactionStrategyOptions.EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_KEY, "0");
        TimeWindowCompactionStrategy twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();
        assertNull(twcs.getNextBackgroundTask(nowInSeconds()));

        // Wait for the expiration of the first sstable
        Thread.sleep(TimeUnit.SECONDS.toMillis(TTL_SECONDS + 1));
        assertNull(twcs.getNextBackgroundTask(nowInSeconds()));

        options.put(TimeWindowCompactionStrategyOptions.UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_KEY, "true");
        twcs = new TimeWindowCompactionStrategy(cfs, options);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            twcs.addSSTable(sstable);

        twcs.startup();
        AbstractCompactionTask t = twcs.getNextBackgroundTask(nowInSeconds());
        assertNotNull(t);
        assertEquals(1, Iterables.size(t.transaction.originals()));
        SSTableReader sstable = t.transaction.originals().iterator().next();
        assertEquals(sstable, expiredSSTable);
        twcs.shutdown();
        t.transaction.abort();
    }

    @Test
    public void testGroupForAntiCompaction()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS("test_group_for_anticompaction");
        cfs.setCompactionParameters(ImmutableMap.of("class", "TimeWindowCompactionStrategy",
                                                    "timestamp_resolution", "MILLISECONDS",
                                                    "compaction_window_size", "1",
                                                    "compaction_window_unit", "MINUTES"));

        List<SSTableReader> sstables = new ArrayList<>(10);
        long curr = System.currentTimeMillis();
        for (int i = 0; i < 10; i++)
            sstables.add(MockSchema.sstableWithTimestamp(i, curr + TimeUnit.MILLISECONDS.convert(i, TimeUnit.MINUTES), cfs));

        cfs.addSSTables(sstables);
        Collection<Collection<SSTableReader>> groups = cfs.getCompactionStrategyManager().getCompactionStrategyFor(sstables.get(0)).groupSSTablesForAntiCompaction(sstables);
        assertTrue(groups.size() > 0);
        for (Collection<SSTableReader> group : groups)
            assertEquals(1, group.size());
    }

    private static Set<CompactionPick> toCompactions(List<CompactionAggregate> aggregates)
    {
        return aggregates.stream().flatMap(aggr -> aggr.getActive().stream()).collect(Collectors.toSet());
    }
}
