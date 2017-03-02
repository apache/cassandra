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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.getBuckets;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.mostInterestingBucket;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.trimToThresholdWithHotness;
import static org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy.validateOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SizeTieredCompactionStrategyTest
{
    public static final String KEYSPACE1 = "SizeTieredCompactionStrategyTest";
    private static final String CF_STANDARD1 = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable tombstone histogram rounding for tests
        System.setProperty("cassandra.streaminghistogram.roundseconds", "1");

        SchemaLoader.prepareServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testOptionsValidation() throws ConfigurationException
    {
        Map<String, String> options = new HashMap<>();
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        options.put(SizeTieredCompactionStrategyOptions.BUCKET_HIGH_KEY, "1.5");
        options.put(SizeTieredCompactionStrategyOptions.MIN_SSTABLE_SIZE_KEY, "10000");
        Map<String, String> unvalidated = validateOptions(options);
        assertTrue(unvalidated.isEmpty());

        try
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "1000.0");
            validateOptions(options);
            fail("bucket_low greater than bucket_high should be rejected");
        }
        catch (ConfigurationException e)
        {
            options.put(SizeTieredCompactionStrategyOptions.BUCKET_LOW_KEY, "0.5");
        }

        options.put("bad_option", "1.0");
        unvalidated = validateOptions(options);
        assertTrue(unvalidated.containsKey("bad_option"));
    }

    @Test
    public void testGetBuckets()
    {
        List<Pair<String, Long>> pairs = new ArrayList<Pair<String, Long>>();
        String[] strings = { "a", "bbbb", "cccccccc", "cccccccc", "bbbb", "a" };
        for (String st : strings)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        List<List<String>> buckets = getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(3, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(2, bucket.size());
            assertEquals(bucket.get(0).length(), bucket.get(1).length());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
        }

        pairs.clear();
        buckets.clear();

        String[] strings2 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings2)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = getBuckets(pairs, 1.5, 0.5, 2);
        assertEquals(2, buckets.size());

        for (List<String> bucket : buckets)
        {
            assertEquals(3, bucket.size());
            assertEquals(bucket.get(0).charAt(0), bucket.get(1).charAt(0));
            assertEquals(bucket.get(1).charAt(0), bucket.get(2).charAt(0));
        }

        // Test the "min" functionality
        pairs.clear();
        buckets.clear();

        String[] strings3 = { "aaa", "bbbbbbbb", "aaa", "bbbbbbbb", "bbbbbbbb", "aaa" };
        for (String st : strings3)
        {
            Pair<String, Long> pair = Pair.create(st, new Long(st.length()));
            pairs.add(pair);
        }

        buckets = getBuckets(pairs, 1.5, 0.5, 10);
        assertEquals(1, buckets.size());
    }

    @Test
    public void testPrepBucket() throws Exception
    {
        String ksname = KEYSPACE1;
        String cfname = "Standard1";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfname);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        ByteBuffer value = ByteBuffer.wrap(new byte[100]);

        // create 3 sstables
        int numSSTables = 3;
        for (int r = 0; r < numSSTables; r++)
        {
            String key = String.valueOf(r);
            new RowUpdateBuilder(cfs.metadata, 0, key)
                .clustering("column").add("val", value)
                .build().applyUnsafe();
            cfs.forceBlockingFlush();
        }
        cfs.forceBlockingFlush();

        List<SSTableReader> sstrs = new ArrayList<>(cfs.getLiveSSTables());
        Pair<List<SSTableReader>, Double> bucket;

        List<SSTableReader> interestingBucket = mostInterestingBucket(Collections.singletonList(sstrs.subList(0, 2)), 4, 32);
        assertTrue("nothing should be returned when all buckets are below the min threshold", interestingBucket.isEmpty());

        sstrs.get(0).overrideReadMeter(new RestorableMeter(100.0, 100.0));
        sstrs.get(1).overrideReadMeter(new RestorableMeter(200.0, 200.0));
        sstrs.get(2).overrideReadMeter(new RestorableMeter(300.0, 300.0));

        long estimatedKeys = sstrs.get(0).estimatedKeys();

        // if we have more than the max threshold, the coldest should be dropped
        bucket = trimToThresholdWithHotness(sstrs, 2);
        assertEquals("one bucket should have been dropped", 2, bucket.left.size());
        double expectedBucketHotness = (200.0 + 300.0) / estimatedKeys;
        assertEquals(String.format("bucket hotness (%f) should be close to %f", bucket.right, expectedBucketHotness),
                     expectedBucketHotness, bucket.right, 1.0);
    }
}
