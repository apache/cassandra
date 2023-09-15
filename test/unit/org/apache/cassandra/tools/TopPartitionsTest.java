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

package org.apache.cassandra.tools;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.openmbean.CompositeData;

import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.Util;


import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Includes test cases for both the 'toppartitions' command and its successor 'profileload'
 */
public class TopPartitionsTest
{
    public static String KEYSPACE = TopPartitionsTest.class.getSimpleName().toLowerCase();
    public static String TABLE = "test";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
        executeInternal(format("CREATE TABLE %s.%s (k text, c text, v text, PRIMARY KEY (k, c))", KEYSPACE, TABLE));
    }

    @Test
    public void testServiceTopPartitionsNoArg() throws Exception
    {
        BlockingQueue<Map<String, List<CompositeData>>> q = new ArrayBlockingQueue<>(1);
        ColumnFamilyStore.all();
        Executors.newCachedThreadPool().execute(() ->
        {
            try
            {
                q.put(StorageService.instance.samplePartitions(null, 1000, 100, 10, Lists.newArrayList("READS", "WRITES")));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        Thread.sleep(100);
        SystemKeyspace.persistLocalMetadata();
        Map<String, List<CompositeData>> result = q.poll(5, TimeUnit.SECONDS);
        List<CompositeData> cd = result.get("WRITES");
        assertEquals(1, cd.size());
    }

    @Test
    public void testServiceTopPartitionsSingleTable() throws Exception
    {
        ColumnFamilyStore.getIfExists("system", "local").beginLocalSampling("READS", 5, 240000);
        String req = "SELECT * FROM system.%s WHERE key='%s'";
        executeInternal(format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
        List<CompositeData> result = ColumnFamilyStore.getIfExists("system", "local").finishLocalSampling("READS", 5);
        assertEquals("If this failed you probably have to raise the beginLocalSampling duration", 1, result.size());
    }

    @Test
    public void testTopPartitionsRowTombstoneAndSSTableCount() throws Exception
    {
        int count = 10;
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
        cfs.disableAutoCompaction();

        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('a', 'a', 'a')", KEYSPACE, TABLE));
        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('a', 'b', 'a')", KEYSPACE, TABLE));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('a', 'c', 'a')", KEYSPACE, TABLE));
        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('b', 'b', 'b')", KEYSPACE, TABLE));
        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('c', 'c', 'c')", KEYSPACE, TABLE));
        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('c', 'd', 'a')", KEYSPACE, TABLE));
        executeInternal(format("INSERT INTO %s.%s(k,c,v) VALUES ('c', 'e', 'a')", KEYSPACE, TABLE));
        executeInternal(format("DELETE FROM %s.%s WHERE k='a' AND c='a'", KEYSPACE, TABLE));
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        // test multi-partition read
        cfs.beginLocalSampling("READ_ROW_COUNT", count, 240000);
        cfs.beginLocalSampling("READ_TOMBSTONE_COUNT", count, 240000);
        cfs.beginLocalSampling("READ_SSTABLE_COUNT", count, 240000);

        executeInternal(format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        Thread.sleep(2000); // simulate waiting before finishing sampling

        List<CompositeData> rowCounts = cfs.finishLocalSampling("READ_ROW_COUNT", count);
        List<CompositeData> tsCounts = cfs.finishLocalSampling("READ_TOMBSTONE_COUNT", count);
        List<CompositeData> sstCounts = cfs.finishLocalSampling("READ_SSTABLE_COUNT", count);

        assertEquals(0, sstCounts.size()); // not tracked on range reads
        assertEquals(3, rowCounts.size()); // 3 partitions read (a, b, c)
        assertEquals(1, tsCounts.size()); // 1 partition w tombstones (a)

        for (CompositeData data : rowCounts)
        {
            String partitionKey = (String) data.get("value");
            long numRows = (long) data.get("count");
            if (partitionKey.equalsIgnoreCase("a"))
            {
                assertEquals(2, numRows);
            }
            else if (partitionKey.equalsIgnoreCase("b"))
                assertEquals(1, numRows);
            else if (partitionKey.equalsIgnoreCase("c"))
                assertEquals(3, numRows);
        }

        assertEquals("a", tsCounts.get(0).get("value"));
        assertEquals(1, (long) tsCounts.get(0).get("count"));

        // test single partition read
        cfs.beginLocalSampling("READ_ROW_COUNT", count, 240000);
        cfs.beginLocalSampling("READ_TOMBSTONE_COUNT", count, 240000);
        cfs.beginLocalSampling("READ_SSTABLE_COUNT", count, 240000);

        executeInternal(format("SELECT * FROM %s.%s WHERE k='a'", KEYSPACE, TABLE));
        executeInternal(format("SELECT * FROM %s.%s WHERE k='b'", KEYSPACE, TABLE));
        executeInternal(format("SELECT * FROM %s.%s WHERE k='c'", KEYSPACE, TABLE));
        Thread.sleep(2000); // simulate waiting before finishing sampling

        rowCounts = cfs.finishLocalSampling("READ_ROW_COUNT", count);
        tsCounts = cfs.finishLocalSampling("READ_TOMBSTONE_COUNT", count);
        sstCounts = cfs.finishLocalSampling("READ_SSTABLE_COUNT", count);

        assertEquals(3, sstCounts.size()); // 3 partitions read
        assertEquals(3, rowCounts.size()); // 3 partitions read
        assertEquals(1, tsCounts.size());  // 3 partitions read only one containing tombstones

        for (CompositeData data : sstCounts)
        {
            String partitionKey = (String) data.get("value");
            long numRows = (long) data.get("count");
            if (partitionKey.equalsIgnoreCase("a"))
            {
                assertEquals(2, numRows);
            }
            else if (partitionKey.equalsIgnoreCase("b"))
                assertEquals(1, numRows);
            else if (partitionKey.equalsIgnoreCase("c"))
                assertEquals(1, numRows);
        }

        for (CompositeData data : rowCounts)
        {
            String partitionKey = (String) data.get("value");
            long numRows = (long) data.get("count");
            if (partitionKey.equalsIgnoreCase("a"))
            {
                assertEquals(2, numRows);
            }
            else if (partitionKey.equalsIgnoreCase("b"))
                assertEquals(1, numRows);
            else if (partitionKey.equalsIgnoreCase("c"))
                assertEquals(3, numRows);
        }

        assertEquals("a", tsCounts.get(0).get("value"));
        assertEquals(1, (long) tsCounts.get(0).get("count"));
    }

    @Test
    public void testStartAndStopScheduledSampling()
    {
        List<String> allSamplers = Arrays.stream(Sampler.SamplerType.values()).map(Enum::toString).collect(Collectors.toList());
        StorageService ss = StorageService.instance;

        assertTrue("Scheduling new sampled tasks should be allowed",
                   ss.startSamplingPartitions(null, null, 10, 10, 100, 10, allSamplers));

        assertEquals(Collections.singletonList("*.*"), ss.getSampleTasks());

        assertFalse("Sampling with duplicate keys should be disallowed",
                    ss.startSamplingPartitions(null, null, 20, 20, 100, 10, allSamplers));

        assertTrue("Existing scheduled sampling tasks should be cancellable", ss.stopSamplingPartitions(null, null));

        Util.spinAssertEquals(Collections.emptyList(), ss::getSampleTasks, 30);

        assertTrue("When nothing is scheduled, you should be able to stop all scheduled sampling tasks",
                   ss.stopSamplingPartitions(null, null));
    }
}
