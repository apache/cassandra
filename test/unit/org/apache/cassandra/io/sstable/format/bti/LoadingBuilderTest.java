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

package org.apache.cassandra.io.sstable.format.bti;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class LoadingBuilderTest extends CQLTester
{
    public static Map<String, Boolean> preloadsMap = new ConcurrentHashMap<>();

    @Test
    @BMRule(name = "Save preload argument of PartitionIndex initialization",
            targetClass = "org.apache.cassandra.io.sstable.format.bti.PartitionIndex",
            targetMethod = "load(org.apache.cassandra.io.util.FileHandle, org.apache.cassandra.dht.IPartitioner, boolean)",
            action = "org.apache.cassandra.io.sstable.format.bti.LoadingBuilderTest.preloadsMap.put($1.path(), $3)")
    public void testPreloadFlag()
    {
        Assume.assumeTrue(BtiFormat.isSelected());
        testPreloadFlag(false);
        testPreloadFlag(true);
    }

    private void testPreloadFlag(boolean disableBloomFilter)
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH bloom_filter_fp_chance = " +
                    (disableBloomFilter ? "1" : "0.01"));
        for (int i = 0; i < 100; ++i)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Set<SSTableReader> ssTables = cfs.getLiveSSTables();
        assertTrue(!ssTables.isEmpty());

        for (SSTableReader rdr : ssTables)
        {
            Descriptor descriptor = rdr.descriptor;
            File partitionIndexFile = descriptor.fileFor(BtiFormat.Components.PARTITION_INDEX);
            // When we flush we should never preload the index file
            verifyPreloadMatches(false, partitionIndexFile);

            // But when we reopen the file from disk, we should do it if there is no bloom filter
            SSTableReader newReader = SSTableReader.open(cfs, descriptor);
            try
            {
                verifyPreloadMatches(disableBloomFilter, partitionIndexFile);
            }
            finally
            {
                newReader.selfRef().release();
            }
        }
    }

    private static void verifyPreloadMatches(boolean disableBloomFilter, File partitionIndexFile)
    {
        Boolean preload = preloadsMap.get(partitionIndexFile.path());
        assertNotNull(partitionIndexFile.toString(), preload);
        assertEquals(disableBloomFilter, preload.booleanValue());
    }
}
