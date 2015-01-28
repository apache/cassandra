package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.apache.cassandra.Util.cellname;

public class BlacklistingCompactionsTest
{
    private static final String KEYSPACE1 = "BlacklistingCompactionsTest";
    private static final String CF_STANDARD1 = "Standard1";

    @After
    public void leakDetect() throws InterruptedException
    {
        System.gc();
        System.gc();
        System.gc();
        Thread.sleep(10);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
        closeStdErr();
    }

    public static void closeStdErr()
    {
        // These tests generate an error message per CorruptSSTableException since it goes through
        // DebuggableThreadPoolExecutor, which will log it in afterExecute.  We could stop that by
        // creating custom CompactionStrategy and CompactionTask classes, but that's kind of a
        // ridiculous amount of effort, especially since those aren't really intended to be wrapped
        // like that.
        System.err.close();
    }

    @Test
    public void testBlacklistingWithSizeTieredCompactionStrategy() throws Exception
    {
        testBlacklisting(SizeTieredCompactionStrategy.class.getCanonicalName());
    }

    @Test
    public void testBlacklistingWithLeveledCompactionStrategy() throws Exception
    {
        testBlacklisting(LeveledCompactionStrategy.class.getCanonicalName());
    }

    public void testBlacklisting(String compactionStrategy) throws Exception
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = cfs.metadata.getMinIndexInterval() * 2 / ROWS_PER_SSTABLE;

        cfs.setCompactionStrategyClass(compactionStrategy);

        // disable compaction while flushing
        cfs.disableAutoCompaction();
        //test index corruption
        //now create a few new SSTables
        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<DecoratedKey>();
        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i % 2));
                Mutation rm = new Mutation(KEYSPACE1, key.getKey());
                long timestamp = j * ROWS_PER_SSTABLE + i;
                rm.add("Standard1", cellname(i / 2), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp);
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                rm.applyUnsafe();
                inserted.add(key);
            }
            cfs.forceBlockingFlush();
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getRangeSlice(cfs).size());
        }

        Collection<SSTableReader> sstables = cfs.getSSTables();
        int currentSSTable = 0;
        int sstablesToCorrupt = 8;

        // corrupt first 'sstablesToCorrupt' SSTables
        for (SSTableReader sstable : sstables)
        {
            if(currentSSTable + 1 > sstablesToCorrupt)
                break;

            RandomAccessFile raf = null;

            try
            {
                raf = new RandomAccessFile(sstable.getFilename(), "rw");
                assertNotNull(raf);
                raf.write(0xFFFFFF);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }

            currentSSTable++;
        }

        int failures = 0;

        // in case something will go wrong we don't want to loop forever using for (;;)
        for (int i = 0; i < sstables.size(); i++)
        {
            try
            {
                cfs.forceMajorCompaction();
            }
            catch (Exception e)
            {
                // kind of a hack since we're not specifying just CorruptSSTableExceptions, or (what we actually expect)
                // an ExecutionException wrapping a CSSTE.  This is probably Good Enough though, since if there are
                // other errors in compaction presumably the other tests would bring that to light.
                failures++;
                continue;
            }

            assertEquals(sstablesToCorrupt + 1, cfs.getSSTables().size());
            break;
        }


        cfs.truncateBlocking();
        assertEquals(failures, sstablesToCorrupt);
    }
}
