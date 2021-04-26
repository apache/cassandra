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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class ZombieSSTablesTest
{
    private static final String KEYSPACE1 = "BlacklistingCompactionsTest";
    private static final String STANDARD_STCS = "Standard_STCS";
    private static final String STANDARD_LCS = "Standard_LCS";
    private static final String STANDARD_TWCS = "Standard_TWCS";
    private static final String MAXIMAL = "_Maximal";
    private static int maxValueSize;

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
                KeyspaceParams.simple(1),
                makeTable(STANDARD_STCS).compaction(CompactionParams.DEFAULT),
                makeTable(STANDARD_LCS).compaction(CompactionParams.lcs(Collections.emptyMap())),
                makeTable(STANDARD_TWCS).compaction(CompactionParams.twcs(Collections.emptyMap())),
                makeTable(STANDARD_STCS + MAXIMAL).compaction(CompactionParams.DEFAULT),
                makeTable(STANDARD_LCS + MAXIMAL).compaction(CompactionParams.lcs(Collections.emptyMap())),
                makeTable(STANDARD_TWCS + MAXIMAL).compaction(CompactionParams.twcs(Collections.emptyMap())));

        maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024);
    }

    /**
     * Return a table metadata, we use types with fixed size to increase the chance of detecting corrupt data
     */
    private static TableMetadata.Builder makeTable(String tableName)
    {
        return SchemaLoader.standardCFMD(KEYSPACE1, tableName, 1, LongType.instance, LongType.instance, LongType.instance);
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);
    }

    @Test
    public void testWithSizeTieredCompactionStrategy() throws Exception
    {
        testZombieSSTables(STANDARD_STCS);
    }

    @Test
    public void testWithLeveledCompactionStrategy() throws Exception
    {
        testZombieSSTables(STANDARD_LCS);
    }

    @Test
    public void testWithTimeWindowCompactionStrategy() throws Exception
    {
        testZombieSSTables(STANDARD_TWCS);
    }

    @Test
    public void testWithSizeTieredCompactionStrategyMaximal() throws Exception
    {
        testZombieSSTablesMaximal(STANDARD_STCS);
    }

    @Test
    public void testWithLeveledCompactionStrategyMaximal() throws Exception
    {
        testZombieSSTablesMaximal(STANDARD_LCS);
    }

    @Test
    public void testWithTimeWindowCompactionStrategyMaximal() throws Exception
    {
        testZombieSSTablesMaximal(STANDARD_TWCS);
    }

    private void prepareZombieSSTables(ColumnFamilyStore cfs) throws Exception
    {
        final int ROWS_PER_SSTABLE = 10;
        final int SSTABLES = 15;
        final int SSTABLES_TO_DELETE = 2;

        cfs.truncateBlocking();

        // disable compaction while flushing
        cfs.disableAutoCompaction();
        //test index corruption
        //now create a few new SSTables
        long maxTimestampExpected = Long.MIN_VALUE;
        Set<DecoratedKey> inserted = new HashSet<>();

        for (int j = 0; j < SSTABLES; j++)
        {
            for (int i = 0; i < ROWS_PER_SSTABLE; i++)
            {
                DecoratedKey key = Util.dk(String.valueOf(i));
                long timestamp = j * ROWS_PER_SSTABLE + i;
                new RowUpdateBuilder(cfs.metadata(), timestamp, key.getKey())
                        .clustering(Long.valueOf(i))
                        .add("val", Long.valueOf(i))
                        .build()
                        .applyUnsafe();
                maxTimestampExpected = Math.max(timestamp, maxTimestampExpected);
                inserted.add(key);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            CompactionsTest.assertMaxTimestamp(cfs, maxTimestampExpected);
            assertEquals(inserted.toString(), inserted.size(), Util.getAll(Util.cmd(cfs).build()).size());
        }

        Collection<SSTableReader> sstables = cfs.getLiveSSTables();

        // delete first 'sstablesToDelete' SSTables, but make it so that the compaction strategy still thinks they
        // are present by not sending the removal notification (this can normally happen due to a race between the add
        // and remove notification for an sstable).
        Set<SSTableReader> toDrop = sstables.stream().limit(SSTABLES_TO_DELETE).collect(Collectors.toSet());
        cfs.getTracker().removeUnsafe(toDrop);
        toDrop.stream().forEach(sstable -> sstable.selfRef().release());    // avoid leak
        assertTrue(Sets.intersection(cfs.getLiveSSTables(), toDrop).isEmpty());
    }

    private void testZombieSSTablesMaximal(String tableName) throws Exception
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName + MAXIMAL);

        prepareZombieSSTables(cfs);
        Collection<AbstractCompactionTask> maximalTasks = cfs.getCompactionStrategyManager().getMaximalTasks(0, false);
        assertNotNull(maximalTasks);
        assertFalse(maximalTasks.isEmpty());
        maximalTasks.stream().forEach(task -> task.transaction.abort());    // avoid leak
    }

    private void testZombieSSTables(String tableName) throws Exception
    {
        // this test does enough rows to force multiple block indexes to be used
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(tableName);

        prepareZombieSSTables(cfs);

        CompactionStrategyManager compactionStrategyManager = cfs.getCompactionStrategyManager();
        compactionStrategyManager.enable();
        AbstractCompactionTask nextBackgroundTask = compactionStrategyManager.getNextBackgroundTask(0);
        assertNotNull(nextBackgroundTask);
        nextBackgroundTask.transaction.abort();    // avoid leak
    }
}
