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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableCompactingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LongLeveledCompactionStrategyTest extends SchemaLoader
{
    @Test
    public void testParallelLeveledCompaction() throws Exception
    {
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();

        WrappingCompactionStrategy strategy = ((WrappingCompactionStrategy) store.getCompactionStrategy());
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getWrappedStrategies().get(1);

        ByteBuffer value = ByteBuffer.wrap(new byte[100 * 1024]); // 100 KB value, make it easy to have multiple files

        // Enough data to have a level 1 and 2
        int rows = 128;
        int columns = 10;

        // Adds enough data to trigger multiple sstable per level
        for (int r = 0; r < rows; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < columns; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
            store.forceBlockingFlush();
        }


        // Execute LCS in parallel
        ExecutorService executor = new ThreadPoolExecutor(4, 4,
                                                          Long.MAX_VALUE, TimeUnit.SECONDS,
                                                          new LinkedBlockingDeque<Runnable>());

        List<Runnable> tasks = new ArrayList<Runnable>();
        while (true)
        {
            while (true)
            {
                final AbstractCompactionTask nextTask = lcs.getNextBackgroundTask(Integer.MIN_VALUE);
                if (nextTask == null)
                    break;

                tasks.add(new Runnable()
                {
                    public void run()
                    {
                        nextTask.execute(null);
                    }
                });
            }

            if (tasks.isEmpty())
                break;

            List<Future<?>> futures = new ArrayList<Future<?>>(tasks.size());
            for (Runnable r : tasks)
                futures.add(executor.submit(r));
            FBUtilities.waitOnFutures(futures);

            tasks.clear();
        }


        // Assert all SSTables are lined up correctly.
        LeveledManifest manifest = lcs.manifest;
        int levels = manifest.getLevelCount();
        for (int level = 0; level < levels; level++)
        {
            List<SSTableReader> sstables = manifest.getLevel(level);

            // score check
            assert (double) SSTableReader.getTotalBytes(sstables) / manifest.maxBytesForLevel(level) < 1.00;

            for (SSTableReader sstable : sstables)
            {
                // level check
                assert level == sstable.getSSTableLevel();

                if (level > 0)
                {// overlap check for levels greater than 0
                    Set<SSTableReader> overlaps = LeveledManifest.overlapping(sstable, sstables);
                    assert overlaps.size() == 1 && overlaps.contains(sstable);
                }
            }
        }
    }

    class CheckThatSSTableIsReleasedOnlyAfterCompactionFinishes implements INotificationConsumer
    {
        public final Set<SSTableReader> finishedCompaction = new HashSet<>();

        boolean failed = false;

        public void handleNotification(INotification received, Object sender)
        {
            if (received instanceof SSTableCompactingNotification)
            {
                SSTableCompactingNotification notification = (SSTableCompactingNotification) received;
                if (!notification.compacting)
                {
                    for (SSTableReader reader : notification.sstables)
                    {
                        finishedCompaction.add(reader);
                    }
                }
            }
            if (received instanceof SSTableListChangedNotification)
            {
                SSTableListChangedNotification notification = (SSTableListChangedNotification) received;
                for (SSTableReader reader : notification.removed)
                {
                    if (finishedCompaction.contains(reader))
                        failed = true;
                }
            }
        }

        boolean isFailed()
        {
            return failed;
        }
    }

    @Test
    public void testAntiCompactionAfterLCS() throws Exception
    {
        testParallelLeveledCompaction();

        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        WrappingCompactionStrategy strategy = ((WrappingCompactionStrategy) store.getCompactionStrategy());

        Collection<SSTableReader> initialSSTables = store.getUnrepairedSSTables();
        assertEquals(store.getSSTables().size(), initialSSTables.size());

        CheckThatSSTableIsReleasedOnlyAfterCompactionFinishes checker = new CheckThatSSTableIsReleasedOnlyAfterCompactionFinishes();
        store.getDataTracker().subscribe(checker);

        //anti-compact a subset of sstables
        Range<Token> range = new Range<Token>(new BytesToken("110".getBytes()), new BytesToken("111".getBytes()), store.partitioner);
        List<Range<Token>> ranges = Arrays.asList(range);
        Refs<SSTableReader> refs = Refs.tryRef(initialSSTables);
        if (refs == null)
            throw new IllegalStateException();
        long repairedAt = 1000;
        CompactionManager.instance.performAnticompaction(store, ranges, refs, repairedAt);

        //check that sstables were released only after compaction finished
        assertFalse("Anti-compaction released sstable from compacting set before compaction was finished",
                    checker.isFailed());

        //check there is only one global ref count
        for (SSTableReader sstable : store.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
        }

        //check that compacting status was clearedd in all sstables
        assertEquals(0, store.getDataTracker().getCompacting().size());

        //make sure readers were replaced correctly on unrepaired leveled manifest after anti-compaction
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getWrappedStrategies().get(1);
        for (SSTableReader reader : initialSSTables)
        {
            Range<Token> sstableRange = new Range<Token>(reader.first.getToken(), reader.last.getToken());
            if (sstableRange.intersects(range))
            {
                assertFalse(lcs.manifest.generations[reader.getSSTableLevel()].contains(reader));
            }
        }
    }

    @Test
    public void testLeveledScanner() throws Exception
    {
        testParallelLeveledCompaction();
        String ksname = "Keyspace1";
        String cfname = "StandardLeveled";
        Keyspace keyspace = Keyspace.open(ksname);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(cfname);
        store.disableAutoCompaction();

        WrappingCompactionStrategy strategy = ((WrappingCompactionStrategy) store.getCompactionStrategy());
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) strategy.getWrappedStrategies().get(1);

        ByteBuffer value = ByteBuffer.wrap(new byte[10 * 1024]); // 10 KB value

        // Adds 10 partitions
        for (int r = 0; r < 10; r++)
        {
            DecoratedKey key = Util.dk(String.valueOf(r));
            Mutation rm = new Mutation(ksname, key.getKey());
            for (int c = 0; c < 10; c++)
            {
                rm.add(cfname, Util.cellname("column" + c), value, 0);
            }
            rm.apply();
        }

        //Flush sstable
        store.forceBlockingFlush();

        Collection<SSTableReader> allSSTables = store.getSSTables();
        for (SSTableReader sstable : allSSTables)
        {
            if (sstable.getSSTableLevel() == 0)
            {
                System.out.println("Mutating L0-SSTABLE level to L1 to simulate a bug: " + sstable.getFilename());
                sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 1);
                sstable.reloadSSTableMetadata();
            }
        }

        try (AbstractCompactionStrategy.ScannerList scannerList = lcs.getScanners(allSSTables))
        {
            //Verify that leveled scanners will always iterate in ascending order (CASSANDRA-9935)
            for (ISSTableScanner scanner : scannerList.scanners)
            {
                DecoratedKey lastKey = null;
                while (scanner.hasNext())
                {
                    OnDiskAtomIterator row = scanner.next();
                    if (lastKey != null)
                    {
                        assertTrue("row " + row.getKey() + " received out of order wrt " + lastKey, row.getKey().compareTo(lastKey) >= 0);
                    }
                    lastKey = row.getKey();
                }
            }
        }
    }
}
