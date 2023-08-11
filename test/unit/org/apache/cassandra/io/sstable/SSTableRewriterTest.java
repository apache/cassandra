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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.Collections.singletonList;
import static org.apache.cassandra.db.compaction.OperationType.COMPACTION;
import static org.apache.cassandra.utils.FBUtilities.nowInSeconds;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SSTableRewriterTest extends SSTableWriterTestBase
{
    @Test
    public void basicTest()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        for (int j = 0; j < 100; j ++)
        {
            new RowUpdateBuilder(cfs.metadata(), j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .apply();
        }
        Util.flush(cfs);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());
        assertEquals(sstables.iterator().next().bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.getCount());
        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, false, 1000);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while(ci.hasNext())
            {
                writer.append(ci.next());
            }
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        assertEquals(1, assertFileCounts(sstables.iterator().next().descriptor.directory.tryListNames()));

        validateCFS(cfs);
        truncate(cfs);
    }
    @Test
    public void basicTest2()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());

        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                writer.append(ci.next());
            }
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        assertEquals(1, assertFileCounts(sstables.iterator().next().descriptor.directory.tryListNames()));

        validateCFS(cfs);
    }

    @Test
    public void getPositionsTest()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());

        long nowInSec = FBUtilities.nowInSeconds();
        boolean checked = false;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                UnfilteredRowIterator row = ci.next();
                writer.append(row);
                if (!checked && writer.currentWriter().getFilePointer() > 1500000)
                {
                    checked = true;
                    for (SSTableReader sstable : cfs.getLiveSSTables())
                    {
                        if (sstable.openReason == SSTableReader.OpenReason.EARLY)
                        {
                            SSTableReader c = txn.current(sstables.iterator().next());
                            Collection<Range<Token>> r = Arrays.asList(new Range<>(cfs.getPartitioner().getMinimumToken(), cfs.getPartitioner().getMinimumToken()));
                            List<SSTableReader.PartitionPositionBounds> tmplinkPositions = sstable.getPositionsForRanges(r);
                            List<SSTableReader.PartitionPositionBounds> compactingPositions = c.getPositionsForRanges(r);
                            assertEquals(1, tmplinkPositions.size());
                            assertEquals(1, compactingPositions.size());
                            assertEquals(0, tmplinkPositions.get(0).lowerPosition);
                            // make sure we have no overlap between the early opened file and the compacting one:
                            assertEquals(tmplinkPositions.get(0).upperPosition, compactingPositions.get(0).lowerPosition);
                            assertEquals(c.uncompressedLength(), compactingPositions.get(0).upperPosition);
                        }
                    }
                }
            }
            assertTrue(checked);
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        assertEquals(1, assertFileCounts(sstables.iterator().next().descriptor.directory.tryListNames()));

        validateCFS(cfs);
        truncate(cfs);
    }

    @Test
    public void testNumberOfFilesAndSizes()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startStorageMetricsLoad = StorageMetrics.load.getCount();
        long startUncompressedLoad = StorageMetrics.uncompressedLoad.getCount();
        long sBytesOnDisk = s.bytesOnDisk();
        long sBytesOnDiskUncompressed = s.logicalBytesOnDisk();
        Set<SSTableReader> compacting = Sets.newHashSet(s);

        List<SSTableReader> sstables;
        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));

            while(ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                    files++;
                    assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    assertEquals(s.bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.getCount());
                    assertEquals(s.bytesOnDisk(), cfs.metric.totalDiskSpaceUsed.getCount());

                }
            }
            sstables = rewriter.finish();
        }

        LifecycleTransaction.waitForDeletions();

        long sum = cfs.getLiveSSTables().stream().mapToLong(SSTableReader::bytesOnDisk).sum();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        long endLoad = StorageMetrics.load.getCount();
        assertEquals(startStorageMetricsLoad - sBytesOnDisk + sum, endLoad);

        long uncompressedSum = cfs.getLiveSSTables().stream().mapToLong(t -> t.logicalBytesOnDisk()).sum();
        long endUncompressedLoad = StorageMetrics.uncompressedLoad.getCount();
        assertEquals(startUncompressedLoad - sBytesOnDiskUncompressed + uncompressedSum, endUncompressedLoad);

        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getLiveSSTables().size());
        LifecycleTransaction.waitForDeletions();

        // tmplink and tmp files should be gone:
        assertEquals(sum, cfs.metric.totalDiskSpaceUsed.getCount());
        assertFileCounts(s.descriptor.directory.tryListNames());
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_dont_clean_readers()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);

        List<SSTableReader> sstables;
        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));

            while(ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                    files++;
                    assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }
            sstables = rewriter.finish();
        }

        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getLiveSSTables().size());
        LifecycleTransaction.waitForDeletions();

        assertFileCounts(s.descriptor.directory.tryListNames());
        validateCFS(cfs);
    }


    @Test
    public void testNumberOfFiles_abort() throws Exception
    {
        testNumberOfFiles_abort(new RewriterTest()
        {
            public void run(ISSTableScanner scanner,
                            CompactionController controller,
                            SSTableReader sstable,
                            ColumnFamilyStore cfs,
                            SSTableRewriter rewriter,
                            LifecycleTransaction txn)
            {
                try (CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
                {
                    int files = 1;
                    while (ci.hasNext())
                    {
                        rewriter.append(ci.next());
                        if (rewriter.currentWriter().getFilePointer() > 25000000)
                        {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory, txn));
                            files++;
                            assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                        }
                    }
                    rewriter.abort();
                }
            }
        });
    }

    @Test
    public void testNumberOfFiles_abort2() throws Exception
    {
        testNumberOfFiles_abort(new RewriterTest()
        {
            public void run(ISSTableScanner scanner,
                            CompactionController controller,
                            SSTableReader sstable,
                            ColumnFamilyStore cfs,
                            SSTableRewriter rewriter,
                            LifecycleTransaction txn)
            {
                try (CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
                {
                    int files = 1;
                    while (ci.hasNext())
                    {
                        rewriter.append(ci.next());
                        if (rewriter.currentWriter().getFilePointer() > 25000000)
                        {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory, txn));
                            files++;
                            assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                        }
                        if (files == 3)
                        {
                            //testing to abort when we have nothing written in the new file
                            rewriter.abort();
                            break;
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testNumberOfFiles_abort3() throws Exception
    {
        testNumberOfFiles_abort(new RewriterTest()
        {
            public void run(ISSTableScanner scanner,
                            CompactionController controller,
                            SSTableReader sstable,
                            ColumnFamilyStore cfs,
                            SSTableRewriter rewriter,
                            LifecycleTransaction txn)
            {
                try(CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
                {
                    int files = 1;
                    while (ci.hasNext())
                    {
                        rewriter.append(ci.next());
                        if (files == 1 && rewriter.currentWriter().getFilePointer() > 10000000)
                        {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory, txn));
                            files++;
                            assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                        }
                    }
                    rewriter.abort();
                }
            }
        });
    }

    private static interface RewriterTest
    {
        public void run(ISSTableScanner scanner,
                        CompactionController controller,
                        SSTableReader sstable,
                        ColumnFamilyStore cfs,
                        SSTableRewriter rewriter,
                        LifecycleTransaction txn);
    }

    private void testNumberOfFiles_abort(RewriterTest test)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        DecoratedKey origFirst = s.getFirst();
        DecoratedKey origLast = s.getLast();
        long startSize = cfs.metric.liveDiskSpaceUsed.getCount();
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false, true))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            test.run(scanner, controller, s, cfs, rewriter, txn);
        }

        LifecycleTransaction.waitForDeletions();

        assertEquals(startSize, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(1, cfs.getLiveSSTables().size());
        assertFileCounts(s.descriptor.directory.tryListNames());
        assertEquals(cfs.getLiveSSTables().iterator().next().getFirst(), origFirst);
        assertEquals(cfs.getLiveSSTables().iterator().next().getLast(), origLast);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_finish_empty_new_writer()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            while(ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getFilePointer() > 2500000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                    files++;
                    assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
                if (files == 3)
                {
                    //testing to finish when we have nothing written in the new file
                    rewriter.finish();
                    break;
                }
            }
        }

        LifecycleTransaction.waitForDeletions();

        assertEquals(files - 1, cfs.getLiveSSTables().size()); // we never wrote anything to the last file
        assertFileCounts(s.descriptor.directory.tryListNames());
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_truncate()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);

        List<SSTableReader> sstables;
        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            while(ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                    files++;
                    assertEquals(cfs.getLiveSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }

            rewriter.finish();
        }

        LifecycleTransaction.waitForDeletions();
        assertFileCounts(s.descriptor.directory.tryListNames());
        validateCFS(cfs);
    }

    @Test
    public void testSmallFiles()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 400);
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);

        List<SSTableReader> sstables;
        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 1000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID()))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            while(ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getOnDiskFilePointer() > 2500000)
                {
                    assertEquals(files, cfs.getLiveSSTables().size()); // all files are now opened early
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                    files++;
                }
            }

            sstables = rewriter.finish();
        }
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getLiveSSTables().size());
        LifecycleTransaction.waitForDeletions();
        assertFileCounts(s.descriptor.directory.tryListNames());

        validateCFS(cfs);
    }

    @Test
    public void testSSTableSplit()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.UNKNOWN, s))
        {
            SSTableSplitter splitter = new SSTableSplitter(cfs, txn, 10);
            splitter.split();

            assertFileCounts(s.descriptor.directory.tryListNames());
            LifecycleTransaction.waitForDeletions();

            for (File f : s.descriptor.directory.tryList())
            {
                // we need to clear out the data dir, otherwise tests running after this breaks
                FileUtils.deleteRecursive(f);
            }
        }
        truncate(cfs);
    }

    @Test
    public void testOfflineAbort() throws Exception
    {
        testAbortHelper(true, true);
    }
    @Test
    public void testOfflineAbort2() throws Exception
    {
        testAbortHelper(false, true);
    }

    @Test
    public void testAbort() throws Exception
    {
        testAbortHelper(false, false);
    }

    @Test
    public void testAbort2()
    {
        testAbortHelper(true, false);
    }

    private void testAbortHelper(boolean earlyException, boolean offline)
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        SSTableReader s = writeFile(cfs, 1000);
        if (!offline)
            cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        try (ISSTableScanner scanner = compacting.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = offline ? LifecycleTransaction.offline(OperationType.UNKNOWN, compacting)
                                       : cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 100, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID())
        )
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            while (ci.hasNext())
            {
                rewriter.append(ci.next());
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                }
            }
            try
            {
                rewriter.throwDuringPrepare(earlyException);
                rewriter.prepareToCommit();
            }
            catch (Throwable t)
            {
                rewriter.abort();
            }
        }
        finally
        {
            if (offline)
                s.selfRef().release();
        }

        LifecycleTransaction.waitForDeletions();

        int filecount = assertFileCounts(s.descriptor.directory.tryListNames());
        assertEquals(filecount, 1);
        if (!offline)
        {
            assertEquals(1, cfs.getLiveSSTables().size());
            validateCFS(cfs);
            truncate(cfs);
        }
        else
        {
            assertEquals(0, cfs.getLiveSSTables().size());
            cfs.truncateBlocking();
        }
        filecount = assertFileCounts(s.descriptor.directory.tryListNames());
        if (offline)
        {
            // the file is not added to the CFS, therefore not truncated away above
            assertEquals(1, filecount);
            for (File f : s.descriptor.directory.tryList())
            {
                FileUtils.deleteRecursive(f);
            }
            filecount = assertFileCounts(s.descriptor.directory.tryListNames());
        }

        assertEquals(0, filecount);
        truncate(cfs);
    }

    @Test
    public void testAllKeysReadable() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        for (int i = 0; i < 100; i++)
        {
            String key = Integer.toString(i);

            for (int j = 0; j < 10; j++)
                new RowUpdateBuilder(cfs.metadata(), 100, key)
                    .clustering(Integer.toString(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .apply();
        }
        Util.flush(cfs);
        cfs.forceMajorCompaction();
        validateKeys(keyspace);

        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader s = cfs.getLiveSSTables().iterator().next();
        Set<SSTableReader> compacting = new HashSet<>();
        compacting.add(s);

        int keyCount = 0;
        try (ISSTableScanner scanner = compacting.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 1, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID())
        )
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            while (ci.hasNext())
            {
                rewriter.append(ci.next());
                if (keyCount % 10 == 0)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
                }
                keyCount++;
                validateKeys(keyspace);
            }
            rewriter.finish();
        }
        validateKeys(keyspace);
        LifecycleTransaction.waitForDeletions();
        validateCFS(cfs);
        truncate(cfs);
    }

    @Test
    public void testCanonicalView()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = Sets.newHashSet(s);
        assertEquals(1, sstables.size());
        boolean checked = false;
        try (ISSTableScanner scanner = sstables.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, sstables, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false, true);
             CompactionIterator ci = new CompactionIterator(COMPACTION, singletonList(scanner), controller, nowInSeconds(), nextTimeUUID())
        )
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                writer.append(ci.next());
                if (!checked && writer.currentWriter().getFilePointer() > 15000000)
                {
                    checked = true;
                    ColumnFamilyStore.ViewFragment viewFragment = cfs.select(View.selectFunction(SSTableSet.CANONICAL));
                    // canonical view should have only one SSTable which is not opened early.
                    assertEquals(1, viewFragment.sstables.size());
                    SSTableReader sstable = viewFragment.sstables.get(0);
                    assertEquals(s.descriptor, sstable.descriptor);
                    assertTrue("Found early opened SSTable in canonical view: " + sstable.getFilename(), sstable.openReason != SSTableReader.OpenReason.EARLY);
                }
            }
        }
        truncateCF();
        validateCFS(cfs);
    }

    /**
     * emulates anticompaction - writing from one source sstable to two new sstables
     */
    @Test
    public void testTwoWriters()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = Sets.newHashSet(s);
        assertEquals(1, sstables.size());
        long nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = SSTableRewriter.constructWithoutEarlyOpening(txn, false, 1000);
             SSTableRewriter writer2 = SSTableRewriter.constructWithoutEarlyOpening(txn, false, 1000);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(COMPACTION, scanners.scanners, controller, nowInSec, nextTimeUUID())
             )
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            writer2.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                if (writer.currentWriter().getFilePointer() < 15000000)
                    writer.append(ci.next());
                else
                    writer2.append(ci.next());
            }
            for (int i = 0; i < 5000; i++)
                assertFalse(Util.getOnlyPartition(Util.cmd(cfs, ByteBufferUtil.bytes(i)).build()).isEmpty());
        }
        truncateCF();
        validateCFS(cfs);
    }

    @Test
    public void testCanonicalSSTables() throws ExecutionException, InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        final ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        cfs.addSSTable(writeFile(cfs, 100));
        Collection<SSTableReader> allSSTables = cfs.getLiveSSTables();
        assertEquals(1, allSSTables.size());
        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failed = new AtomicBoolean(false);
        Runnable r = () -> {
            while (!done.get())
            {
                Iterable<SSTableReader> sstables = cfs.getSSTables(SSTableSet.CANONICAL);
                if (Iterables.size(sstables) != 1)
                {
                    failed.set(true);
                    return;
                }
            }
        };
        Thread t = NamedThreadFactory.createAnonymousThread(r);
        try
        {
            t.start();
            cfs.forceMajorCompaction();
        }
        finally
        {
            done.set(true);
            t.join(20);
        }
        assertFalse(failed.get());


    }

    /**
     * tests SSTableRewriter ctor arg controlling whether writers metadata buffers are released.
     * Verifies that writers trip an assert when updated after cleared on switch
     *
     * CASSANDRA-14834
     */
    @Test
    public void testWriterClearing()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();

        // Can't update a writer that is eagerly cleared on switch
        boolean eagerWriterMetaRelease = true;
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(new HashSet<>(), OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 1000000, false, eagerWriterMetaRelease)
        )
        {
            SSTableWriter firstWriter = getWriter(cfs, dir, txn);
            rewriter.switchWriter(firstWriter);
            rewriter.switchWriter(getWriter(cfs, dir, txn));
            try
            {
                UnfilteredRowIterator uri = mock(UnfilteredRowIterator.class);
                when(uri.partitionLevelDeletion()).thenReturn(DeletionTime.build(0, 0));
                when(uri.partitionKey()).thenReturn(bopKeyFromInt(0));
                // should not be able to append after buffer release on switch
                firstWriter.append(uri);
                fail("Expected AssertionError was not thrown.");
            }
            catch (AssertionError ae)
            {
                if (!ae.getMessage().contains("update is being called after releaseBuffers") && !ae.getMessage().contains("Attempt to use a closed data output"))
                    throw ae;
            }
        }
    }

    static DecoratedKey bopKeyFromInt(int i)
    {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(i);
        bb.rewind();
        return ByteOrderedPartitioner.instance.decorateKey(bb);
    }

    private void validateKeys(Keyspace ks)
    {
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(ks.getColumnFamilyStore(CF), key).build());
            assertTrue(partition != null && partition.rowCount() > 0);
        }
    }

    public static SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        return Iterables.getFirst(writeFiles(cfs, 1, count * 5, count / 100), null);
    }

    public static Set<SSTableReader> writeFiles(ColumnFamilyStore cfs, int fileCount, int partitionCount, int cellCount)
    {
        int i = 0;
        Set<SSTableReader> result = new LinkedHashSet<>();
        for (int f = 0 ; f < fileCount ; f++)
        {
            File dir = cfs.getDirectories().getDirectoryForNewSSTables();
            Descriptor desc = cfs.newSSTableDescriptor(dir);

            try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, desc, 0, 0, null, false, new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS)))
            {
                int end = f == fileCount - 1 ? partitionCount : ((f + 1) * partitionCount) / fileCount;
                for ( ; i < end ; i++)
                {
                    UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), ByteBufferUtil.bytes(i));
                    for (int j = 0; j < cellCount ; j++)
                        builder.newRow(Integer.toString(i)).add("val", random(0, 1000));

                    writer.append(builder.build().unfilteredIterator());
                }
                result.addAll(writer.finish(true));
            }
        }
        return result;
    }
}
