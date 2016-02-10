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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionIterator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.*;

public class SSTableRewriterTest extends SSTableWriterTestBase
{
    @Test
    public void basicTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        for (int j = 0; j < 100; j ++)
        {
            new RowUpdateBuilder(cfs.metadata, j, String.valueOf(j))
                .clustering("0")
                .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                .build()
                .apply();
        }
        cfs.forceBlockingFlush();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());
        assertEquals(sstables.iterator().next().bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.getCount());
        int nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = SSTableRewriter.constructKeepingOriginals(txn, false, 1000);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while(ci.hasNext())
            {
                writer.append(ci.next());
            }
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list());
        assertEquals(1, filecounts);
        truncate(cfs);
    }
    @Test
    public void basicTest2() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());

        int nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID()))
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                writer.append(ci.next());
            }
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list());
        assertEquals(1, filecounts);
    }

    @Test
    public void getPositionsTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertEquals(1, sstables.size());

        int nowInSec = FBUtilities.nowInSeconds();
        boolean checked = false;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID()))
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
                            List<Pair<Long, Long>> tmplinkPositions = sstable.getPositionsForRanges(r);
                            List<Pair<Long, Long>> compactingPositions = c.getPositionsForRanges(r);
                            assertEquals(1, tmplinkPositions.size());
                            assertEquals(1, compactingPositions.size());
                            assertEquals(0, tmplinkPositions.get(0).left.longValue());
                            // make sure we have no overlap between the early opened file and the compacting one:
                            assertEquals(tmplinkPositions.get(0).right.longValue(), compactingPositions.get(0).left.longValue());
                            assertEquals(c.uncompressedLength(), compactingPositions.get(0).right.longValue());
                        }
                    }
                }
            }
            assertTrue(checked);
            writer.finish();
        }
        LifecycleTransaction.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list());
        assertEquals(1, filecounts);
        truncate(cfs);
    }

    @Test
    public void testNumberOfFilesAndSizes() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startStorageMetricsLoad = StorageMetrics.load.getCount();
        long sBytesOnDisk = s.bytesOnDisk();
        Set<SSTableReader> compacting = Sets.newHashSet(s);

        List<SSTableReader> sstables;
        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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

        long sum = 0;
        for (SSTableReader x : cfs.getLiveSSTables())
            sum += x.bytesOnDisk();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(startStorageMetricsLoad - sBytesOnDisk + sum, StorageMetrics.load.getCount());
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getLiveSSTables().size());
        LifecycleTransaction.waitForDeletions();

        // tmplink and tmp files should be gone:
        assertEquals(sum, cfs.metric.totalDiskSpaceUsed.getCount());
        assertFileCounts(s.descriptor.directory.list());
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_dont_clean_readers() throws Exception
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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

        assertFileCounts(s.descriptor.directory.list());
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
                try (CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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
                try (CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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
                try(CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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

    private void testNumberOfFiles_abort(RewriterTest test) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        DecoratedKey origFirst = s.first;
        DecoratedKey origLast = s.last;
        long startSize = cfs.metric.liveDiskSpaceUsed.getCount();
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory, txn));
            test.run(scanner, controller, s, cfs, rewriter, txn);
        }

        LifecycleTransaction.waitForDeletions();

        assertEquals(startSize, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(1, cfs.getLiveSSTables().size());
        assertFileCounts(s.descriptor.directory.list());
        assertEquals(cfs.getLiveSSTables().iterator().next().first, origFirst);
        assertEquals(cfs.getLiveSSTables().iterator().next().last, origLast);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_finish_empty_new_writer() throws Exception
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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
        assertFileCounts(s.descriptor.directory.list());
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_truncate() throws Exception
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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

        LifecycleTransaction.waitForDeletions();
        assertFileCounts(s.descriptor.directory.list());
        validateCFS(cfs);
    }

    @Test
    public void testSmallFiles() throws Exception
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 1000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID()))
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
        assertFileCounts(s.descriptor.directory.list());

        validateCFS(cfs);
    }

    @Test
    public void testSSTableSplit() throws InterruptedException
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

            assertFileCounts(s.descriptor.directory.list());
            LifecycleTransaction.waitForDeletions();

            for (File f : s.descriptor.directory.listFiles())
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
    public void testAbort2() throws Exception
    {
        testAbortHelper(true, false);
    }

    private void testAbortHelper(boolean earlyException, boolean offline) throws Exception
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 100, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID())
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

        int filecount = assertFileCounts(s.descriptor.directory.list());
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
        filecount = assertFileCounts(s.descriptor.directory.list());
        if (offline)
        {
            // the file is not added to the CFS, therefore not truncated away above
            assertEquals(1, filecount);
            for (File f : s.descriptor.directory.listFiles())
            {
                FileUtils.deleteRecursive(f);
            }
            filecount = assertFileCounts(s.descriptor.directory.list());
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
                new RowUpdateBuilder(cfs.metadata, 100, key)
                    .clustering(Integer.toString(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .apply();
        }
        cfs.forceBlockingFlush();
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
             SSTableRewriter rewriter = new SSTableRewriter(txn, 1000, 1, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID())
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
    public void testCanonicalView() throws Exception
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
             SSTableRewriter writer = new SSTableRewriter(txn, 1000, 10000000, false);
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, Collections.singletonList(scanner), controller, FBUtilities.nowInSeconds(), UUIDGen.getTimeUUID())
        )
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory, txn));
            while (ci.hasNext())
            {
                writer.append(ci.next());
                if (!checked && writer.currentWriter().getFilePointer() > 15000000)
                {
                    checked = true;
                    ColumnFamilyStore.ViewFragment viewFragment = cfs.select(View.select(SSTableSet.CANONICAL));
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
     *
     * @throws IOException
     */
    @Test
    public void testTwoWriters() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = Sets.newHashSet(s);
        assertEquals(1, sstables.size());
        int nowInSec = FBUtilities.nowInSeconds();
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = SSTableRewriter.constructWithoutEarlyOpening(txn, false, 1000);
             SSTableRewriter writer2 = SSTableRewriter.constructWithoutEarlyOpening(txn, false, 1000);
             CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(nowInSec));
             CompactionIterator ci = new CompactionIterator(OperationType.COMPACTION, scanners.scanners, controller, nowInSec, UUIDGen.getTimeUUID())
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
        return Iterables.getFirst(writeFiles(cfs, 1, count * 5, count / 100, 1000), null);
    }

    public static Set<SSTableReader> writeFiles(ColumnFamilyStore cfs, int fileCount, int partitionCount, int cellCount, int cellSize)
    {
        int i = 0;
        Set<SSTableReader> result = new LinkedHashSet<>();
        for (int f = 0 ; f < fileCount ; f++)
        {
            File dir = cfs.getDirectories().getDirectoryForNewSSTables();
            String filename = cfs.getSSTablePath(dir);

            try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, filename, 0, 0, new SerializationHeader(true, cfs.metadata, cfs.metadata.partitionColumns(), EncodingStats.NO_STATS)))
            {
                int end = f == fileCount - 1 ? partitionCount : ((f + 1) * partitionCount) / fileCount;
                for ( ; i < end ; i++)
                {
                    UpdateBuilder builder = UpdateBuilder.create(cfs.metadata, ByteBufferUtil.bytes(i));
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
