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
import org.junit.BeforeClass;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.LazilyCompactedRow;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

public class SSTableRewriterTest extends SchemaLoader
{
    private static final String KEYSPACE = "SSTableRewriterTest";
    private static final String CF = "Standard1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                SimpleStrategy.class,
                KSMetaData.optsWithRF(1),
                SchemaLoader.standardCFMD(KEYSPACE, CF));
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
        SSTableDeletingTask.waitForDeletions();
    }

    @Test
    public void basicTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());

        for (int j = 0; j < 100; j ++)
        {
            ByteBuffer key = ByteBufferUtil.bytes(String.valueOf(j));
            Mutation rm = new Mutation(KEYSPACE, key);
            rm.add(CF, Util.cellname("0"), ByteBufferUtil.EMPTY_BYTE_BUFFER, j);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        assertEquals(sstables.iterator().next().bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.getCount());
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(cfs, txn, 1000, false);)
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while(scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
            }
            writer.finish();
        }
        SSTableDeletingTask.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
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
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());

        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(cfs, txn, 1000, false, 10000000);)
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while (scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
            }
            writer.finish();
        }
        SSTableDeletingTask.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
        assertEquals(1, filecounts);
    }

    @Test
    public void getPositionsTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());

        boolean checked = false;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(cfs, txn, 1000, false, 10000000))
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while (scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
                if (!checked && writer.currentWriter().getFilePointer() > 15000000)
                {
                    checked = true;
                    for (SSTableReader sstable : cfs.getSSTables())
                    {
                        if (sstable.openReason == SSTableReader.OpenReason.EARLY)
                        {
                            SSTableReader c = txn.current(sstables.iterator().next());
                            Collection<Range<Token>> r = Arrays.asList(new Range<>(cfs.partitioner.getMinimumToken(), cfs.partitioner.getMinimumToken()));
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
        SSTableDeletingTask.waitForDeletions();
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
        assertEquals(1, filecounts);
        truncate(cfs);
        SSTableDeletingTask.waitForDeletions();
        validateCFS(cfs);
    }

    @Test
    public void testFileRemoval() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < 100; i++)
            cf.addColumn(Util.cellname(i), ByteBuffer.allocate(1000), 1);
        File dir = cfs.directories.getDirectoryForNewSSTables();

        try (SSTableWriter writer = getWriter(cfs, dir);)
        {
            for (int i = 0; i < 10000; i++)
                writer.append(StorageService.getPartitioner().decorateKey(random(i, 10)), cf);
            SSTableReader s = writer.setMaxDataAge(1000).openEarly();
            assert s != null;
            assertFileCounts(dir.list(), 2, 2);
            for (int i = 10000; i < 20000; i++)
                writer.append(StorageService.getPartitioner().decorateKey(random(i, 10)), cf);
            SSTableReader s2 = writer.setMaxDataAge(1000).openEarly();
            assertTrue(s.last.compareTo(s2.last) < 0);
            assertFileCounts(dir.list(), 2, 2);
            s.markObsolete();
            s.selfRef().release();
            s2.selfRef().release();
            // These checks don't work on Windows because the writer has the channel still
            // open till .abort() is called (via the builder)
            if (!FBUtilities.isWindows())
            {
                SSTableDeletingTask.waitForDeletions();
                assertFileCounts(dir.list(), 0, 2);
            }
            writer.abort();
            SSTableDeletingTask.waitForDeletions();
            int datafiles = assertFileCounts(dir.list(), 0, 0);
            assertEquals(datafiles, 0);
            validateCFS(cfs);
        }
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 10000000))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    assertEquals(s.bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.getCount());
                    assertEquals(s.bytesOnDisk(), cfs.metric.totalDiskSpaceUsed.getCount());

                }
            }
            sstables = rewriter.finish();
        }
        long sum = 0;
        for (SSTableReader x : cfs.getSSTables())
            sum += x.bytesOnDisk();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(startStorageMetricsLoad - sBytesOnDisk + sum, StorageMetrics.load.getCount());
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        SSTableDeletingTask.waitForDeletions();

        // tmplink and tmp files should be gone:
        assertEquals(sum, cfs.metric.totalDiskSpaceUsed.getCount());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 10000000))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }
            sstables = rewriter.finish();
        }

        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        SSTableDeletingTask.waitForDeletions();

        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }


    @Test
    public void testNumberOfFiles_abort() throws Exception
    {
        testNumberOfFiles_abort(new RewriterTest()
        {
            public void run(ISSTableScanner scanner, CompactionController controller, SSTableReader sstable, ColumnFamilyStore cfs, SSTableRewriter rewriter)
            {
                int files = 1;
                while(scanner.hasNext())
                {
                    rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                    if (rewriter.currentWriter().getFilePointer() > 25000000)
                    {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory));
                        files++;
                        assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    }
                }
                rewriter.abort();
            }
        });
    }

    @Test
    public void testNumberOfFiles_abort2() throws Exception
    {
        testNumberOfFiles_abort(new RewriterTest()
        {
            public void run(ISSTableScanner scanner, CompactionController controller, SSTableReader sstable, ColumnFamilyStore cfs, SSTableRewriter rewriter)
            {
                int files = 1;
                while(scanner.hasNext())
                {
                    rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                    if (rewriter.currentWriter().getFilePointer() > 25000000)
                    {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory));
                        files++;
                        assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    }
                    if (files == 3)
                    {
                        //testing to abort when we have nothing written in the new file
                        rewriter.abort();
                        break;
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
            public void run(ISSTableScanner scanner, CompactionController controller, SSTableReader sstable, ColumnFamilyStore cfs, SSTableRewriter rewriter)
            {
                int files = 1;
                while(scanner.hasNext())
                {
                    rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                    if (files == 1 && rewriter.currentWriter().getFilePointer() > 10000000)
                    {
                        rewriter.switchWriter(getWriter(cfs, sstable.descriptor.directory));
                        files++;
                        assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    }
                }
                rewriter.abort();
            }
        });
    }

    private static interface RewriterTest
    {
        public void run(ISSTableScanner scanner, CompactionController controller, SSTableReader sstable, ColumnFamilyStore cfs, SSTableRewriter rewriter);
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 10000000);)
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            test.run(scanner, controller, s, cfs, rewriter);
        }

        SSTableDeletingTask.waitForDeletions();

        assertEquals(startSize, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(1, cfs.getSSTables().size());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        assertEquals(cfs.getSSTables().iterator().next().first, origFirst);
        assertEquals(cfs.getSSTables().iterator().next().last, origLast);
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 10000000))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
                if (files == 3)
                {
                    //testing to finish when we have nothing written in the new file
                    rewriter.finish();
                    break;
                }
            }
        }

        SSTableDeletingTask.waitForDeletions();

        assertEquals(files - 1, cfs.getSSTables().size()); // we never wrote anything to the last file
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 10000000))
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
            }

            sstables = rewriter.finish();
        }

        SSTableDeletingTask.waitForDeletions();
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        cfs.truncateBlocking();
        SSTableDeletingTask.waitForDeletions();
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 1000000);)
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 2500000)
                {
                    assertEquals(files, cfs.getSSTables().size()); // all files are now opened early
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                }
            }

            sstables = rewriter.finish();
        }

        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        SSTableDeletingTask.waitForDeletions();
        assertFileCounts(s.descriptor.directory.list(), 0, 0);

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

            assertFileCounts(s.descriptor.directory.list(), 0, 0);

            s.selfRef().release();
            SSTableDeletingTask.waitForDeletions();

            for (File f : s.descriptor.directory.listFiles())
            {
                // we need to clear out the data dir, otherwise tests running after this breaks
                FileUtils.deleteRecursive(f);
            }
        }
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
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, offline, 10000000);
        )
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while (scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
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

        SSTableDeletingTask.waitForDeletions();

        int filecount = assertFileCounts(s.descriptor.directory.list(), 0, 0);
        assertEquals(filecount, 1);
        if (!offline)
        {
            assertEquals(1, cfs.getSSTables().size());
            validateCFS(cfs);
        }
        truncate(cfs);
        SSTableDeletingTask.waitForDeletions();
        filecount = assertFileCounts(s.descriptor.directory.list(), 0, 0);
        if (offline)
        {
            // the file is not added to the CFS, therefore not truncated away above
            assertEquals(1, filecount);
            for (File f : s.descriptor.directory.listFiles())
            {
                FileUtils.deleteRecursive(f);
            }
            filecount = assertFileCounts(s.descriptor.directory.list(), 0, 0);
        }

        assertEquals(0, filecount);
    }

    @Test
    public void testAllKeysReadable() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        truncate(cfs);
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add(CF, Util.cellname(Integer.toString(j)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 100);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        cfs.forceMajorCompaction();
        validateKeys(keyspace);

        assertEquals(1, cfs.getSSTables().size());
        SSTableReader s = cfs.getSSTables().iterator().next();
        Set<SSTableReader> compacting = new HashSet<>();
        compacting.add(s);

        int keyCount = 0;
        try (ISSTableScanner scanner = compacting.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(compacting, OperationType.UNKNOWN);
             SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false, 1);
        )
        {
            rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
            while (scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (keyCount % 10 == 0)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                }
                keyCount++;
                validateKeys(keyspace);
            }
            rewriter.finish();
        }
        validateKeys(keyspace);
        SSTableDeletingTask.waitForDeletions();
        validateCFS(cfs);
    }

    @Test
    public void testCanonicalView() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = Sets.newHashSet(s);
        assertEquals(1, sstables.size());
        boolean checked = false;
        try (ISSTableScanner scanner = sstables.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, sstables, 0);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(cfs, txn, 1000, false, 10000000);
        )
        {
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while (scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Collections.singletonList(scanner.next()));
                writer.append(row);
                if (!checked && writer.currentWriter().getFilePointer() > 15000000)
                {
                    checked = true;
                    ColumnFamilyStore.ViewFragment viewFragment = cfs.select(ColumnFamilyStore.CANONICAL_SSTABLES);
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
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = Sets.newHashSet(s);
        assertEquals(1, sstables.size());
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(sstables);
             LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN);
             SSTableRewriter writer = new SSTableRewriter(cfs, txn, 1000, false, false);
             SSTableRewriter writer2 = new SSTableRewriter(cfs, txn, 1000, false, false))
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            writer2.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while (scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Collections.singletonList(scanner.next()));

                if (writer.currentWriter().getFilePointer() < 15000000)
                    writer.append(row);
                else
                    writer2.append(row);
            }
            for (int i = 0; i < 5000; i++)
            {
                DecoratedKey key = Util.dk(ByteBufferUtil.bytes(i));
                ColumnFamily cf = Util.getColumnFamily(keyspace, key, CF);
                assertTrue(cf != null);
            }
        }
        truncateCF();
        validateCFS(cfs);
    }


    private void validateKeys(Keyspace ks)
    {
        for (int i = 0; i < 100; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            ColumnFamily cf = Util.getColumnFamily(ks, key, CF);
            assertTrue(cf != null);
        }
    }

    public static void truncate(ColumnFamilyStore cfs)
    {
        cfs.truncateBlocking();
        Uninterruptibles.sleepUninterruptibly(10L,TimeUnit.MILLISECONDS);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());
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
            File dir = cfs.directories.getDirectoryForNewSSTables();
            String filename = cfs.getTempSSTablePath(dir);

            SSTableWriter writer = SSTableWriter.create(filename, 0, 0);
            int end = f == fileCount - 1 ? partitionCount : ((f + 1) * partitionCount) / fileCount;
            for ( ; i < end ; i++)
            {
                ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
                for (int j = 0; j < cellCount ; j++)
                    cf.addColumn(Util.cellname(j), random(0, cellSize), 1);
                writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
            }
            result.add(writer.finish(true));
        }
        return result;
    }

    public static void validateCFS(ColumnFamilyStore cfs)
    {
        Set<Integer> liveDescriptors = new HashSet<>();
        long spaceUsed = 0;
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
            liveDescriptors.add(sstable.descriptor.generation);
            spaceUsed += sstable.bytesOnDisk();
        }
        for (File dir : cfs.directories.getCFDirectories())
        {
            for (File f : dir.listFiles())
            {
                if (f.getName().contains("Data"))
                {
                    Descriptor d = Descriptor.fromFilename(f.getAbsolutePath());
                    assertTrue(d.toString(), liveDescriptors.contains(d.generation));
                }
            }
        }
        assertEquals(spaceUsed, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(spaceUsed, cfs.metric.totalDiskSpaceUsed.getCount());
        assertTrue(cfs.getTracker().getCompacting().isEmpty());
    }


    public static int assertFileCounts(String [] files, int expectedtmplinkCount, int expectedtmpCount)
    {
        int tmplinkcount = 0;
        int tmpcount = 0;
        int datacount = 0;
        for (String f : files)
        {
            if (f.endsWith("-CRC.db"))
                continue;
            if (f.contains("tmplink-"))
                tmplinkcount++;
            else if (f.contains("tmp-"))
                tmpcount++;
            else if (f.contains("Data"))
                datacount++;
        }
        assertEquals(expectedtmplinkCount, tmplinkcount);
        assertEquals(expectedtmpCount, tmpcount);
        return datacount;
    }

    public static SSTableWriter getWriter(ColumnFamilyStore cfs, File directory)
    {
        String filename = cfs.getTempSSTablePath(directory);
        return SSTableWriter.create(filename, 0, 0);
    }

    public static ByteBuffer random(int i, int size)
    {
        byte[] bytes = new byte[size + 4];
        ThreadLocalRandom.current().nextBytes(bytes);
        ByteBuffer r = ByteBuffer.wrap(bytes);
        r.putInt(0, i);
        return r;
    }

}
