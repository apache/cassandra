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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
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
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    }


    @Test
    public void basicTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
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
        SSTableRewriter writer = new SSTableRewriter(cfs, sstables, 1000, false);
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);)
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while(scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
            }
        }
        Collection<SSTableReader> newsstables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newsstables , OperationType.COMPACTION);
        Thread.sleep(100);
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
        assertEquals(1, filecounts);

    }
    @Test
    public void basicTest2() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter writer = new SSTableRewriter(cfs, sstables, 1000, false);
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);)
        {
            ISSTableScanner scanner = scanners.scanners.get(0);
            CompactionController controller = new CompactionController(cfs, sstables, cfs.gcBefore(System.currentTimeMillis()));
            writer.switchWriter(getWriter(cfs, sstables.iterator().next().descriptor.directory));
            while (scanner.hasNext())
            {
                AbstractCompactedRow row = new LazilyCompactedRow(controller, Arrays.asList(scanner.next()));
                writer.append(row);
            }
        }
        Collection<SSTableReader> newsstables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newsstables, OperationType.COMPACTION);
        Thread.sleep(100);
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
        assertEquals(1, filecounts);
    }

    @Test
    public void getPositionsTest() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter writer = new SSTableRewriter(cfs, sstables, 1000, false);
        boolean checked = false;
        try (AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategy().getScanners(sstables);)
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
                            SSTableReader c = sstables.iterator().next();
                            long lastKeySize = sstable.getPosition(sstable.last, SSTableReader.Operator.GT).position - sstable.getPosition(sstable.last, SSTableReader.Operator.EQ).position;
                            Collection<Range<Token>> r = Arrays.asList(new Range<>(cfs.partitioner.getMinimumToken(), cfs.partitioner.getMinimumToken()));
                            List<Pair<Long, Long>> tmplinkPositions = sstable.getPositionsForRanges(r);
                            List<Pair<Long, Long>> compactingPositions = c.getPositionsForRanges(r);
                            assertEquals(1, tmplinkPositions.size());
                            assertEquals(1, compactingPositions.size());
                            assertEquals(0, tmplinkPositions.get(0).left.longValue());
                            // make sure we have one key overlap between the early opened file and the compacting one:
                            assertEquals(tmplinkPositions.get(0).right.longValue(), compactingPositions.get(0).left + lastKeySize);
                            assertEquals(c.uncompressedLength(), compactingPositions.get(0).right.longValue());
                        }
                    }
                }
            }
        }
        assertTrue(checked);
        Collection<SSTableReader> newsstables = writer.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(sstables, newsstables, OperationType.COMPACTION);
        Thread.sleep(100);
        validateCFS(cfs);
        int filecounts = assertFileCounts(sstables.iterator().next().descriptor.directory.list(), 0, 0);
        assertEquals(1, filecounts);
        cfs.truncateBlocking();
        Thread.sleep(1000); // make sure the deletion tasks have run etc
        validateCFS(cfs);
    }

    @Test
    public void testFileRemoval() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < 100; i++)
            cf.addColumn(Util.cellname(i), ByteBuffer.allocate(1000), 1);
        File dir = cfs.directories.getDirectoryForNewSSTables();

        SSTableWriter writer = getWriter(cfs, dir);
        for (int i = 0; i < 500; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        SSTableReader s = writer.openEarly(1000);
        assertFileCounts(dir.list(), 2, 3);

        for (int i = 500; i < 1000; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        SSTableReader s2 = writer.openEarly(1000);

        assertTrue(s != s2);
        assertFileCounts(dir.list(), 2, 3);

        s.setReplacedBy(s2);
        s2.markObsolete();
        s.releaseReference();
        s2.releaseReference();

        writer.abort(false);

        Thread.sleep(1000);
        int datafiles = assertFileCounts(dir.list(), 0, 0);
        assertEquals(datafiles, 0);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFilesAndSizes() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startStorageMetricsLoad = StorageMetrics.load.count();
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                    assertEquals(s.bytesOnDisk(), cfs.metric.liveDiskSpaceUsed.count());
                    assertEquals(s.bytesOnDisk(), cfs.metric.totalDiskSpaceUsed.count());

                }
            }
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        long sum = 0;
        for (SSTableReader x : cfs.getSSTables())
            sum += x.bytesOnDisk();
        assertEquals(sum, cfs.metric.liveDiskSpaceUsed.count());
        assertEquals(startStorageMetricsLoad - s.bytesOnDisk() + sum, StorageMetrics.load.count());
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        // tmplink and tmp files should be gone:
        assertEquals(sum, cfs.metric.totalDiskSpaceUsed.count());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_dont_clean_readers() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
        }
        List<SSTableReader> sstables = rewriter.finish();
        assertEquals(files, sstables.size());
        assertEquals(files + 1, cfs.getSSTables().size());
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }


    @Test
    public void testNumberOfFiles_abort() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        long startSize = cfs.metric.liveDiskSpaceUsed.count();
        DecoratedKey origFirst = s.first;
        DecoratedKey origLast = s.last;
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
        }
        rewriter.abort();
        Thread.sleep(1000);
        assertEquals(startSize, cfs.metric.liveDiskSpaceUsed.count());
        assertEquals(1, cfs.getSSTables().size());
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        assertEquals(cfs.getSSTables().iterator().next().first, origFirst);
        assertEquals(cfs.getSSTables().iterator().next().last, origLast);
        validateCFS(cfs);

    }

    @Test
    public void testNumberOfFiles_abort2() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        DecoratedKey origFirst = s.first;
        DecoratedKey origLast = s.last;
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
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
        Thread.sleep(1000);
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
        cfs.truncateBlocking();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);

        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
            while(scanner.hasNext())
            {
                rewriter.append(new LazilyCompactedRow(controller, Arrays.asList(scanner.next())));
                if (rewriter.currentWriter().getOnDiskFilePointer() > 25000000)
                {
                    rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));
                    files++;
                    assertEquals(cfs.getSSTables().size(), files); // we have one original file plus the ones we have switched out.
                }
                if (files == 3)
                {
                    //testing to finish when we have nothing written in the new file
                    List<SSTableReader> sstables = rewriter.finish();
                    cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
                    break;
                }
            }
        }
        Thread.sleep(1000);
        assertEquals(files - 1, cfs.getSSTables().size()); // we never wrote anything to the last file
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }

    @Test
    public void testNumberOfFiles_truncate() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 1000);
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        cfs.truncateBlocking();
        Thread.sleep(1000); // make sure the deletion tasks have run etc
        validateCFS(cfs);
    }

    @Test
    public void testSmallFiles() throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();

        SSTableReader s = writeFile(cfs, 400);
        cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(1000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        rewriter.switchWriter(getWriter(cfs, s.descriptor.directory));

        int files = 1;
        try (ISSTableScanner scanner = s.getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
        }
        List<SSTableReader> sstables = rewriter.finish();
        cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, sstables, OperationType.COMPACTION);
        assertEquals(files, sstables.size());
        assertEquals(files, cfs.getSSTables().size());
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        validateCFS(cfs);
    }
    @Test
    public void testSSTableSplit() throws InterruptedException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        cfs.disableAutoCompaction();
        SSTableReader s = writeFile(cfs, 1000);
        cfs.getDataTracker().markCompacting(Arrays.asList(s));
        SSTableSplitter splitter = new SSTableSplitter(cfs, s, 10);
        splitter.split();
        Thread.sleep(1000);
        assertFileCounts(s.descriptor.directory.list(), 0, 0);
        for (File f : s.descriptor.directory.listFiles())
        {
            // we need to clear out the data dir, otherwise tests running after this breaks
            f.delete();
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
        cfs.truncateBlocking();
        SSTableReader s = writeFile(cfs, 1000);
        if (!offline)
            cfs.addSSTable(s);
        Set<SSTableReader> compacting = Sets.newHashSet(s);
        SSTableRewriter.overrideOpenInterval(10000000);
        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, offline);
        SSTableWriter w = getWriter(cfs, s.descriptor.directory);
        rewriter.switchWriter(w);
        try (ISSTableScanner scanner = compacting.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
                rewriter.finishAndThrow(earlyException);
            }
            catch (Throwable t)
            {
                rewriter.abort();
            }
        }
        Thread.sleep(1000);
        int filecount = assertFileCounts(s.descriptor.directory.list(), 0, 0);
        assertEquals(filecount, 1);
        if (!offline)
        {
            assertEquals(1, cfs.getSSTables().size());
            validateCFS(cfs);
        }
        cfs.truncateBlocking();
        Thread.sleep(1000);
        filecount = assertFileCounts(s.descriptor.directory.list(), 0, 0);
        if (offline)
        {
            // the file is not added to the CFS, therefor not truncated away above
            assertEquals(1, filecount);
            for (File f : s.descriptor.directory.listFiles())
            {
                f.delete();
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
        cfs.truncateBlocking();
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
        cfs.getDataTracker().markCompacting(compacting);

        SSTableRewriter rewriter = new SSTableRewriter(cfs, compacting, 1000, false);
        SSTableRewriter.overrideOpenInterval(1);
        SSTableWriter w = getWriter(cfs, s.descriptor.directory);
        rewriter.switchWriter(w);
        int keyCount = 0;
        try (ISSTableScanner scanner = compacting.iterator().next().getScanner();
             CompactionController controller = new CompactionController(cfs, compacting, 0))
        {
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
            try
            {
                cfs.getDataTracker().markCompactedSSTablesReplaced(compacting, rewriter.finish(), OperationType.COMPACTION);
                cfs.getDataTracker().unmarkCompacting(compacting);
            }
            catch (Throwable t)
            {
                rewriter.abort();
            }
        }
        validateKeys(keyspace);
        Thread.sleep(1000);
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

    private SSTableReader writeFile(ColumnFamilyStore cfs, int count)
    {
        ArrayBackedSortedColumns cf = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        for (int i = 0; i < count / 100; i++)
            cf.addColumn(Util.cellname(i), ByteBuffer.allocate(1000), 1);
        File dir = cfs.directories.getDirectoryForNewSSTables();
        String filename = cfs.getTempSSTablePath(dir);

        SSTableWriter writer = SSTableWriter.create(filename, 0, 0);

        for (int i = 0; i < count * 5; i++)
            writer.append(StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(i)), cf);
        return writer.closeAndOpenReader();
    }

    private void validateCFS(ColumnFamilyStore cfs)
    {
        Set<Integer> liveDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.referenceCount());
            liveDescriptors.add(sstable.descriptor.generation);
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
        assertTrue(cfs.getDataTracker().getCompacting().isEmpty());
    }


    private int assertFileCounts(String [] files, int expectedtmplinkCount, int expectedtmpCount)
    {
        int tmplinkcount = 0;
        int tmpcount = 0;
        int datacount = 0;
        for (String f : files)
        {
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

    private SSTableWriter getWriter(ColumnFamilyStore cfs, File directory)
    {
        String filename = cfs.getTempSSTablePath(directory);
        return SSTableWriter.create(filename, 0, 0);
    }
}
