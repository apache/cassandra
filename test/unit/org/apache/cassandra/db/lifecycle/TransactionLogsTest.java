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
package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedSegmentedFile;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;
import org.apache.cassandra.utils.concurrent.Transactional;

public class TransactionLogsTest extends AbstractTransactionalTest
{
    private static final String KEYSPACE = "TransactionLogsTest";

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    protected AbstractTransactionalTest.TestableTransaction newTest() throws Exception
    {
        TransactionLogs.waitForDeletions();
        SSTableReader.resetTidying();
        return new TxnTest();
    }

    private static final class TxnTest extends TestableTransaction
    {
        private final static class Transaction extends Transactional.AbstractTransactional implements Transactional
        {
            final ColumnFamilyStore cfs;
            final TransactionLogs txnLogs;
            final SSTableReader sstableOld;
            final SSTableReader sstableNew;
            final TransactionLogs.SSTableTidier tidier;

            public Transaction(ColumnFamilyStore cfs, TransactionLogs txnLogs) throws IOException
            {
                this.cfs = cfs;
                this.txnLogs = txnLogs;
                this.sstableOld = sstable(cfs, 0, 128);
                this.sstableNew = sstable(cfs, 1, 128);

                assertNotNull(txnLogs);
                assertNotNull(txnLogs.getId());
                Assert.assertEquals(OperationType.COMPACTION, txnLogs.getType());

                txnLogs.trackNew(sstableNew);
                tidier = txnLogs.obsoleted(sstableOld);
                assertNotNull(tidier);
            }

            protected Throwable doCommit(Throwable accumulate)
            {
                sstableOld.markObsolete(tidier);
                sstableOld.selfRef().release();
                TransactionLogs.waitForDeletions();

                Throwable ret = txnLogs.commit(accumulate);

                sstableNew.selfRef().release();
                return ret;
            }

            protected Throwable doAbort(Throwable accumulate)
            {
                tidier.abort();
                TransactionLogs.waitForDeletions();

                Throwable ret = txnLogs.abort(accumulate);

                sstableNew.selfRef().release();
                sstableOld.selfRef().release();
                return ret;
            }

            protected void doPrepare()
            {
                txnLogs.prepareToCommit();
            }

            protected void assertInProgress() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), Sets.newHashSet(Iterables.concat(sstableNew.getAllFilePaths(),
                                                                                      sstableOld.getAllFilePaths())));
                assertFiles(txnLogs.getLogsFolder(), Sets.newHashSet(txnLogs.getData().oldLog().file.getPath(),
                                                                     txnLogs.getData().newLog().file.getPath()));
                assertEquals(2, TransactionLogs.getLogFiles(cfs.metadata).size());
            }

            protected void assertPrepared() throws Exception
            {
            }

            protected void assertAborted() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
                assertFiles(txnLogs.getLogsFolder(), Collections.<String>emptySet());
                assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());
            }

            protected void assertCommitted() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
                assertFiles(txnLogs.getLogsFolder(), Collections.<String>emptySet());
                assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());
            }
        }

        final Transaction txn;

        private TxnTest() throws IOException
        {
            this(MockSchema.newCFS(KEYSPACE));
        }

        private TxnTest(ColumnFamilyStore cfs) throws IOException
        {
            this(cfs, new TransactionLogs(OperationType.COMPACTION, cfs.metadata));
        }

        private TxnTest(ColumnFamilyStore cfs, TransactionLogs txnLogs) throws IOException
        {
            this(new Transaction(cfs, txnLogs));
        }

        private TxnTest(Transaction txn)
        {
            super(txn);
            this.txn = txn;
        }

        protected void assertInProgress() throws Exception
        {
            txn.assertInProgress();
        }

        protected void assertPrepared() throws Exception
        {
            txn.assertPrepared();
        }

        protected void assertAborted() throws Exception
        {
            txn.assertAborted();
        }

        protected void assertCommitted() throws Exception
        {
            txn.assertCommitted();
        }
    }

    @Test
    public void testUntrack() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // complete a transaction without keep the new files since they were untracked
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstableNew);
        transactionLogs.untrackNew(sstableNew);

        transactionLogs.finish();

        assertFiles(transactionLogs.getDataFolder(), Collections.<String>emptySet());
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitSameDesc() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld1 = sstable(cfs, 0, 128);
        SSTableReader sstableOld2 = sstable(cfs, 0, 256);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstableNew);

        sstableOld1.setReplaced();

        TransactionLogs.SSTableTidier tidier = transactionLogs.obsoleted(sstableOld2);
        assertNotNull(tidier);

        transactionLogs.finish();

        sstableOld2.markObsolete(tidier);

        sstableOld1.selfRef().release();
        sstableOld2.selfRef().release();

        TransactionLogs.waitForDeletions();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstable);
        transactionLogs.finish();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstable.selfRef().release();
    }

    @Test
    public void testCommitOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        TransactionLogs.SSTableTidier tidier = transactionLogs.obsoleted(sstable);
        assertNotNull(tidier);

        transactionLogs.finish();
        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        TransactionLogs.waitForDeletions();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>());
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());
    }

    @Test
    public void testAbortOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstable);
        transactionLogs.abort();

        sstable.selfRef().release();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>());
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());
    }

    @Test
    public void testAbortOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        TransactionLogs.SSTableTidier tidier = transactionLogs.obsoleted(sstable);
        assertNotNull(tidier);

        tidier.abort();
        transactionLogs.abort();

        sstable.selfRef().release();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());
    }

    private File copyToTmpFile(File file) throws IOException
    {
        File ret = File.createTempFile(file.getName(), ".tmp");
        ret.deleteOnExit();
        Files.copy(file.toPath(), ret.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return ret;
    }

    @Test
    public void testRemoveUnfinishedLeftovers_newLogFound() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstableNew);
        TransactionLogs.SSTableTidier tidier = transactionLogs.obsoleted(sstableOld);

        File tmpNewLog = copyToTmpFile(transactionLogs.getData().newLog().file);
        File tmpOldLog = copyToTmpFile(transactionLogs.getData().oldLog().file);

        Set<File> tmpFiles = new HashSet<>(TransactionLogs.getLogFiles(cfs.metadata));
        for (String p : sstableNew.getAllFilePaths())
            tmpFiles.add(new File(p));

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, TransactionLogs.getTemporaryFiles(cfs.metadata, sstableNew.descriptor.directory));

        // normally called at startup
        TransactionLogs.removeUnfinishedLeftovers(cfs.metadata);

        // sstable should not have been removed because the new log was found
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());

        tidier.run();

        // copy old transaction files contents back or transactionlogs will throw assertions
        Files.move(tmpNewLog.toPath(), transactionLogs.getData().newLog().file.toPath());
        Files.move(tmpOldLog.toPath(), transactionLogs.getData().oldLog().file.toPath());

        transactionLogs.close();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_oldLogFound() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a committed transaction (new log file deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstableNew);
        TransactionLogs.SSTableTidier tidier = transactionLogs.obsoleted(sstableOld);

        File tmpNewLog = copyToTmpFile(transactionLogs.getData().newLog().file);
        File tmpOldLog = copyToTmpFile(transactionLogs.getData().oldLog().file);

        transactionLogs.getData().newLog().delete(false);

        Set<File> tmpFiles = new HashSet<>(TransactionLogs.getLogFiles(cfs.metadata));
        for (String p : sstableOld.getAllFilePaths())
            tmpFiles.add(new File(p));

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, TransactionLogs.getTemporaryFiles(cfs.metadata, sstableOld.descriptor.directory));

        // normally called at startup
        TransactionLogs.removeUnfinishedLeftovers(cfs.metadata);

        // sstable should have been removed because there was no new log.
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());

        tidier.run();

        // copy old transaction files contents back or transactionlogs will throw assertions
        Files.move(tmpNewLog.toPath(), transactionLogs.getData().newLog().file.toPath());
        Files.move(tmpOldLog.toPath(), transactionLogs.getData().oldLog().file.toPath());

        transactionLogs.close();
    }

    @Test
    public void testGetTemporaryFiles() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable1 = sstable(cfs, 0, 128);

        File dataFolder = sstable1.descriptor.directory;

        Set<File> tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.WRITE, cfs.metadata);
        Directories directories = new Directories(cfs.metadata);

        File[] beforeSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

        SSTableReader sstable2 = sstable(cfs, 1, 128);
        transactionLogs.trackNew(sstable2);

        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(2, sstables.size());

        File[] afterSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());
        int numNewFiles = afterSecondSSTable.length - beforeSecondSSTable.length;
        assertTrue(numNewFiles == sstable2.getAllFilePaths().size());

        tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(numNewFiles + 2, tmpFiles.size()); //the extra files are the transaction log files

        File ssTable2DataFile = new File(sstable2.descriptor.filenameFor(Component.DATA));
        File ssTable2IndexFile = new File(sstable2.descriptor.filenameFor(Component.PRIMARY_INDEX));

        assertTrue(tmpFiles.contains(ssTable2DataFile));
        assertTrue(tmpFiles.contains(ssTable2IndexFile));

        List<File> files = directories.sstableLister().listFiles();
        List<File> filesNoTmp = directories.sstableLister().skipTemporary(true).listFiles();
        assertNotNull(files);
        assertNotNull(filesNoTmp);

        assertTrue(files.contains(ssTable2DataFile));
        assertTrue(files.contains(ssTable2IndexFile));

        assertFalse(filesNoTmp.contains(ssTable2DataFile));
        assertFalse(filesNoTmp.contains(ssTable2IndexFile));

        transactionLogs.finish();

        //Now it should be empty since the transaction has finished
        tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        filesNoTmp = directories.sstableLister().skipTemporary(true).listFiles();
        assertNotNull(filesNoTmp);
        assertTrue(filesNoTmp.contains(ssTable2DataFile));
        assertTrue(filesNoTmp.contains(ssTable2IndexFile));

        sstable1.selfRef().release();
        sstable2.selfRef().release();
    }

    public static SSTableReader sstable(ColumnFamilyStore cfs, int generation, int size) throws IOException
    {
        Directories dir = new Directories(cfs.metadata);
        Descriptor descriptor = new Descriptor(dir.getDirectoryForNewSSTables(), cfs.keyspace.getName(), cfs.getColumnFamilyName(), generation);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            file.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
            {
                raf.setLength(size);
            }
        }

        SegmentedFile dFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.DATA))), 0);
        SegmentedFile iFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.PRIMARY_INDEX))), 0);

        SerializationHeader header = SerializationHeader.make(cfs.metadata, Collections.EMPTY_LIST);
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata.comparator)
                                                 .finalizeMetadata(Murmur3Partitioner.instance.getClass().getCanonicalName(), 0.01f, -1, header)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor,
                                                          components,
                                                          cfs.metadata,
                                                          Murmur3Partitioner.instance,
                                                          dFile,
                                                          iFile,
                                                          MockSchema.indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(),
                                                          1L,
                                                          metadata,
                                                          SSTableReader.OpenReason.NORMAL,
                                                          header);
        reader.first = reader.last = MockSchema.readerBounds(generation);
        return reader;
    }

    private static void assertFiles(String dirPath, Set<String> expectedFiles)
    {
        File dir = new File(dirPath);
        for (File file : dir.listFiles())
        {
            if (file.isDirectory())
                continue;

            String filePath = file.getPath();
            assertTrue(filePath, expectedFiles.contains(filePath));
            expectedFiles.remove(filePath);
        }

        assertTrue(expectedFiles.isEmpty());
    }
}
