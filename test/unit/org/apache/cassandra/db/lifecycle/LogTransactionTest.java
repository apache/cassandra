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
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedSegmentedFile;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.concurrent.AbstractTransactionalTest;
import org.apache.cassandra.utils.concurrent.Transactional;

public class LogTransactionTest extends AbstractTransactionalTest
{
    private static final String KEYSPACE = "TransactionLogsTest";

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    protected AbstractTransactionalTest.TestableTransaction newTest() throws Exception
    {
        LogTransaction.waitForDeletions();
        SSTableReader.resetTidying();
        return new TxnTest();
    }

    private static final class TxnTest extends TestableTransaction
    {
        private final static class Transaction extends Transactional.AbstractTransactional implements Transactional
        {
            final ColumnFamilyStore cfs;
            final LogTransaction txnLogs;
            final SSTableReader sstableOld;
            final SSTableReader sstableNew;
            final LogTransaction.SSTableTidier tidier;

            Transaction(ColumnFamilyStore cfs, LogTransaction txnLogs) throws IOException
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
                LogTransaction.waitForDeletions();

                Throwable ret = txnLogs.commit(accumulate);

                sstableNew.selfRef().release();
                return ret;
            }

            protected Throwable doAbort(Throwable accumulate)
            {
                tidier.abort();
                LogTransaction.waitForDeletions();

                Throwable ret = txnLogs.abort(accumulate);

                sstableNew.selfRef().release();
                sstableOld.selfRef().release();
                return ret;
            }

            protected void doPrepare()
            {
                txnLogs.prepareToCommit();
            }

            void assertInProgress() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), Sets.newHashSet(Iterables.concat(sstableNew.getAllFilePaths(),
                                                                                      sstableOld.getAllFilePaths(),
                                                                                      Collections.singleton(txnLogs.getLogFile().file.getPath()))));
            }

            void assertPrepared() throws Exception
            {
            }

            void assertAborted() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
            }

            void assertCommitted() throws Exception
            {
                assertFiles(txnLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
            }
        }

        final Transaction txn;

        private TxnTest() throws IOException
        {
            this(MockSchema.newCFS(KEYSPACE));
        }

        private TxnTest(ColumnFamilyStore cfs) throws IOException
        {
            this(cfs, new LogTransaction(OperationType.COMPACTION, cfs.metadata));
        }

        private TxnTest(ColumnFamilyStore cfs, LogTransaction txnLogs) throws IOException
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
        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        log.untrackNew(sstableNew);

        log.finish();

        sstableNew.selfRef().release();
        Thread.sleep(1);
        LogTransaction.waitForDeletions();

        assertFiles(log.getDataFolder(), Collections.<String>emptySet());
    }

    @Test
    public void testCommitSameDesc() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld1 = sstable(cfs, 0, 128);
        SSTableReader sstableOld2 = sstable(cfs, 0, 256);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);

        sstableOld1.setReplaced();

        LogTransaction.SSTableTidier tidier = log.obsoleted(sstableOld2);
        assertNotNull(tidier);

        log.finish();

        sstableOld2.markObsolete(tidier);

        sstableOld1.selfRef().release();
        sstableOld2.selfRef().release();

        assertFiles(log.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstable);
        log.finish();

        assertFiles(log.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));

        sstable.selfRef().release();
    }

    @Test
    public void testCommitOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        LogTransaction.SSTableTidier tidier = log.obsoleted(sstable);
        assertNotNull(tidier);

        log.finish();
        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        assertFiles(log.getDataFolder(), new HashSet<>());
    }

    @Test
    public void testAbortOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstable);
        log.abort();

        sstable.selfRef().release();

        assertFiles(log.getDataFolder(), new HashSet<>());
    }

    @Test
    public void testAbortOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        LogTransaction.SSTableTidier tidier = log.obsoleted(sstable);
        assertNotNull(tidier);

        tidier.abort();
        log.abort();

        sstable.selfRef().release();

        assertFiles(log.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
    }

    @Test
    public void testRemoveUnfinishedLeftovers_abort() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        LogTransaction.SSTableTidier tidier = log.obsoleted(sstableOld);

        Set<File> tmpFiles = sstableNew.getAllFilePaths().stream().map(File::new).collect(Collectors.toSet());

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, LogAwareFileLister.getTemporaryFiles(sstableNew.descriptor.directory));

        // normally called at startup
        LogTransaction.removeUnfinishedLeftovers(cfs.metadata);

        // sstableOld should be only table left
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(log.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));

        tidier.run();

        // complete the transaction before releasing files
        log.close();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_commit() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a committed transaction (new log file deleted)
        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        LogTransaction.SSTableTidier tidier = log.obsoleted(sstableOld);

        //Fake a commit
        log.getLogFile().commit();

        Set<File> tmpFiles = sstableOld.getAllFilePaths().stream().map(File::new).collect(Collectors.toSet());

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, LogAwareFileLister.getTemporaryFiles(sstableOld.descriptor.directory));

        // normally called at startup
        LogTransaction.removeUnfinishedLeftovers(cfs.metadata);

        // sstableNew should be only table left
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(log.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));

        tidier.run();

        // complete the transaction to avoid LEAK errors
        assertNull(log.complete(null));
    }

    @Test
    public void testGetTemporaryFiles() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable1 = sstable(cfs, 0, 128);

        File dataFolder = sstable1.descriptor.directory;

        Set<File> tmpFiles = LogAwareFileLister.getTemporaryFiles(dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        try(LogTransaction log = new LogTransaction(OperationType.WRITE, cfs.metadata))
        {
            Directories directories = new Directories(cfs.metadata);

            File[] beforeSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

            SSTableReader sstable2 = sstable(cfs, 1, 128);
            log.trackNew(sstable2);

            Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
            assertEquals(2, sstables.size());

            // this should contain sstable1, sstable2 and the transaction log file
            File[] afterSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

            int numNewFiles = afterSecondSSTable.length - beforeSecondSSTable.length;
            assertEquals(numNewFiles - 1, sstable2.getAllFilePaths().size()); // new files except for transaction log file

            tmpFiles = LogAwareFileLister.getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(numNewFiles - 1, tmpFiles.size());

            File ssTable2DataFile = new File(sstable2.descriptor.filenameFor(Component.DATA));
            File ssTable2IndexFile = new File(sstable2.descriptor.filenameFor(Component.PRIMARY_INDEX));

            assertTrue(tmpFiles.contains(ssTable2DataFile));
            assertTrue(tmpFiles.contains(ssTable2IndexFile));

            List<File> files = directories.sstableLister(Directories.OnTxnErr.THROW).listFiles();
            List<File> filesNoTmp = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true).listFiles();
            assertNotNull(files);
            assertNotNull(filesNoTmp);

            assertTrue(files.contains(ssTable2DataFile));
            assertTrue(files.contains(ssTable2IndexFile));

            assertFalse(filesNoTmp.contains(ssTable2DataFile));
            assertFalse(filesNoTmp.contains(ssTable2IndexFile));

            log.finish();

            //Now it should be empty since the transaction has finished
            tmpFiles = LogAwareFileLister.getTemporaryFiles(dataFolder);
            assertNotNull(tmpFiles);
            assertEquals(0, tmpFiles.size());

            filesNoTmp = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true).listFiles();
            assertNotNull(filesNoTmp);
            assertTrue(filesNoTmp.contains(ssTable2DataFile));
            assertTrue(filesNoTmp.contains(ssTable2IndexFile));

            sstable1.selfRef().release();
            sstable2.selfRef().release();
        }
    }

    @Test
    public void testWrongChecksumLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum
                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d,0,0][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));
                          },
                          true);
    }

    @Test
    public void testWrongChecksumSecondFromLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake two lines with invalid checksum
                              FileUtils.append(t.getLogFile().file,
                                               String.format("add:[ma-3-big,%d,4][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));

                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d,0,0][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));
                          },
                          false);
    }

    @Test
    public void testWrongChecksumLastLineMissingFile() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum and also delete one of the old files
                              for (String filePath : s.getAllFilePaths())
                              {
                                  if (filePath.endsWith("Data.db"))
                                  {
                                      assertTrue(FileUtils.delete(filePath));
                                      t.getLogFile().sync();
                                      break;
                                  }
                              }

                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d,0,0][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));
                          },
                          false);
    }

    @Test
    public void testWrongChecksumLastLineWrongRecordFormat() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum and a wrong record format (extra spaces)
                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d ,0 ,0 ][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));
                          },
                          true);
    }

    @Test
    public void testMissingChecksumLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          {
                              // Fake a commit without a checksum
                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d,0,0]",
                                                             System.currentTimeMillis()));
                          },
                          true);
    }

    @Test
    public void testMissingChecksumSecondFromLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake two lines without a checksum
                              FileUtils.append(t.getLogFile().file,
                                               String.format("add:[ma-3-big,%d,4]",
                                                             System.currentTimeMillis()));

                              FileUtils.append(t.getLogFile().file,
                                               String.format("commit:[%d,0,0]",
                                                             System.currentTimeMillis()));
                          },
                          false);
    }

    private static void testCorruptRecord(BiConsumer<LogTransaction, SSTableReader> modifier, boolean isRecoverable) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        File dataFolder = sstableOld.descriptor.directory;

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        log.obsoleted(sstableOld);

        // Modify the transaction log or disk state for sstableOld
        modifier.accept(log, sstableOld);

        String txnFilePath = log.getLogFile().file.getPath();

        assertNull(log.complete(null));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        // The files on disk, for old files make sure to exclude the files that were deleted by the modifier
        Set<String> newFiles = sstableNew.getAllFilePaths().stream().collect(Collectors.toSet());
        Set<String> oldFiles = sstableOld.getAllFilePaths().stream().filter(p -> new File(p).exists()).collect(Collectors.toSet());

        //This should filter as in progress since the last record is corrupt
        assertFiles(newFiles, LogAwareFileLister.getTemporaryFiles(dataFolder));
        assertFiles(oldFiles, LogAwareFileLister.getFinalFiles(dataFolder));

        if (isRecoverable)
        { // the corruption is recoverable but the commit record is unreadable so the transaction is still in progress

            //This should remove new files
            LogTransaction.removeUnfinishedLeftovers(cfs.metadata);

            // make sure to exclude the old files that were deleted by the modifier
            assertFiles(dataFolder.getPath(), oldFiles);
        }
        else
        { // if an intermediate line was also modified, it should ignore the tx log file

            //This should not remove any files
            LogTransaction.removeUnfinishedLeftovers(cfs.metadata);

            assertFiles(dataFolder.getPath(), Sets.newHashSet(Iterables.concat(newFiles,
                                                                               oldFiles,
                                                                               Collections.singleton(txnFilePath))));
        }
    }

    @Test
    public void testObsoletedDataFileUpdateTimeChanged() throws IOException
    {
        testObsoletedFilesChanged(sstable ->
                                  {
                                      // increase the modification time of the Data file
                                      for (String filePath : sstable.getAllFilePaths())
                                      {
                                          if (filePath.endsWith("Data.db"))
                                              assertTrue(new File(filePath).setLastModified(System.currentTimeMillis() + 60000)); //one minute later
                                      }
                                  });
    }

    private static void testObsoletedFilesChanged(Consumer<SSTableReader> modifier) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        LogTransaction log = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(log);

        log.trackNew(sstableNew);
        /*TransactionLog.SSTableTidier tidier =*/ log.obsoleted(sstableOld);

        //modify the old sstable files
        modifier.accept(sstableOld);

        //Fake a commit
        log.getLogFile().commit();

        //This should not remove the old files
        LogTransaction.removeUnfinishedLeftovers(cfs.metadata);

        assertFiles(log.getDataFolder(), Sets.newHashSet(Iterables.concat(
                                                                                    sstableNew.getAllFilePaths(),
                                                                                    sstableOld.getAllFilePaths(),
                                                                                    Collections.singleton(log.getLogFile().file.getPath()))));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        // complete the transaction to avoid LEAK errors
        assertNull(log.complete(null));

        assertFiles(log.getDataFolder(), Sets.newHashSet(Iterables.concat(
                                                                                    sstableNew.getAllFilePaths(),
                                                                                    sstableOld.getAllFilePaths(),
                                                                                    Collections.singleton(log.getLogFile().file.getPath()))));
    }

    @Test
    public void testGetTemporaryFilesSafeAfterObsoletion() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);
        File dataFolder = sstable.descriptor.directory;

        LogTransaction logs = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(logs);

        LogTransaction.SSTableTidier tidier = logs.obsoleted(sstable);

        logs.finish();

        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        // This should race with the asynchronous deletion of txn log files
        // It doesn't matter what it returns but it should not throw because the txn
        // was completed before deleting files (i.e. releasing sstables)
        for (int i = 0; i < 200; i++)
            LogAwareFileLister.getTemporaryFiles(dataFolder);
    }

    @Test
    public void testGetTemporaryFilesThrowsIfCompletingAfterObsoletion() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);
        File dataFolder = sstable.descriptor.directory;

        LogTransaction logs = new LogTransaction(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(logs);

        LogTransaction.SSTableTidier tidier = logs.obsoleted(sstable);

        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        LogTransaction.waitForDeletions();

        try
        {
            // This should race with the asynchronous deletion of txn log files
            // it should throw because we are violating the requirement that a transaction must
            // finish before deleting files (i.e. releasing sstables)
            LogAwareFileLister.getTemporaryFiles(dataFolder);
            fail("Expected runtime exception");
        }
        catch(RuntimeException e)
        {
            //pass
        }

        logs.finish();
    }

    private static SSTableReader sstable(ColumnFamilyStore cfs, int generation, int size) throws IOException
    {
        Directories dir = new Directories(cfs.metadata);
        Descriptor descriptor = new Descriptor(dir.getDirectoryForNewSSTables(), cfs.keyspace.getName(), cfs.getTableName(), generation);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            if (!file.exists())
                assertTrue(file.createNewFile());
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
            {
                raf.setLength(size);
            }
        }

        SegmentedFile dFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.DATA))), RandomAccessReader.DEFAULT_BUFFER_SIZE, 0);
        SegmentedFile iFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.PRIMARY_INDEX))), RandomAccessReader.DEFAULT_BUFFER_SIZE, 0);

        SerializationHeader header = SerializationHeader.make(cfs.metadata, Collections.emptyList());
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata.comparator)
                                                 .finalizeMetadata(cfs.metadata.partitioner.getClass().getCanonicalName(), 0.01f, -1, header)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor,
                                                          components,
                                                          cfs.metadata,
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
        assertFiles(dirPath, expectedFiles, false);
    }

    private static void assertFiles(String dirPath, Set<String> expectedFiles, boolean excludeNonExistingFiles)
    {
        LogTransaction.waitForDeletions();

        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        if (files != null)
        {
            for (File file : files)
            {
                if (file.isDirectory())
                    continue;

                String filePath = file.getPath();
                assertTrue(filePath, expectedFiles.contains(filePath));
                expectedFiles.remove(filePath);
            }
        }

        if (excludeNonExistingFiles)
        {
            for (String filePath : expectedFiles)
            {
                File file = new File(filePath);
                if (!file.exists())
                    expectedFiles.remove(filePath);
            }
        }

        assertTrue(expectedFiles.toString(), expectedFiles.isEmpty());
    }

    // Check either that a temporary file is expected to exist (in the existingFiles) or that
    // it does not exist any longer (on Windows we need to check File.exists() because a list
    // might return a file as existing even if it does not)
    private static void assertFiles(Iterable<String> existingFiles, Set<File> temporaryFiles)
    {
        for (String filePath : existingFiles)
        {
            File file = new File(filePath);
            assertTrue(filePath, temporaryFiles.contains(file));
            temporaryFiles.remove(file);
        }

        for (File file : temporaryFiles)
        {
            if (!file.exists())
                temporaryFiles.remove(file);
        }

        assertTrue(temporaryFiles.toString(), temporaryFiles.isEmpty());
    }
}
