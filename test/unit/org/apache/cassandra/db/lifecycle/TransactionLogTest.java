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

public class TransactionLogTest extends AbstractTransactionalTest
{
    private static final String KEYSPACE = "TransactionLogsTest";

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    protected AbstractTransactionalTest.TestableTransaction newTest() throws Exception
    {
        TransactionLog.waitForDeletions();
        SSTableReader.resetTidying();
        return new TxnTest();
    }

    private static final class TxnTest extends TestableTransaction
    {
        private final static class Transaction extends Transactional.AbstractTransactional implements Transactional
        {
            final ColumnFamilyStore cfs;
            final TransactionLog txnLogs;
            final SSTableReader sstableOld;
            final SSTableReader sstableNew;
            final TransactionLog.SSTableTidier tidier;

            Transaction(ColumnFamilyStore cfs, TransactionLog txnLogs) throws IOException
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
                TransactionLog.waitForDeletions();

                Throwable ret = txnLogs.commit(accumulate);

                sstableNew.selfRef().release();
                return ret;
            }

            protected Throwable doAbort(Throwable accumulate)
            {
                tidier.abort();
                TransactionLog.waitForDeletions();

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
                                                                                      Collections.singleton(txnLogs.getData().getLogFile().file.getPath()))));
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
            this(cfs, new TransactionLog(OperationType.COMPACTION, cfs.metadata));
        }

        private TxnTest(ColumnFamilyStore cfs, TransactionLog txnLogs) throws IOException
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
        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);
        transactionLog.untrackNew(sstableNew);

        transactionLog.finish();

        sstableNew.selfRef().release();
        Thread.sleep(1);
        TransactionLog.waitForDeletions();

        assertFiles(transactionLog.getDataFolder(), Collections.<String>emptySet());
    }

    @Test
    public void testCommitSameDesc() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld1 = sstable(cfs, 0, 128);
        SSTableReader sstableOld2 = sstable(cfs, 0, 256);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);

        sstableOld1.setReplaced();

        TransactionLog.SSTableTidier tidier = transactionLog.obsoleted(sstableOld2);
        assertNotNull(tidier);

        transactionLog.finish();

        sstableOld2.markObsolete(tidier);

        sstableOld1.selfRef().release();
        sstableOld2.selfRef().release();

        assertFiles(transactionLog.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstable);
        transactionLog.finish();

        assertFiles(transactionLog.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));

        sstable.selfRef().release();
    }

    @Test
    public void testCommitOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        TransactionLog.SSTableTidier tidier = transactionLog.obsoleted(sstable);
        assertNotNull(tidier);

        transactionLog.finish();
        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        assertFiles(transactionLog.getDataFolder(), new HashSet<>());
    }

    @Test
    public void testAbortOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstable);
        transactionLog.abort();

        sstable.selfRef().release();

        assertFiles(transactionLog.getDataFolder(), new HashSet<>());
    }

    @Test
    public void testAbortOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        TransactionLog.SSTableTidier tidier = transactionLog.obsoleted(sstable);
        assertNotNull(tidier);

        tidier.abort();
        transactionLog.abort();

        sstable.selfRef().release();

        assertFiles(transactionLog.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
    }

    @Test
    public void testRemoveUnfinishedLeftovers_abort() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);
        TransactionLog.SSTableTidier tidier = transactionLog.obsoleted(sstableOld);

        Set<File> tmpFiles = Sets.newHashSet(Iterables.concat(sstableNew.getAllFilePaths().stream().map(File::new).collect(Collectors.toList()),
                                                              Collections.singleton(transactionLog.getData().getLogFile().file)));

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, TransactionLog.getTemporaryFiles(cfs.metadata, sstableNew.descriptor.directory));

        // normally called at startup
        TransactionLog.removeUnfinishedLeftovers(cfs.metadata);

        // sstableOld should be only table left
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLog.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));

        tidier.run();

        // complete the transaction to avoid LEAK errors
        transactionLog.close();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_commit() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a committed transaction (new log file deleted)
        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);
        TransactionLog.SSTableTidier tidier = transactionLog.obsoleted(sstableOld);

        //Fake a commit
        transactionLog.getData().getLogFile().commit();

        Set<File> tmpFiles = Sets.newHashSet(Iterables.concat(sstableOld.getAllFilePaths().stream().map(p -> new File(p)).collect(Collectors.toList()),
                                                              Collections.singleton(transactionLog.getData().getLogFile().file)));

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();

        Assert.assertEquals(tmpFiles, TransactionLog.getTemporaryFiles(cfs.metadata, sstableOld.descriptor.directory));

        // normally called at startup
        TransactionLog.removeUnfinishedLeftovers(cfs.metadata);

        // sstableNew should be only table left
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLog.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));

        tidier.run();

        // complete the transaction to avoid LEAK errors
        assertNull(transactionLog.complete(null));
    }

    @Test
    public void testGetTemporaryFiles() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable1 = sstable(cfs, 0, 128);

        File dataFolder = sstable1.descriptor.directory;

        Set<File> tmpFiles = TransactionLog.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        TransactionLog transactionLog = new TransactionLog(OperationType.WRITE, cfs.metadata);
        Directories directories = new Directories(cfs.metadata);

        File[] beforeSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

        SSTableReader sstable2 = sstable(cfs, 1, 128);
        transactionLog.trackNew(sstable2);

        Map<Descriptor, Set<Component>> sstables = directories.sstableLister(Directories.OnTxnErr.THROW).list();
        assertEquals(2, sstables.size());

        // this should contain sstable1, sstable2 and the transaction log file
        File[] afterSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

        int numNewFiles = afterSecondSSTable.length - beforeSecondSSTable.length;
        assertEquals(numNewFiles - 1, sstable2.getAllFilePaths().size()); // new files except for transaction log file

        tmpFiles = TransactionLog.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(numNewFiles, tmpFiles.size());

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

        transactionLog.finish();

        //Now it should be empty since the transaction has finished
        tmpFiles = TransactionLog.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        filesNoTmp = directories.sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true).listFiles();
        assertNotNull(filesNoTmp);
        assertTrue(filesNoTmp.contains(ssTable2DataFile));
        assertTrue(filesNoTmp.contains(ssTable2IndexFile));

        sstable1.selfRef().release();
        sstable2.selfRef().release();
    }

    @Test
    public void testWrongChecksumLastLine() throws IOException
    {
        testCorruptRecord((t, s) ->
                          { // Fake a commit with invalid checksum
                              FileUtils.append(t.getData().getLogFile().file,
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
                              FileUtils.append(t.getData().getLogFile().file,
                                               String.format("add:[ma-3-big,%d,4][%d]",
                                                             System.currentTimeMillis(),
                                                             12345678L));

                              FileUtils.append(t.getData().getLogFile().file,
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
                                      FileUtils.delete(filePath);
                                      break;
                                  }
                              }

                              FileUtils.append(t.getData().getLogFile().file,
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
                              FileUtils.append(t.getData().getLogFile().file,
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
                              FileUtils.append(t.getData().getLogFile().file,
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
                              FileUtils.append(t.getData().getLogFile().file,
                                               String.format("add:[ma-3-big,%d,4]",
                                                             System.currentTimeMillis()));

                              FileUtils.append(t.getData().getLogFile().file,
                                               String.format("commit:[%d,0,0]",
                                                             System.currentTimeMillis()));
                          },
                          false);
    }

    private void testCorruptRecord(BiConsumer<TransactionLog, SSTableReader> modifier, boolean isRecoverable) throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        File dataFolder = sstableOld.descriptor.directory;

        // simulate tracking sstables with a committed transaction except the checksum will be wrong
        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);
        transactionLog.obsoleted(sstableOld);

        //Modify the transaction log in some way
        modifier.accept(transactionLog, sstableOld);

        String txnFilePath = transactionLog.getData().getLogFile().file.getPath();

        assertNull(transactionLog.complete(null));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        if (isRecoverable)
        { // the corruption is recoverable, we assume there is a commit record

            //This should return the old files and the tx log
            assertFiles(Iterables.concat(sstableOld.getAllFilePaths(), Collections.singleton(txnFilePath)),
                        TransactionLog.getTemporaryFiles(cfs.metadata, dataFolder));

            //This should remove old files
            TransactionLog.removeUnfinishedLeftovers(cfs.metadata);

            assertFiles(dataFolder.getPath(), Sets.newHashSet(sstableNew.getAllFilePaths()));
        }
        else
        { // if an intermediate line was modified, we cannot tell,
          // it should just throw and handle the exception with a log message

            //This should not return any files
            assertEquals(Collections.emptyList(), new TransactionLog.FileLister(dataFolder.toPath(),
                                                                                (file, type) -> type != Directories.FileType.FINAL,
                                                                                Directories.OnTxnErr.IGNORE).list());

            try
            {
                //This should throw a RuntimeException
                new TransactionLog.FileLister(dataFolder.toPath(),
                                              (file, type) -> type != Directories.FileType.FINAL,
                                              Directories.OnTxnErr.THROW).list();
                fail("Expected exception");
            }
            catch (RuntimeException ex)
            {
                // pass
                ex.printStackTrace();
            }

            //This should not remove any files
            TransactionLog.removeUnfinishedLeftovers(cfs.metadata);

            assertFiles(dataFolder.getPath(), Sets.newHashSet(Iterables.concat(sstableNew.getAllFilePaths(),
                                                                               sstableOld.getAllFilePaths(),
                                                                               Collections.singleton(txnFilePath))),
                        true);
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
        TransactionLog transactionLog = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLog);

        transactionLog.trackNew(sstableNew);
        /*TransactionLog.SSTableTidier tidier =*/ transactionLog.obsoleted(sstableOld);

        //modify the old sstable files
        modifier.accept(sstableOld);

        //Fake a commit
        transactionLog.getData().getLogFile().commit();

        //This should not remove the old files
        TransactionLog.removeUnfinishedLeftovers(cfs.metadata);

        assertFiles(transactionLog.getDataFolder(), Sets.newHashSet(Iterables.concat(
                                                                                    sstableNew.getAllFilePaths(),
                                                                                    sstableOld.getAllFilePaths(),
                                                                                    Collections.singleton(transactionLog.getData().getLogFile().file.getPath()))));

        sstableOld.selfRef().release();
        sstableNew.selfRef().release();

        // complete the transaction to avoid LEAK errors
        assertNull(transactionLog.complete(null));

        assertFiles(transactionLog.getDataFolder(), Sets.newHashSet(Iterables.concat(
                                                                                    sstableNew.getAllFilePaths(),
                                                                                    sstableOld.getAllFilePaths(),
                                                                                    Collections.singleton(transactionLog.getData().getLogFile().file.getPath()))));
    }

    @Test
    public void testGetTemporaryFilesSafeAfterObsoletion_1() throws Throwable
    {
        testGetTemporaryFilesSafeAfterObsoletion(true);
    }

    @Test
    public void testGetTemporaryFilesSafeAfterObsoletion_2() throws Throwable
    {
        testGetTemporaryFilesSafeAfterObsoletion(false);
    }

    private void testGetTemporaryFilesSafeAfterObsoletion(boolean finishBefore) throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);
        File dataFolder = sstable.descriptor.directory;

        TransactionLog transactionLogs = new TransactionLog(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        TransactionLog.SSTableTidier tidier = transactionLogs.obsoleted(sstable);

        if (finishBefore)
            transactionLogs.finish();

        sstable.markObsolete(tidier);
        sstable.selfRef().release();

        for (int i = 0; i < 100; i++)
        {
            // This should race with the asynchronous deletion of txn log files
            // It doesn't matter what it returns but it should not throw
            TransactionLog.getTemporaryFiles(cfs.metadata, dataFolder);
        }

        if (!finishBefore)
            transactionLogs.finish();
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
        TransactionLog.waitForDeletions();

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

    private static void assertFiles(Iterable<String> filePaths, Set<File> expectedFiles)
    {
        for (String filePath : filePaths)
        {
            File file = new File(filePath);
            assertTrue(filePath, expectedFiles.contains(file));
            expectedFiles.remove(file);
        }

        assertTrue(expectedFiles.isEmpty());
    }
}
