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

package org.apache.cassandra.db.commitlog;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CDCState;
import org.apache.cassandra.exceptions.CDCWriteException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;

public class CommitLogSegmentManagerCDCTest extends CQLTester
{
    private static final Random random = new Random();

    @BeforeClass
    public static void setUpClass()
    {
        ServerTestUtils.daemonInitialization();

        DatabaseDescriptor.setCDCEnabled(true);
        DatabaseDescriptor.setCDCTotalSpaceInMiB(1024);
        CQLTester.setUpClass();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        super.beforeTest();
        // Need to clean out any files from previous test runs. Prevents flaky test failures.
        CommitLog.instance.stopUnsafe(true);
        CommitLog.instance.start();
        ((CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager).updateCDCTotalSize();
    }

    @Test
    public void testCDCWriteFailure() throws Throwable
    {
        testWithCDCSpaceInMb(32, () -> {
            createTableAndBulkWrite();
            expectCurrentCDCState(CDCState.FORBIDDEN);

            // Confirm we can create a non-cdc table and write to it even while at cdc capacity
            createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
            execute("INSERT INTO %s (idx, data) VALUES (1, '1');");

            // Confirm that, on flush+recyle, we see files show up in cdc_raw
            CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
            Keyspace.open(keyspace())
                    .getColumnFamilyStore(currentTable())
                    .forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            CommitLog.instance.forceRecycleAllSegments();
            cdcMgr.awaitManagementTasksCompletion();
            Assert.assertTrue("Expected files to be moved to overflow.", getCDCRawCount() > 0);

            // Simulate a CDC consumer reading files then deleting them
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList())
                FileUtils.deleteWithConfirm(f);

            // Update size tracker to reflect deleted files. Should flip flag on current allocatingFrom to allow.
            cdcMgr.updateCDCTotalSize();
            expectCurrentCDCState(CDCState.PERMITTED);
        });
    }

    @Test
    public void testSegmentFlaggingOnCreation() throws Throwable
    {
        testSegmentFlaggingOnCreation0();
    }

    @Test
    public void testSegmentFlaggingWithNonblockingOnCreation() throws Throwable
    {
        testWithNonblockingMode(this::testSegmentFlaggingOnCreation0);
    }

    @Test
    public void testNonblockingShouldMaintainSteadyDiskUsage() throws Throwable
    {
        final int commitlogSize = DatabaseDescriptor.getCommitLogSegmentSize() / 1024 / 1024;
        final int targetFilesCount = 3;
        final long cdcSizeLimit = commitlogSize * targetFilesCount;
        final int mutationSize = DatabaseDescriptor.getCommitLogSegmentSize() / 3;
        testWithNonblockingMode(() -> testWithCDCSpaceInMb((int) cdcSizeLimit, () -> {
            CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;

            createTableAndBulkWrite(mutationSize);

            long actualSize = cdcMgr.updateCDCTotalSize();
            long cdcSizeLimitBytes = cdcSizeLimit * 1024 * 1024;
            Assert.assertTrue(String.format("Actual size (%s) should not exceed the size limit (%s)", actualSize, cdcSizeLimitBytes),
                              actualSize <= cdcSizeLimitBytes * targetFilesCount);
            Assert.assertTrue(String.format("Actual size (%s) should be at least the mutation size (%s)", actualSize, mutationSize),
                              actualSize >= mutationSize);
        }));
    }

    @Test // switch from blocking to nonblocking, then back to blocking
    public void testSwitchingCDCWriteModes() throws Throwable
    {
        String tableName = createTableAndBulkWrite();
        expectCurrentCDCState(CDCState.FORBIDDEN);
        testWithNonblockingMode(() -> {
            bulkWrite(tableName);
            expectCurrentCDCState(CDCState.CONTAINS);
        });
        bulkWrite(tableName);
        expectCurrentCDCState(CDCState.FORBIDDEN);
    }

    @Test
    public void testCDCIndexFileWriteOnSync() throws IOException
    {
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        new RowUpdateBuilder(currentTableMetadata(), 0, 1)
            .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
            .build().apply();

        CommitLog.instance.sync(true);
        CommitLogSegment currentSegment = CommitLog.instance.segmentManager.allocatingFrom();
        int syncOffset = currentSegment.lastSyncedOffset;

        // Confirm index file is written
        File cdcIndexFile = currentSegment.getCDCIndexFile();
        Assert.assertTrue("Index file not written: " + cdcIndexFile, cdcIndexFile.exists());

        // Read index value and confirm it's == end from last sync
        String input = null;
        // There could be a race between index file update (truncate & write) and read. See CASSANDRA-17416
        // It is possible to read an empty line. In this case, re-try at most 5 times.
        for (int i = 0; input == null && i < 5; i++)
        {
            if (i != 0) // add a little pause between each attempt
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

            try (BufferedReader in = new BufferedReader(new FileReader(cdcIndexFile)))
            {
                input = in.readLine();
            }
        }

        if (input == null)
        {
            Assert.fail("Unable to read the CDC index file after several attempts");
        }

        int indexOffset = Integer.parseInt(input);
        Assert.assertTrue("The offset read from CDC index file should be equal or larger than the offset after sync. See CASSANDRA-17416",
                          syncOffset <= indexOffset);
    }

    @Test
    public void testCompletedFlag() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegment initialSegment = CommitLog.instance.segmentManager.allocatingFrom();

        testWithCDCSpaceInMb(8, () -> bulkWrite(tableName));

        CommitLog.instance.forceRecycleAllSegments();

        // Confirm index file is written
        File cdcIndexFile = initialSegment.getCDCIndexFile();
        Assert.assertTrue("Index file not written: " + cdcIndexFile, cdcIndexFile.exists());

        // Read index file and confirm second line is COMPLETED
        BufferedReader in = new BufferedReader(new FileReader(cdcIndexFile));
        String input = in.readLine();
        input = in.readLine();
        Assert.assertTrue("Expected COMPLETED in index file, got: " + input, input.equals("COMPLETED"));
        in.close();
    }

    @Test
    public void testDeleteLinkOnDiscardNoCDC() throws Throwable
    {
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
        new RowUpdateBuilder(currentTableMetadata(), 0, 1)
            .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
            .build().apply();
        CommitLogSegment currentSegment = CommitLog.instance.segmentManager.allocatingFrom();

        // Confirm that, with no CDC data present, we've hard-linked but have no index file
        Path linked = new File(DatabaseDescriptor.getCDCLogLocation(), currentSegment.logFile.name()).toPath();
        File cdcIndexFile = currentSegment.getCDCIndexFile();
        Assert.assertTrue("File does not exist: " + linked, Files.exists(linked));
        Assert.assertFalse("Expected index file to not be created but found: " + cdcIndexFile, cdcIndexFile.exists());

        // Sync and confirm no index written as index is written on flush
        CommitLog.instance.sync(true);
        Assert.assertTrue("File does not exist: " + linked, Files.exists(linked));
        Assert.assertFalse("Expected index file to not be created but found: " + cdcIndexFile, cdcIndexFile.exists());

        // Force a full recycle and confirm hard-link is deleted
        CommitLog.instance.forceRecycleAllSegments();
        CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
        Assert.assertFalse("Expected hard link to CLS to be deleted on non-cdc segment: " + linked, Files.exists(linked));
    }

    @Test
    public void testRetainLinkOnDiscardCDC() throws Throwable
    {
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegment currentSegment = CommitLog.instance.segmentManager.allocatingFrom();
        File cdcIndexFile = currentSegment.getCDCIndexFile();
        Assert.assertFalse("Expected no index file before flush but found: " + cdcIndexFile, cdcIndexFile.exists());

        new RowUpdateBuilder(currentTableMetadata(), 0, 1)
            .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
            .build().apply();

        Path linked = new File(DatabaseDescriptor.getCDCLogLocation(), currentSegment.logFile.name()).toPath();
        // Confirm that, with CDC data present but not yet flushed, we've hard-linked but have no index file
        Assert.assertTrue("File does not exist: " + linked, Files.exists(linked));

        // Sync and confirm index written as index is written on flush
        CommitLog.instance.sync(true);
        Assert.assertTrue("File does not exist: " + linked, Files.exists(linked));
        Assert.assertTrue("Expected cdc index file after flush but found none: " + cdcIndexFile, cdcIndexFile.exists());

        // Force a full recycle and confirm all files remain
        CommitLog.instance.forceRecycleAllSegments();
        Assert.assertTrue("File does not exist: " + linked, Files.exists(linked));
        Assert.assertTrue("Expected cdc index file after recycle but found none: " + cdcIndexFile, cdcIndexFile.exists());
    }

    @Test
    public void testReplayLogic() throws Throwable
    {
        // Assert.assertEquals(0, new File(DatabaseDescriptor.getCDCLogLocation()).tryList().length);
        testWithCDCSpaceInMb(8, this::createTableAndBulkWrite);

        CommitLog.instance.sync(true);
        CommitLog.instance.stopUnsafe(false);

        // Build up a list of expected index files after replay and then clear out cdc_raw
        List<CDCIndexData> oldData = parseCDCIndexData();
        for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList())
            FileUtils.deleteWithConfirm(f.absolutePath());

        try
        {
            Assert.assertEquals("Expected 0 files in CDC folder after deletion. ",
                                0, new File(DatabaseDescriptor.getCDCLogLocation()).tryList().length);
        }
        finally
        {
            // If we don't have a started commitlog, assertions will cause the test to hang. I assume it's some assumption
            // hang in the shutdown on CQLTester trying to clean up / drop keyspaces / tables and hanging applying
            // mutations.
            CommitLog.instance.start();
            CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
        }
        CDCTestReplayer replayer = new CDCTestReplayer();
        replayer.examineCommitLog();

        // Rough sanity check -> should be files there now.
        Assert.assertTrue("Expected non-zero number of files in CDC folder after restart.",
                          new File(DatabaseDescriptor.getCDCLogLocation()).tryList().length > 0);

        // Confirm all the old indexes in old are present and >= the original offset, as we flag the entire segment
        // as cdc written on a replay.
        List<CDCIndexData> newData = parseCDCIndexData();
        for (CDCIndexData cid : oldData)
        {
            boolean found = false;
            for (CDCIndexData ncid : newData)
            {
                if (cid.fileName.equals(ncid.fileName))
                {
                    Assert.assertTrue("New CDC index file expected to have >= offset in old.", ncid.offset >= cid.offset);
                    found = true;
                }
            }
            if (!found)
            {
                StringBuilder errorMessage = new StringBuilder();
                errorMessage.append(String.format("Missing old CDCIndexData in new set after replay: %s\n", cid));
                errorMessage.append("List of CDCIndexData in new set of indexes after replay:\n");
                for (CDCIndexData ncid : newData)
                    errorMessage.append(String.format("   %s\n", ncid));
                Assert.fail(errorMessage.toString());
            }
        }

        // And make sure we don't have new CDC Indexes we don't expect
        for (CDCIndexData ncid : newData)
        {
            boolean found = false;
            for (CDCIndexData cid : oldData)
            {
                if (cid.fileName.equals(ncid.fileName))
                    found = true;
            }
            if (!found)
                Assert.fail(String.format("Unexpected new CDCIndexData found after replay: %s\n", ncid));
        }
    }

    private List<CDCIndexData> parseCDCIndexData()
    {
        List<CDCIndexData> results = new ArrayList<>();
        try
        {
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList())
            {
                if (f.name().contains("_cdc.idx"))
                    results.add(new CDCIndexData(f));
            }
        }
        catch (IOException e)
        {
            Assert.fail(String.format("Failed to parse CDCIndexData: %s", e.getMessage()));
        }
        return results;
    }

    private static class CDCIndexData
    {
        private final String fileName;
        private final int offset;

        CDCIndexData(File f) throws IOException
        {
            String line = "";
            try (BufferedReader br = new BufferedReader(new FileReader(f)))
            {
                line = br.readLine();
            }
            catch (Exception e)
            {
                throw e;
            }
            fileName = f.name();
            offset = Integer.parseInt(line);
        }

        @Override
        public String toString()
        {
            return String.format("%s,%d", fileName, offset);
        }

        @Override
        public boolean equals(Object other)
        {
            CDCIndexData cid = (CDCIndexData)other;
            return fileName.equals(cid.fileName) && offset == cid.offset;
        }
    }

    private ByteBuffer randomizeBuffer(int size)
    {
        byte[] toWrap = new byte[size];
        random.nextBytes(toWrap);
        return ByteBuffer.wrap(toWrap);
    }

    private int getCDCRawCount()
    {
        return new File(DatabaseDescriptor.getCDCLogLocation()).tryList().length;
    }

    private void expectCurrentCDCState(CDCState expectedState)
    {
        CDCState currentState = CommitLog.instance.segmentManager.allocatingFrom().getCDCState();
        if (currentState != expectedState)
        {
            logger.error("expectCurrentCDCState violation! Expected state: {}. Found state: {}. Current CDC allocation: {}",
                         expectedState, currentState, ((CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager).updateCDCTotalSize());
            Assert.fail(String.format("Received unexpected CDCState on current allocatingFrom segment. Expected: %s. Received: %s",
                        expectedState, currentState));
        }
    }

    private void testWithNonblockingMode(Testable test) throws Throwable
    {
        boolean original = DatabaseDescriptor.getCDCBlockWrites();
        CommitLog.instance.setCDCBlockWrites(false);
        try
        {
            test.run();
        }
        finally
        {
            CommitLog.instance.setCDCBlockWrites(original);
        }
    }

    private void testWithCDCSpaceInMb(int size, Testable test) throws Throwable
    {
        int origSize = (int) DatabaseDescriptor.getCDCTotalSpace() / 1024 / 1024;
        DatabaseDescriptor.setCDCTotalSpaceInMiB(size);
        try
        {
            test.run();
        }
        finally
        {
            DatabaseDescriptor.setCDCTotalSpaceInMiB(origSize);
        }
    }

    private String createTableAndBulkWrite() throws Throwable
    {
        return createTableAndBulkWrite(DatabaseDescriptor.getCommitLogSegmentSize() / 3);
    }

    private String createTableAndBulkWrite(int mutationSize) throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        bulkWrite(tableName, mutationSize);
        return tableName;
    }

    private void bulkWrite(String tableName) throws Throwable
    {
        bulkWrite(tableName, DatabaseDescriptor.getCommitLogSegmentSize() / 3);
    }

    private void bulkWrite(String tableName, int mutationSize) throws Throwable
    {
        TableMetadata ccfm = Keyspace.open(keyspace()).getColumnFamilyStore(tableName).metadata();
        boolean blockWrites = DatabaseDescriptor.getCDCBlockWrites();
        // Spin to make sure we hit CDC capacity
        try
        {
            for (int i = 0; i < 1000; i++)
            {
                new RowUpdateBuilder(ccfm, 0, i)
                .add("data", randomizeBuffer(mutationSize))
                .build().applyFuture().get();
            }
            if (blockWrites)
                Assert.fail("Expected CDCWriteException from full CDC but did not receive it.");
        }
        catch (CDCWriteException e)
        {
            if (!blockWrites)
                Assert.fail("Excepted no CDCWriteException when not blocking writes but received it.");
        }
    }

    private void testSegmentFlaggingOnCreation0() throws Throwable
    {
        testWithCDCSpaceInMb(16, () -> {
            boolean blockWrites = DatabaseDescriptor.getCDCBlockWrites();

            createTableAndBulkWrite();

            CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
            expectCurrentCDCState(blockWrites? CDCState.FORBIDDEN : CDCState.CONTAINS);

            // When block writes, releasing CDC commit logs should update the CDC state to PERMITTED
            if (blockWrites)
            {
                CommitLog.instance.forceRecycleAllSegments();

                cdcMgr.awaitManagementTasksCompletion();
                // Delete all files in cdc_raw
                for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList())
                    f.delete();
                cdcMgr.updateCDCTotalSize();
                // Confirm cdc update process changes flag on active segment
                expectCurrentCDCState(CDCState.PERMITTED);
            }

            // Clear out archived CDC files
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).tryList()) {
                FileUtils.deleteWithConfirm(f);
            }
        });
    }

    private interface Testable
    {
        void run() throws Throwable;
    }
}
