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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
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
    public static void checkConfig()
    {
        Assume.assumeTrue(DatabaseDescriptor.isCDCEnabled());
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
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
        TableMetadata cfm = currentTableMetadata();

        // Confirm that logic to check for whether or not we can allocate new CDC segments works
        Integer originalCDCSize = DatabaseDescriptor.getCDCSpaceInMB();
        try
        {
            DatabaseDescriptor.setCDCSpaceInMB(32);
            // Spin until we hit CDC capacity and make sure we get a CDCWriteException
            try
            {
                // Should trigger on anything < 20:1 compression ratio during compressed test
                for (int i = 0; i < 100; i++)
                {
                    new RowUpdateBuilder(cfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
                Assert.fail("Expected CDCWriteException from full CDC but did not receive it.");
            }
            catch (CDCWriteException e)
            {
                // expected, do nothing
            }
            expectCurrentCDCState(CDCState.FORBIDDEN);

            // Confirm we can create a non-cdc table and write to it even while at cdc capacity
            createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
            execute("INSERT INTO %s (idx, data) VALUES (1, '1');");

            // Confirm that, on flush+recyle, we see files show up in cdc_raw
            Keyspace.open(keyspace()).getColumnFamilyStore(currentTable()).forceBlockingFlush();
            CommitLog.instance.forceRecycleAllSegments();
            cdcMgr.awaitManagementTasksCompletion();
            Assert.assertTrue("Expected files to be moved to overflow.", getCDCRawCount() > 0);

            // Simulate a CDC consumer reading files then deleting them
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
                FileUtils.deleteWithConfirm(f);

            // Update size tracker to reflect deleted files. Should flip flag on current allocatingFrom to allow.
            cdcMgr.updateCDCTotalSize();
            expectCurrentCDCState(CDCState.PERMITTED);
        }
        finally
        {
            DatabaseDescriptor.setCDCSpaceInMB(originalCDCSize);
        }
    }

    @Test
    public void testSegmentFlaggingOnCreation() throws Throwable
    {
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
        String ct = createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");

        int origSize = DatabaseDescriptor.getCDCSpaceInMB();
        try
        {
            DatabaseDescriptor.setCDCSpaceInMB(16);
            TableMetadata ccfm = Keyspace.open(keyspace()).getColumnFamilyStore(ct).metadata();
            // Spin until we hit CDC capacity and make sure we get a CDCWriteException
            try
            {
                for (int i = 0; i < 1000; i++)
                {
                    new RowUpdateBuilder(ccfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
                Assert.fail("Expected CDCWriteException from full CDC but did not receive it.");
            }
            catch (CDCWriteException e) { }

            expectCurrentCDCState(CDCState.FORBIDDEN);
            CommitLog.instance.forceRecycleAllSegments();

            cdcMgr.awaitManagementTasksCompletion();
            // Delete all files in cdc_raw
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
                f.delete();
            cdcMgr.updateCDCTotalSize();
            // Confirm cdc update process changes flag on active segment
            expectCurrentCDCState(CDCState.PERMITTED);

            // Clear out archived CDC files
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles()) {
                FileUtils.deleteWithConfirm(f);
            }
        }
        finally
        {
            DatabaseDescriptor.setCDCSpaceInMB(origSize);
        }
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
        BufferedReader in = new BufferedReader(new FileReader(cdcIndexFile));
        String input = in.readLine();
        Integer offset = Integer.parseInt(input);
        Assert.assertEquals(syncOffset, (long)offset);
        in.close();
    }

    @Test
    public void testCompletedFlag() throws IOException
    {
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegment initialSegment = CommitLog.instance.segmentManager.allocatingFrom();
        DatabaseDescriptor.setCDCSpaceInMB(8);
        try
        {
            for (int i = 0; i < 1000; i++)
            {
                new RowUpdateBuilder(currentTableMetadata(), 0, 1)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                .build().apply();
            }
        }
        catch (CDCWriteException ce)
        {
            // pass. Expected since we'll have a file or two linked on restart of CommitLog due to replay
        }

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
        Path linked = new File(DatabaseDescriptor.getCDCLogLocation(), currentSegment.logFile.getName()).toPath();
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

        Path linked = new File(DatabaseDescriptor.getCDCLogLocation(), currentSegment.logFile.getName()).toPath();
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
    public void testReplayLogic() throws IOException
    {
        // Assert.assertEquals(0, new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length);
        String table_name = createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");

        DatabaseDescriptor.setCDCSpaceInMB(8);
        TableMetadata ccfm = Keyspace.open(keyspace()).getColumnFamilyStore(table_name).metadata();
        try
        {
            for (int i = 0; i < 1000; i++)
            {
                new RowUpdateBuilder(ccfm, 0, i)
                    .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                    .build().apply();
            }
            Assert.fail("Expected CDCWriteException from full CDC but did not receive it.");
        }
        catch (CDCWriteException e)
        {
            // pass
        }

        CommitLog.instance.sync(true);
        CommitLog.instance.stopUnsafe(false);

        // Build up a list of expected index files after replay and then clear out cdc_raw
        List<CDCIndexData> oldData = parseCDCIndexData();
        for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
            FileUtils.deleteWithConfirm(f.getAbsolutePath());

        try
        {
            Assert.assertEquals("Expected 0 files in CDC folder after deletion. ",
                                0, new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length);
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
                          new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length > 0);

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
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
            {
                if (f.getName().contains("_cdc.idx"))
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
            try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f))))
            {
                line = br.readLine();
            }
            catch (Exception e)
            {
                throw e;
            }
            fileName = f.getName();
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
        return new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length;
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
}
