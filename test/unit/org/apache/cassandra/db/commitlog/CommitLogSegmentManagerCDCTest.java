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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogSegment.CDCState;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.FileUtils;

public class CommitLogSegmentManagerCDCTest extends CQLTester
{
    private static Random random = new Random();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setCDCEnabled(true);
        CQLTester.setUpClass();
    }

    @Before
    public void before() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
        for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles())
            FileUtils.deleteWithConfirm(f);
    }

    @Test
    public void testCDCWriteTimeout() throws Throwable
    {
        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;
        CFMetaData cfm = currentTableMetadata();

        // Confirm that logic to check for whether or not we can allocate new CDC segments works
        Integer originalCDCSize = DatabaseDescriptor.getCDCSpaceInMB();
        try
        {
            DatabaseDescriptor.setCDCSpaceInMB(32);
            // Spin until we hit CDC capacity and make sure we get a WriteTimeout
            try
            {
                // Should trigger on anything < 20:1 compression ratio during compressed test
                for (int i = 0; i < 100; i++)
                {
                    new RowUpdateBuilder(cfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
                Assert.fail("Expected WriteTimeoutException from full CDC but did not receive it.");
            }
            catch (WriteTimeoutException e)
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
    public void testCLSMCDCDiscardLogic() throws Throwable
    {
        CommitLogSegmentManagerCDC cdcMgr = (CommitLogSegmentManagerCDC)CommitLog.instance.segmentManager;

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=false;");
        for (int i = 0; i < 8; i++)
        {
            new RowUpdateBuilder(currentTableMetadata(), 0, i)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 4)) // fit 3 in a segment
                .build().apply();
        }

        // Should have 4 segments CDC since we haven't flushed yet, 3 PERMITTED, one of which is active, and 1 PERMITTED, in waiting
        Assert.assertEquals(4 * DatabaseDescriptor.getCommitLogSegmentSize(), cdcMgr.updateCDCTotalSize());
        expectCurrentCDCState(CDCState.PERMITTED);
        CommitLog.instance.forceRecycleAllSegments();

        // on flush, these PERMITTED should be deleted
        Assert.assertEquals(0, new File(DatabaseDescriptor.getCDCLogLocation()).listFiles().length);

        createTable("CREATE TABLE %s (idx int, data text, primary key(idx)) WITH cdc=true;");
        for (int i = 0; i < 8; i++)
        {
            new RowUpdateBuilder(currentTableMetadata(), 0, i)
                .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 4))
                .build().apply();
        }
        // 4 total again, 3 CONTAINS, 1 in waiting PERMITTED
        Assert.assertEquals(4 * DatabaseDescriptor.getCommitLogSegmentSize(), cdcMgr.updateCDCTotalSize());
        CommitLog.instance.forceRecycleAllSegments();
        expectCurrentCDCState(CDCState.PERMITTED);

        // On flush, PERMITTED is deleted, CONTAINS is preserved.
        cdcMgr.awaitManagementTasksCompletion();
        int seen = getCDCRawCount();
        Assert.assertTrue("Expected >3 files in cdc_raw, saw: " + seen, seen >= 3);
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
            CFMetaData ccfm = Keyspace.open(keyspace()).getColumnFamilyStore(ct).metadata;
            // Spin until we hit CDC capacity and make sure we get a WriteTimeout
            try
            {
                for (int i = 0; i < 1000; i++)
                {
                    new RowUpdateBuilder(ccfm, 0, i)
                        .add("data", randomizeBuffer(DatabaseDescriptor.getCommitLogSegmentSize() / 3))
                        .build().apply();
                }
                Assert.fail("Expected WriteTimeoutException from full CDC but did not receive it.");
            }
            catch (WriteTimeoutException e) { }

            expectCurrentCDCState(CDCState.FORBIDDEN);
            CommitLog.instance.forceRecycleAllSegments();

            cdcMgr.awaitManagementTasksCompletion();
            new File(DatabaseDescriptor.getCDCLogLocation()).listFiles()[0].delete();
            cdcMgr.updateCDCTotalSize();
            // Confirm cdc update process changes flag on active segment
            expectCurrentCDCState(CDCState.PERMITTED);

            // Clear out archived CDC files
            for (File f : new File(DatabaseDescriptor.getCDCLogLocation()).listFiles()) {
                FileUtils.deleteWithConfirm(f);
            }

            // Set space to 0, confirm newly allocated segments are FORBIDDEN
            DatabaseDescriptor.setCDCSpaceInMB(0);
            CommitLog.instance.forceRecycleAllSegments();
            CommitLog.instance.segmentManager.awaitManagementTasksCompletion();
            expectCurrentCDCState(CDCState.FORBIDDEN);
        }
        finally
        {
            DatabaseDescriptor.setCDCSpaceInMB(origSize);
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

    private void expectCurrentCDCState(CDCState state)
    {
        Assert.assertEquals("Received unexpected CDCState on current allocatingFrom segment.",
            state, CommitLog.instance.segmentManager.allocatingFrom().getCDCState());
    }
}
