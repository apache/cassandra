/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.db;

import java.io.*;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.commitlog.CommitLog;

import org.apache.cassandra.db.commitlog.CommitLogHeader;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.Pair;

public class CommitLogTest extends CleanupHelper
{
    @Test
    public void testCleanup() throws IOException, ExecutionException, InterruptedException
    {
        assert CommitLog.instance().getSegmentCount() == 1;
        CommitLog.setSegmentSize(1000);

        Table table = Table.open("Keyspace1");
        ColumnFamilyStore store1 = table.getColumnFamilyStore("Standard1");
        ColumnFamilyStore store2 = table.getColumnFamilyStore("Standard2");
        RowMutation rm;
        byte[] value = new byte[501];

        // add data.  use relatively large values to force quick segment creation since we have a low flush threshold in the test config.
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation("Keyspace1", "key1".getBytes());
            rm.add(new QueryPath("Standard1", null, "Column1".getBytes()), value, new TimestampClock(0));
            rm.add(new QueryPath("Standard2", null, "Column1".getBytes()), value, new TimestampClock(0));
            rm.apply();
        }
        assert CommitLog.instance().getSegmentCount() > 1;

        // nothing should get removed after flushing just Standard1
        store1.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() > 1;

        // after flushing Standard2 we should be able to clean out all segments
        store2.forceBlockingFlush();
        assert CommitLog.instance().getSegmentCount() == 1;
    }

    @Test
    public void testRecoveryWithEmptyHeader() throws Exception
    {
        testRecovery(new byte[0], new byte[10]);
    }

    @Test
    public void testRecoveryWithShortHeader() throws Exception
    {
        testRecovery(new byte[2], new byte[10]);
    }

    @Test
    public void testRecoveryWithGarbageHeader() throws Exception
    {
        byte[] garbage = new byte[100];
        (new java.util.Random()).nextBytes(garbage);
        testRecovery(garbage, garbage);
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        CommitLog.recover(new File[] {tmpFiles().right});
    }

    @Test
    public void testRecoveryWithShortLog() throws Exception
    {
        // force EOF while reading log
        testRecoveryWithBadSizeArgument(100, 10);
    }

    @Test
    public void testRecoveryWithShortSize() throws Exception
    {
        testRecovery(new byte[0], new byte[2]);
    }

    @Test
    public void testRecoveryWithShortCheckSum() throws Exception
    {
        testRecovery(new byte[0], new byte[6]);
    }

    @Test
    public void testRecoveryWithGarbageLog() throws Exception
    {
        byte[] garbage = new byte[100];
        (new java.util.Random()).nextBytes(garbage);
        testRecovery(new byte[0], garbage);
    }

    @Test
    public void testRecoveryWithBadSizeChecksum() throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(100);
        testRecoveryWithBadSizeArgument(100, 100, ~checksum.getValue());
    }
    
    @Test
    public void testRecoveryWithZeroSegmentSizeArgument() throws Exception
    {
        // many different combinations of 4 bytes (garbage) will be read as zero by readInt()
        testRecoveryWithBadSizeArgument(0, 10); // zero size, but no EOF
    }

    @Test
    public void testRecoveryWithNegativeSizeArgument() throws Exception
    {
        // garbage from a partial/bad flush could be read as a negative size even if there is no EOF
        testRecoveryWithBadSizeArgument(-10, 10); // negative size, but no EOF
    }

    protected void testRecoveryWithBadSizeArgument(int size, int dataSize) throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(size);
        testRecoveryWithBadSizeArgument(size, dataSize, checksum.getValue());
    }

    protected void testRecoveryWithBadSizeArgument(int size, int dataSize, long checksum) throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        dout.writeInt(size);
        dout.writeLong(checksum);
        dout.write(new byte[dataSize]);
        dout.close();
        testRecovery(new byte[0], out.toByteArray());
    }

    protected Pair<File, File> tmpFiles() throws IOException
    {
        File logFile = File.createTempFile("testRecoveryWithPartiallyWrittenHeaderTestFile", null);
        File headerFile = new File(CommitLogHeader.getHeaderPathFromSegmentPath(logFile.getAbsolutePath()));
        logFile.deleteOnExit();
        headerFile.deleteOnExit();
        assert logFile.length() == 0;
        assert headerFile.length() == 0;
        return new Pair<File, File>(headerFile, logFile);
    }

    protected void testRecovery(byte[] headerData, byte[] logData) throws Exception
    {
        Pair<File, File> tmpFiles = tmpFiles();
        File logFile = tmpFiles.right;
        File headerFile = tmpFiles.left;
        OutputStream lout = new FileOutputStream(logFile);
        OutputStream hout = new FileOutputStream(headerFile);
        lout.write(logData);
        hout.write(headerData);
        //statics make it annoying to test things correctly
        CommitLog.recover(new File[] {logFile}); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
    }
}
