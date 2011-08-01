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
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.Pair;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CommitLogTest extends CleanupHelper
{
    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        CommitLog.recover(new File[] {tmpFile()});
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
        testRecovery(new byte[2]);
    }

    @Test
    public void testRecoveryWithShortCheckSum() throws Exception
    {
        testRecovery(new byte[6]);
    }

    @Test
    public void testRecoveryWithGarbageLog() throws Exception
    {
        byte[] garbage = new byte[100];
        (new java.util.Random()).nextBytes(garbage);
        testRecovery(garbage);
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

    @Test
    public void testDontDeleteIfDirty() throws Exception
    {
        CommitLog.instance.resetUnsafe();
        // Roughly 32 MB mutation
        RowMutation rm = new RowMutation("Keyspace1", bytes("k"));
        rm.add(new QueryPath("Standard1", null, bytes("c1")), ByteBuffer.allocate(32 * 1024 * 1024), 0);

        // Adding it 5 times
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        // Adding new mutation on another CF
        RowMutation rm2 = new RowMutation("Keyspace1", bytes("k"));
        rm2.add(new QueryPath("Standard2", null, bytes("c1")), ByteBuffer.allocate(4), 0);
        CommitLog.instance.add(rm2);

        assert CommitLog.instance.segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance.segmentsCount();

        int cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance.segmentsCount();
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        CommitLog.instance.resetUnsafe();
        // Roughly 32 MB mutation
        RowMutation rm = new RowMutation("Keyspace1", bytes("k"));
        rm.add(new QueryPath("Standard1", null, bytes("c1")), ByteBuffer.allocate(32 * 1024 * 1024), 0);

        // Adding it twice (won't change segment)
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        assert CommitLog.instance.segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance.segmentsCount();

        // "Flush": this won't delete anything
        int cfid1 = rm.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid1, CommitLog.instance.getContext());

        assert CommitLog.instance.segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance.segmentsCount();

        // Adding new mutation on another CF so that a new segment is created
        RowMutation rm2 = new RowMutation("Keyspace1", bytes("k"));
        rm2.add(new QueryPath("Standard2", null, bytes("c1")), ByteBuffer.allocate(64 * 1024 * 1024), 0);
        CommitLog.instance.add(rm2);
        CommitLog.instance.add(rm2);

        assert CommitLog.instance.segmentsCount() == 2 : "Expecting 2 segments, got " + CommitLog.instance.segmentsCount();


        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        int cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.segmentsCount() == 1 : "Expecting 1 segment, got " + CommitLog.instance.segmentsCount();
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
        testRecovery(out.toByteArray());
    }

    protected File tmpFile() throws IOException
    {
        File logFile = File.createTempFile("testRecoveryWithPartiallyWrittenHeaderTestFile", null);
        logFile.deleteOnExit();
        assert logFile.length() == 0;
        return logFile;
    }

    protected void testRecovery(byte[] logData) throws Exception
    {
        File logFile = tmpFile();
        OutputStream lout = new FileOutputStream(logFile);
        lout.write(logData);
        //statics make it annoying to test things correctly
        CommitLog.recover(new File[] {logFile}); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
    }
}
