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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.commitlog.MalformedCommitLogException;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.PureJavaCrc32;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CommitLogTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2));
        System.setProperty("cassandra.commitlog.stop_on_errors", "true");
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        testMalformed(badLogFile(new byte[0]));
    }

    @Test
    public void testRecoveryWithShortLog() throws Exception
    {
        // force EOF while reading log
        testMalformed(badLogFile(100, 10));
    }

    @Test
    public void testRecoveryWithShortSize() throws Exception
    {
        testMalformed(new byte[2]);
    }

    @Test
    public void testRecoveryWithShortCheckSum() throws Exception
    {
        testMalformed(new byte[6]);
    }

    @Test
    public void testRecoveryWithGarbageLog() throws Exception
    {
        testMalformed(garbage(100));
    }

    @Test
    public void testRecoveryWithBadSizeChecksum() throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(100);
        testMalformed(badLogFile(100, checksum.getValue(), new byte[100]));
        testMalformed(badLogFile(100, checksum.getValue(), garbage(100)));
    }

    @Test
    public void testRecoveryWithBadSize() throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(100);
        testMalformed(badLogFile(120, checksum.getValue(), garbage(100)));
    }

    @Test
    public void testRecoveryWithZeroSegmentSizeArgument() throws Exception
    {
        // many different combinations of 4 bytes (garbage) will be read as zero by readInt()
        testMalformed(badLogFile(0, -1L, 10)); // zero size, but no EOF
    }

    @Test
    public void testRecoveryWithNegativeSizeArgument() throws Exception
    {
        // garbage from a partial/bad flush could be read as a negative size even if there is no EOF
        testMalformed(badLogFile(-10, 10)); // zero size, but no EOF
    }

    @Test
    public void testDontDeleteIfDirty() throws Exception
    {
        CommitLog.instance.resetUnsafe();
        // Roughly 32 MB mutation
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize()/4), 0);

        // Adding it 5 times
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        // Adding new mutation on another CF
        Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add(CF2, Util.cellname("c1"), ByteBuffer.allocate(4), 0);
        CommitLog.instance.add(rm2);

        assert CommitLog.instance.activeSegments() == 2 : "Expecting 2 segments, got " + CommitLog.instance.activeSegments();

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.activeSegments() == 2 : "Expecting 2 segments, got " + CommitLog.instance.activeSegments();
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        DatabaseDescriptor.getCommitLogSegmentSize();
        CommitLog.instance.resetUnsafe();
        // Roughly 32 MB mutation
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize()/4) - 1), 0);

        // Adding it twice (won't change segment)
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();

        // "Flush": this won't delete anything
        UUID cfid1 = rm.getColumnFamilyIds().iterator().next();
        CommitLog.instance.sync(true);
        CommitLog.instance.discardCompletedSegments(cfid1, CommitLog.instance.getContext());

        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();

        // Adding new mutation on another CF, large enough (including CL entry overhead) that a new segment is created
        Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add(CF2, Util.cellname("c1"), ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize()/2) - 100), 0);
        CommitLog.instance.add(rm2);
        // also forces a new segment, since each entry-with-overhead is just under half the CL size
        CommitLog.instance.add(rm2);
        CommitLog.instance.add(rm2);

        assert CommitLog.instance.activeSegments() == 3 : "Expecting 3 segments, got " + CommitLog.instance.activeSegments();


        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();
    }

    private static int getMaxRecordDataSize(String keyspace, ByteBuffer key, String table, CellName column)
    {
        Mutation rm = new Mutation(keyspace, key);
        rm.add(table, column, ByteBuffer.allocate(0), 0);

        int max = (DatabaseDescriptor.getCommitLogSegmentSize() / 2);
        max -= CommitLogSegment.ENTRY_OVERHEAD_SIZE; // log entry overhead
        return max - (int) Mutation.serializer.serializedSize(rm, MessagingService.current_version);
    }

    private static int getMaxRecordDataSize()
    {
        return getMaxRecordDataSize(KEYSPACE1, bytes("k"), CF1, Util.cellname("c1"));
    }

    // CASSANDRA-3615
    @Test
    public void testEqualRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe();

        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(getMaxRecordDataSize()), 0);
        CommitLog.instance.add(rm);
    }

    @Test
    public void testExceedRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe();
        try
        {
            Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
            rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(1 + getMaxRecordDataSize()), 0);
            CommitLog.instance.add(rm);
            throw new AssertionError("mutation larger than limit was accepted");
        }
        catch (IllegalArgumentException e)
        {
            // IAE is thrown on too-large mutations
        }
    }

    // construct log file with correct chunk checksum for the provided size/position
    protected File badLogFile(int markerSize, int realSize) throws Exception
    {
        return badLogFile(markerSize, garbage(realSize));
    }

    protected File badLogFile(int markerSize, byte[] data) throws Exception
    {
        File logFile = tmpFile();
        CommitLogDescriptor descriptor = CommitLogDescriptor.fromFileName(logFile.getName());
        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        crc.updateInt(CommitLogDescriptor.HEADER_SIZE);
        return badLogFile(markerSize, crc.getCrc(), data, logFile);
    }

    protected byte[] garbage(int size)
    {
        byte[] garbage = new byte[size];
        (new java.util.Random()).nextBytes(garbage);
        return garbage;
    }

    protected File badLogFile(int markerSize, long checksum, int realSize) throws Exception
    {
        return badLogFile(markerSize, checksum, realSize, tmpFile());
    }

    protected File badLogFile(int markerSize, long checksum, int realSize, File logFile) throws Exception
    {
        return badLogFile(markerSize, checksum, new byte[realSize], logFile);
    }

    protected File badLogFile(int markerSize, long checksum, byte[] chunk) throws Exception
    {
        return badLogFile(markerSize, checksum, chunk, tmpFile());
    }

    protected File badLogFile(int markerSize, long checksum, byte[] chunk, File logFile) throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(out);
        ByteBuffer buffer = ByteBuffer.allocate(CommitLogDescriptor.HEADER_SIZE);
        CommitLogDescriptor.writeHeader(buffer, CommitLogDescriptor.fromFileName(logFile.getName()));
        out.write(buffer.array());
        dout.writeInt(markerSize);
        dout.writeLong(checksum);
        dout.write(chunk);
        dout.close();
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(out.toByteArray());
            lout.close();
        }
        return logFile;
    }

    protected File badLogFile(byte[] contents) throws Exception
    {
        File logFile = tmpFile();
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(contents);
            lout.close();
        }
        return logFile;
    }

    protected File tmpFile() throws IOException
    {
        File logFile = File.createTempFile("CommitLog-" + CommitLogDescriptor.current_version + "-", ".log");
        logFile.deleteOnExit();
        assert logFile.length() == 0;
        return logFile;
    }

    private void testMalformed(byte[] contents) throws Exception
    {
        testMalformed(badLogFile(contents));
        testMalformed(badLogFile(contents.length, contents));
    }

    private void testMalformed(File logFile) throws Exception
    {
        CommitLogReplayer.setIgnoreErrors(true);
        CommitLog.instance.recover(new File[]{ logFile });
        CommitLogReplayer.setIgnoreErrors(false);
        try
        {
            CommitLog.instance.recover(new File[]{ logFile });
            Assert.assertFalse(true);
        }
        catch (Throwable t)
        {
            if (!(t instanceof MalformedCommitLogException))
                throw t;
        }
    }

    @Test
    public void testVersions()
    {
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-1340512736956320000.log"));
        Assert.assertTrue(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog--2-1340512736956320000.log"));
        Assert.assertFalse(CommitLogDescriptor.isValid("CommitLog-2-1340512736956320000-123.log"));

        Assert.assertEquals(1340512736956320000L, CommitLogDescriptor.fromFileName("CommitLog-2-1340512736956320000.log").id);

        Assert.assertEquals(MessagingService.current_version, new CommitLogDescriptor(1340512736956320000L).getMessagingVersion());
        String newCLName = "CommitLog-" + CommitLogDescriptor.current_version + "-1340512736956320000.log";
        Assert.assertEquals(MessagingService.current_version, CommitLogDescriptor.fromFileName(newCLName).getMessagingVersion());
    }

    @Test
    public void testCommitFailurePolicy_stop()
    {
        File commitDir = new File(DatabaseDescriptor.getCommitLogLocation());

        try
        {

            DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.stop);
            commitDir.setWritable(false);
            Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
            rm.add("Standard1", Util.cellname("c1"), ByteBuffer.allocate(100), 0);

            // Adding it twice (won't change segment)
            CommitLog.instance.add(rm);
            Uninterruptibles.sleepUninterruptibly((int) DatabaseDescriptor.getCommitLogSyncBatchWindow(), TimeUnit.MILLISECONDS);
            Assert.assertFalse(StorageService.instance.isRPCServerRunning());
            Assert.assertFalse(StorageService.instance.isNativeTransportRunning());
            Assert.assertFalse(StorageService.instance.isInitialized());

        }
        finally
        {
            commitDir.setWritable(true);
        }
    }

}
