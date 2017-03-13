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

package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

@RunWith(Parameterized.class)
public class CommitLogTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String KEYSPACE2 = "CommitLogTestNonDurable";
    private static final String CF1 = "Standard1";
    private static final String CF2 = "Standard2";

    public CommitLogTest(ParameterizedClass commitLogCompression)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
    }

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][] {
                { null }, // No compression
                { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.<String, String>emptyMap()) },
                { new ParameterizedClass(SnappyCompressor.class.getName(), Collections.<String, String>emptyMap()) },
                { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.<String, String>emptyMap()) } });
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    false,
                                    true,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF2));
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        runExpecting(new WrappedRunnable() {
            public void runMayThrow() throws Exception
            {
                CommitLog.instance.recover(new File[]{ tmpFile(CommitLogDescriptor.current_version) });
            }
        }, CommitLogReplayException.class);
    }

    @Test
    public void testRecoveryWithEmptyLog20() throws Exception
    {
        CommitLog.instance.recover(new File[]{ tmpFile(CommitLogDescriptor.VERSION_20) });
    }

    @Test
    public void testRecoveryWithZeroLog() throws Exception
    {
        testRecovery(new byte[10], null);
    }

    @Test
    public void testRecoveryWithShortLog() throws Exception
    {
        // force EOF while reading log
        testRecoveryWithBadSizeArgument(100, 10);
    }

    @Test
    public void testRecoveryWithShortPadding() throws Exception
    {
        // If we have 0-3 bytes remaining, commitlog replayer
        // should pass, because there's insufficient room
        // left in the segment for the legacy size marker.
        testRecovery(new byte[1], null);
        testRecovery(new byte[2], null);
        testRecovery(new byte[3], null);
    }

    @Test
    public void testRecoveryWithShortSize() throws Exception
    {
        runExpecting(new WrappedRunnable()  {
            public void runMayThrow() throws Exception {
                byte[] data = new byte[5];
                data[3] = 1; // Not a legacy marker, give it a fake (short) size
                testRecovery(data, CommitLogDescriptor.VERSION_20);
            }
        }, CommitLogReplayException.class);
    }

    @Test
    public void testRecoveryWithShortCheckSum() throws Exception
    {
        byte[] data = new byte[8];
        data[3] = 10;   // make sure this is not a legacy end marker.
        testRecovery(data, CommitLogReplayException.class);
    }

    @Test
    public void testRecoveryWithShortMutationSize() throws Exception
    {
        testRecoveryWithBadSizeArgument(9, 10);
    }

    private void testRecoveryWithGarbageLog() throws Exception
    {
        byte[] garbage = new byte[100];
        (new java.util.Random()).nextBytes(garbage);
        testRecovery(garbage, CommitLogDescriptor.current_version);
    }

    @Test
    public void testRecoveryWithGarbageLog_fail() throws Exception
    {
        runExpecting(new WrappedRunnable() {
            public void runMayThrow() throws Exception
            {
                testRecoveryWithGarbageLog();
            }
        }, CommitLogReplayException.class);
    }

    @Test
    public void testRecoveryWithGarbageLog_ignoredByProperty() throws Exception
    {
        try {
            System.setProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY, "true");
            testRecoveryWithGarbageLog();
        } finally {
            System.clearProperty(CommitLogReplayer.IGNORE_REPLAY_ERRORS_PROPERTY);
        }
    }

    @Test
    public void testRecoveryWithBadSizeChecksum() throws Exception
    {
        Checksum checksum = new CRC32();
        checksum.update(100);
        testRecoveryWithBadSizeArgument(100, 100, ~checksum.getValue());
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
        rm2.add(CF2, Util.cellname("c1"), ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize()/2) - 200), 0);
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
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add("Standard1", Util.cellname("c1"), ByteBuffer.allocate(0), 0);

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
        Mutation rm = new Mutation(KEYSPACE1, bytes("k"));
        rm.add(CF1, Util.cellname("c1"), ByteBuffer.allocate(getMaxRecordDataSize()), 0);
        CommitLog.instance.add(rm);
    }

    @Test
    public void testExceedRecordLimit() throws Exception
    {
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
        testRecovery(out.toByteArray(), CommitLogReplayException.class);
    }

    protected File tmpFile(int version) throws IOException
    {
        File logFile = File.createTempFile("CommitLog-" + version + "-", ".log");
        logFile.deleteOnExit();
        assert logFile.length() == 0;
        return logFile;
    }

    protected void testRecovery(byte[] logData, int version) throws Exception
    {
        File logFile = tmpFile(version);
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(logData);
            //statics make it annoying to test things correctly
            CommitLog.instance.recover(logFile.getPath()); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
        }
    }

    protected void testRecovery(CommitLogDescriptor desc, byte[] logData) throws Exception
    {
        File logFile = tmpFile(desc.version);
        CommitLogDescriptor fromFile = CommitLogDescriptor.fromFileName(logFile.getName());
        // Change id to match file.
        desc = new CommitLogDescriptor(desc.version, fromFile.id, desc.compression);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(buf.array(), 0, buf.position());
            lout.write(logData);
            //statics make it annoying to test things correctly
            CommitLog.instance.recover(logFile.getPath()); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
        }
    }

    @Test
    public void testRecoveryWithIdMismatch() throws Exception
    {
        CommitLogDescriptor desc = new CommitLogDescriptor(4, null);
        final File logFile = tmpFile(desc.version);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(buf.array(), 0, buf.position());

            runExpecting(new WrappedRunnable() {
                public void runMayThrow() throws Exception
                {
                    CommitLog.instance.recover(logFile.getPath()); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
                }
            }, CommitLogReplayException.class);
        }
    }

    protected void runExpecting(Runnable r, Class<?> expected)
    {
        JVMStabilityInspector.Killer originalKiller;
        KillerForTests killerForTests;

        killerForTests = new KillerForTests();
        originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        Throwable caught = null;
        try
        {
            r.run();
        }
        catch (RuntimeException e)
        {
            if (expected != e.getCause().getClass())
                throw new AssertionError("Expected exception " + expected + ", got " + e, e);
            caught = e;
        }
        if (expected != null && caught == null)
            Assert.fail("Expected exception " + expected + " but call completed successfully.");

        JVMStabilityInspector.replaceKiller(originalKiller);
        Assert.assertEquals("JVM killed", expected != null, killerForTests.wasKilled());
    }

    protected void testRecovery(final byte[] logData, Class<?> expected) throws Exception
    {
        runExpecting(new WrappedRunnable() {
            public void runMayThrow() throws Exception
            {
                testRecovery(logData, CommitLogDescriptor.VERSION_20);
            }
        }, expected);
        runExpecting(new WrappedRunnable() {
            public void runMayThrow() throws Exception
            {
                testRecovery(new CommitLogDescriptor(4, null), logData);
            }
        }, expected);
    }

    @Test
    public void testTruncateWithoutSnapshot() throws ExecutionException, InterruptedException, IOException
    {
        boolean prev = DatabaseDescriptor.isAutoSnapshot();
        DatabaseDescriptor.setAutoSnapshot(false);
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard1");
        ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore("Standard2");

        final Mutation rm1 = new Mutation(KEYSPACE1, bytes("k"));
        rm1.add("Standard1", Util.cellname("c1"), ByteBuffer.allocate(100), 0);
        rm1.apply();
        cfs1.truncateBlocking();
        DatabaseDescriptor.setAutoSnapshot(prev);
        final Mutation rm2 = new Mutation(KEYSPACE1, bytes("k"));
        rm2.add("Standard2", Util.cellname("c1"), ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 4), 0);

        for (int i = 0 ; i < 5 ; i++)
            CommitLog.instance.add(rm2);

        Assert.assertEquals(2, CommitLog.instance.activeSegments());
        ReplayPosition position = CommitLog.instance.getContext();
        for (Keyspace ks : Keyspace.system())
            for (ColumnFamilyStore syscfs : ks.getColumnFamilyStores())
                CommitLog.instance.discardCompletedSegments(syscfs.metadata.cfId, position);
        CommitLog.instance.discardCompletedSegments(cfs2.metadata.cfId, position);
        Assert.assertEquals(1, CommitLog.instance.activeSegments());
    }

    @Test
    public void testTruncateWithoutSnapshotNonDurable() throws IOException
    {
        boolean prevAutoSnapshot = DatabaseDescriptor.isAutoSnapshot();
        DatabaseDescriptor.setAutoSnapshot(false);
        Keyspace notDurableKs = Keyspace.open(KEYSPACE2);
        Assert.assertFalse(notDurableKs.getMetadata().durableWrites);
        ColumnFamilyStore cfs = notDurableKs.getColumnFamilyStore("Standard1");
        CellNameType type = notDurableKs.getColumnFamilyStore("Standard1").getComparator();
        Mutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new Mutation(KEYSPACE2, dk.getKey());
        rm.add("Standard1", Util.cellname("Column1"), ByteBufferUtil.bytes("abcd"), 0);
        rm.apply();

        ReadCommand command = new SliceByNamesReadCommand(KEYSPACE2, dk.getKey(), "Standard1", System.currentTimeMillis(), new NamesQueryFilter(FBUtilities.singleton(Util.cellname("Column1"), type)));
        Row row = command.getRow(notDurableKs);
        Cell col = row.cf.getColumn(Util.cellname("Column1"));
        Assert.assertEquals(col.value(), ByteBuffer.wrap("abcd".getBytes()));
        cfs.truncateBlocking();
        DatabaseDescriptor.setAutoSnapshot(prevAutoSnapshot);
        row = command.getRow(notDurableKs);
        Assert.assertEquals(null, row.cf);
    }
}
