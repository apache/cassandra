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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
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
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CommitLogTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String KEYSPACE2 = "CommitLogTestNonDurable";
    private static final String STANDARD1 = "Standard1";
    private static final String STANDARD2 = "Standard2";

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
                { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()) },
                { new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()) },
                { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()) } });
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        // Disable durable writes for system keyspaces to prevent system mutations, e.g. sstable_activity,
        // to end up in CL segments and cause unexpected results in this test wrt counting CL segments,
        // see CASSANDRA-12854
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD2, 0, AsciiType.instance, BytesType.instance));
        SchemaLoader.createKeyspace(KEYSPACE2,
                                    KeyspaceParams.simpleTransient(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD2, 0, AsciiType.instance, BytesType.instance));
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        runExpecting(() -> {
            CommitLog.instance.recover(new File[]{ tmpFile(CommitLogDescriptor.current_version) });
            return null;
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
        byte[] data = new byte[5];
        data[3] = 1; // Not a legacy marker, give it a fake (short) size
        runExpecting(() -> {
            testRecovery(data, CommitLogDescriptor.VERSION_20);
            return null;
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
        runExpecting(() -> {
            testRecoveryWithGarbageLog();
            return null;
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
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD2);

        // Roughly 32 MB mutation
        Mutation m = new RowUpdateBuilder(cfs1.metadata, 0, "k")
                     .clustering("bytes")
                     .add("val", ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize()/4))
                     .build();

        // Adding it 5 times
        CommitLog.instance.add(m);
        CommitLog.instance.add(m);
        CommitLog.instance.add(m);
        CommitLog.instance.add(m);
        CommitLog.instance.add(m);

        // Adding new mutation on another CF
        Mutation m2 = new RowUpdateBuilder(cfs2.metadata, 0, "k")
                      .clustering("bytes")
                      .add("val", ByteBuffer.allocate(4))
                      .build();
        CommitLog.instance.add(m2);

        assert CommitLog.instance.activeSegments() == 2 : "Expecting 2 segments, got " + CommitLog.instance.activeSegments();

        UUID cfid2 = m2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, ReplayPosition.NONE, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.activeSegments() == 2 : "Expecting 2 segments, got " + CommitLog.instance.activeSegments();
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD2);

        // Roughly 32 MB mutation
        Mutation rm = new RowUpdateBuilder(cfs1.metadata, 0, "k")
                      .clustering("bytes")
                      .add("val", ByteBuffer.allocate((DatabaseDescriptor.getCommitLogSegmentSize()/4) - 1))
                      .build();

        // Adding it twice (won't change segment)
        CommitLog.instance.add(rm);
        CommitLog.instance.add(rm);

        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();

        // "Flush": this won't delete anything
        UUID cfid1 = rm.getColumnFamilyIds().iterator().next();
        CommitLog.instance.sync(true);
        CommitLog.instance.discardCompletedSegments(cfid1, ReplayPosition.NONE, CommitLog.instance.getContext());

        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();

        // Adding new mutation on another CF, large enough (including CL entry overhead) that a new segment is created
        Mutation rm2 = new RowUpdateBuilder(cfs2.metadata, 0, "k")
                       .clustering("bytes")
                       .add("val", ByteBuffer.allocate(DatabaseDescriptor.getMaxMutationSize() - 200))
                       .build();
        CommitLog.instance.add(rm2);
        // also forces a new segment, since each entry-with-overhead is just under half the CL size
        CommitLog.instance.add(rm2);
        CommitLog.instance.add(rm2);

        assert CommitLog.instance.activeSegments() == 3 : "Expecting 3 segments, got " + CommitLog.instance.activeSegments();


        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, ReplayPosition.NONE, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assert CommitLog.instance.activeSegments() == 1 : "Expecting 1 segment, got " + CommitLog.instance.activeSegments();
    }

    private static int getMaxRecordDataSize(String keyspace, ByteBuffer key, String cfName, String colName)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(cfName);
        // We don't want to allocate a size of 0 as this is optimized under the hood and our computation would
        // break testEqualRecordLimit
        int allocSize = 1;
        Mutation rm = new RowUpdateBuilder(cfs.metadata, 0, key)
                      .clustering(colName)
                      .add("val", ByteBuffer.allocate(allocSize)).build();

        int max = DatabaseDescriptor.getMaxMutationSize();
        max -= CommitLogSegment.ENTRY_OVERHEAD_SIZE; // log entry overhead

        // Note that the size of the value if vint encoded. So we first compute the ovehead of the mutation without the value and it's size
        int mutationOverhead = (int)Mutation.serializer.serializedSize(rm, MessagingService.current_version) - (VIntCoding.computeVIntSize(allocSize) + allocSize);
        max -= mutationOverhead;

        // Now, max is the max for both the value and it's size. But we want to know how much we can allocate, i.e. the size of the value.
        int sizeOfMax = VIntCoding.computeVIntSize(max);
        max -= sizeOfMax;
        assert VIntCoding.computeVIntSize(max) == sizeOfMax; // sanity check that we're still encoded with the size we though we would
        return max;
    }

    private static int getMaxRecordDataSize()
    {
        return getMaxRecordDataSize(KEYSPACE1, bytes("k"), STANDARD1, "bytes");
    }

    // CASSANDRA-3615
    @Test
    public void testEqualRecordLimit() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation rm = new RowUpdateBuilder(cfs.metadata, 0, "k")
                      .clustering("bytes")
                      .add("val", ByteBuffer.allocate(getMaxRecordDataSize()))
                      .build();

        CommitLog.instance.add(rm);
    }

    @Test
    public void testExceedRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        try
        {
            Mutation rm = new RowUpdateBuilder(cfs.metadata, 0, "k")
                          .clustering("bytes")
                          .add("val", ByteBuffer.allocate(1 + getMaxRecordDataSize()))
                          .build();
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

    protected Void testRecovery(byte[] logData, int version) throws Exception
    {
        File logFile = tmpFile(version);
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(logData);
            //statics make it annoying to test things correctly
            CommitLog.instance.recover(logFile.getPath()); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
        }
        return null;
    }

    protected Void testRecovery(CommitLogDescriptor desc, byte[] logData) throws Exception
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
        return null;
    }

    @Test
    public void testRecoveryWithIdMismatch() throws Exception
    {
        CommitLogDescriptor desc = new CommitLogDescriptor(4, null);
        File logFile = tmpFile(desc.version);
        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc);
        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(buf.array(), 0, buf.position());

            runExpecting(() -> {
                CommitLog.instance.recover(logFile.getPath()); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
                return null;
            }, CommitLogReplayException.class);
        }
    }

    @Test
    public void testRecoveryWithBadCompressor() throws Exception
    {
        CommitLogDescriptor desc = new CommitLogDescriptor(4, new ParameterizedClass("UnknownCompressor", null));
        runExpecting(() -> {
            testRecovery(desc, new byte[0]);
            return null;
        }, CommitLogReplayException.class);
    }

    protected void runExpecting(Callable<Void> r, Class<?> expected)
    {
        JVMStabilityInspector.Killer originalKiller;
        KillerForTests killerForTests;

        killerForTests = new KillerForTests();
        originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        Throwable caught = null;
        try
        {
            r.call();
        }
        catch (Throwable t)
        {
            if (expected != t.getClass())
                throw new AssertionError("Expected exception " + expected + ", got " + t, t);
            caught = t;
        }
        if (expected != null && caught == null)
            Assert.fail("Expected exception " + expected + " but call completed successfully.");

        JVMStabilityInspector.replaceKiller(originalKiller);
        assertEquals("JVM killed", expected != null, killerForTests.wasKilled());
    }

    protected void testRecovery(final byte[] logData, Class<?> expected) throws Exception
    {
        runExpecting(() -> testRecovery(logData, CommitLogDescriptor.VERSION_20), expected);
        runExpecting(() -> testRecovery(new CommitLogDescriptor(4, null), logData), expected);
    }

    @Test
    public void testTruncateWithoutSnapshot() throws ExecutionException, InterruptedException, IOException
    {
        boolean originalState = DatabaseDescriptor.isAutoSnapshot();
        try
        {
            CommitLog.instance.resetUnsafe(true);
            boolean prev = DatabaseDescriptor.isAutoSnapshot();
            DatabaseDescriptor.setAutoSnapshot(false);
            ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
            ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD2);

            new RowUpdateBuilder(cfs1.metadata, 0, "k").clustering("bytes").add("val", ByteBuffer.allocate(100)).build().applyUnsafe();
            cfs1.truncateBlocking();
            DatabaseDescriptor.setAutoSnapshot(prev);
            Mutation m2 = new RowUpdateBuilder(cfs2.metadata, 0, "k")
                          .clustering("bytes")
                          .add("val", ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 4))
                          .build();

            for (int i = 0 ; i < 5 ; i++)
                CommitLog.instance.add(m2);

            assertEquals(2, CommitLog.instance.activeSegments());
            ReplayPosition position = CommitLog.instance.getContext();
            for (Keyspace ks : Keyspace.system())
                for (ColumnFamilyStore syscfs : ks.getColumnFamilyStores())
                    CommitLog.instance.discardCompletedSegments(syscfs.metadata.cfId, ReplayPosition.NONE, position);
            CommitLog.instance.discardCompletedSegments(cfs2.metadata.cfId, ReplayPosition.NONE, position);
            assertEquals(1, CommitLog.instance.activeSegments());
        }
        finally
        {
            DatabaseDescriptor.setAutoSnapshot(originalState);
        }
    }

    @Test
    public void testTruncateWithoutSnapshotNonDurable() throws IOException
    {
        boolean originalState = DatabaseDescriptor.getAutoSnapshot();
        try
        {
            DatabaseDescriptor.setAutoSnapshot(false);
            Keyspace notDurableKs = Keyspace.open(KEYSPACE2);
            Assert.assertFalse(notDurableKs.getMetadata().params.durableWrites);

            ColumnFamilyStore cfs = notDurableKs.getColumnFamilyStore("Standard1");
            new RowUpdateBuilder(cfs.metadata, 0, "key1")
                .clustering("bytes").add("val", ByteBufferUtil.bytes("abcd"))
                .build()
                .applyUnsafe();

            assertTrue(Util.getOnlyRow(Util.cmd(cfs).columns("val").build())
                            .cells().iterator().next().value().equals(ByteBufferUtil.bytes("abcd")));

            cfs.truncateBlocking();

            Util.assertEmpty(Util.cmd(cfs).columns("val").build());
        }
        finally
        {
            DatabaseDescriptor.setAutoSnapshot(originalState);
        }
    }

    @Test
    public void testUnwriteableFlushRecovery() throws ExecutionException, InterruptedException, IOException
    {
        CommitLog.instance.resetUnsafe(true);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.ignore);

            for (int i = 0 ; i < 5 ; i++)
            {
                new RowUpdateBuilder(cfs.metadata, 0, "k")
                    .clustering("c" + i).add("val", ByteBuffer.allocate(100))
                    .build()
                    .apply();

                if (i == 2)
                {
                    try (Closeable c = Util.markDirectoriesUnwriteable(cfs))
                    {
                        cfs.forceBlockingFlush();
                    }
                    catch (Throwable t)
                    {
                        // expected. Cause (after some wrappings) should be a write error
                        while (!(t instanceof FSWriteError))
                            t = t.getCause();
                    }
                }
                else
                    cfs.forceBlockingFlush();
            }
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }

        CommitLog.instance.sync(true);
        System.setProperty("cassandra.replayList", KEYSPACE1 + "." + STANDARD1);
        // Currently we don't attempt to re-flush a memtable that failed, thus make sure data is replayed by commitlog.
        // If retries work subsequent flushes should clear up error and this should change to expect 0.
        Assert.assertEquals(1, CommitLog.instance.resetUnsafe(false));
    }

    public void testOutOfOrderFlushRecovery(BiConsumer<ColumnFamilyStore, Memtable> flushAction, boolean performCompaction)
            throws ExecutionException, InterruptedException, IOException
    {
        CommitLog.instance.resetUnsafe(true);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        for (int i = 0 ; i < 5 ; i++)
        {
            new RowUpdateBuilder(cfs.metadata, 0, "k")
                .clustering("c" + i).add("val", ByteBuffer.allocate(100))
                .build()
                .apply();

            Memtable current = cfs.getTracker().getView().getCurrentMemtable();
            if (i == 2)
                current.makeUnflushable();

            flushAction.accept(cfs, current);
        }
        if (performCompaction)
            cfs.forceMajorCompaction();
        // Make sure metadata saves and reads fine
        for (SSTableReader reader : cfs.getLiveSSTables())
            reader.reloadSSTableMetadata();

        CommitLog.instance.sync(true);
        System.setProperty("cassandra.replayList", KEYSPACE1 + "." + STANDARD1);
        // In the absence of error, this should be 0 because forceBlockingFlush/forceRecycleAllSegments would have
        // persisted all data in the commit log. Because we know there was an error, there must be something left to
        // replay.
        Assert.assertEquals(1, CommitLog.instance.resetUnsafe(false));
    }

    BiConsumer<ColumnFamilyStore, Memtable> flush = (cfs, current) ->
    {
        try
        {
            cfs.forceBlockingFlush();
        }
        catch (Throwable t)
        {
            // expected after makeUnflushable. Cause (after some wrappings) should be a write error
            while (!(t instanceof FSWriteError))
                t = t.getCause();
            // Wait for started flushes to complete.
            cfs.switchMemtableIfCurrent(current);
        }
    };

    BiConsumer<ColumnFamilyStore, Memtable> recycleSegments = (cfs, current) ->
    {
        // Move to new commit log segment and try to flush all data. Also delete segments that no longer contain
        // flushed data.
        // This does not stop on errors and should retain segments for which flushing failed.
        CommitLog.instance.forceRecycleAllSegments();

        // Wait for started flushes to complete.
        cfs.switchMemtableIfCurrent(current);
    };

    @Test
    public void testOutOfOrderFlushRecovery() throws ExecutionException, InterruptedException, IOException
    {
        testOutOfOrderFlushRecovery(flush, false);
    }

    @Test
    public void testOutOfOrderLogDiscard() throws ExecutionException, InterruptedException, IOException
    {
        testOutOfOrderFlushRecovery(recycleSegments, false);
    }

    @Test
    public void testOutOfOrderFlushRecoveryWithCompaction() throws ExecutionException, InterruptedException, IOException
    {
        testOutOfOrderFlushRecovery(flush, true);
    }

    @Test
    public void testOutOfOrderLogDiscardWithCompaction() throws ExecutionException, InterruptedException, IOException
    {
        testOutOfOrderFlushRecovery(recycleSegments, true);
    }
}

