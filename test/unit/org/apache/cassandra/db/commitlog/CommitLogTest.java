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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.collect.Iterables;

import org.junit.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLogReplayer.CommitLogReplayException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommitLogTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String KEYSPACE2 = "CommitLogTestNonDurable";
    private static final String STANDARD1 = "Standard1";
    private static final String STANDARD2 = "Standard2";

    String logDirectory;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
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

    @Before
    public void setup() throws IOException
    {
        logDirectory = DatabaseDescriptor.getCommitLogLocation() + "/unit";
        new File(logDirectory).mkdirs();
    }

    @Test
    public void testRecoveryWithEmptyLog() throws Exception
    {
        runExpecting(() -> {
            CommitLog.instance.recover(tmpFile(CommitLogDescriptor.current_version));
            return null;
        }, CommitLogReplayException.class);
    }

    @Test
    public void testRecoveryWithEmptyLog20() throws Exception
    {
        CommitLog.instance.recover(tmpFile(CommitLogDescriptor.VERSION_20));
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
    public void testRecoveryWithShortSize() throws Exception
    {
        runExpecting(() -> {
            testRecovery(new byte[2], CommitLogDescriptor.VERSION_20);
            return null;
        }, CommitLogReplayException.class);
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
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        ColumnFamilyStore cfs2 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD2);

        // Roughly 32 MB mutation
        Mutation m = new RowUpdateBuilder(cfs1.metadata, 0, "k")
                     .clustering("bytes")
                     .add("val", ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 4))
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

        assertEquals(2, CommitLog.instance.activeSegments());

        UUID cfid2 = m2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segments
        assertEquals(2, CommitLog.instance.activeSegments());
    }

    @Test
    public void testDeleteIfNotDirty() throws Exception
    {
        DatabaseDescriptor.getCommitLogSegmentSize();
        CommitLog.instance.resetUnsafe(true);
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

        assertEquals(1, CommitLog.instance.activeSegments());

        // "Flush": this won't delete anything
        UUID cfid1 = rm.getColumnFamilyIds().iterator().next();
        CommitLog.instance.sync(true);
        CommitLog.instance.discardCompletedSegments(cfid1, CommitLog.instance.getContext());

        assertEquals(1, CommitLog.instance.activeSegments());

        // Adding new mutation on another CF, large enough (including CL entry overhead) that a new segment is created
        Mutation rm2 = new RowUpdateBuilder(cfs2.metadata, 0, "k")
                       .clustering("bytes")
                       .add("val", ByteBuffer.allocate(DatabaseDescriptor.getMaxMutationSize() - 200))
                       .build();
        CommitLog.instance.add(rm2);
        // also forces a new segment, since each entry-with-overhead is just under half the CL size
        CommitLog.instance.add(rm2);
        CommitLog.instance.add(rm2);

        assertEquals(3, CommitLog.instance.activeSegments());

        // "Flush" second cf: The first segment should be deleted since we
        // didn't write anything on cf1 since last flush (and we flush cf2)

        UUID cfid2 = rm2.getColumnFamilyIds().iterator().next();
        CommitLog.instance.discardCompletedSegments(cfid2, CommitLog.instance.getContext());

        // Assert we still have both our segment
        assertEquals(1, CommitLog.instance.activeSegments());
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
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation rm = new RowUpdateBuilder(cfs.metadata, 0, "k")
                      .clustering("bytes")
                      .add("val", ByteBuffer.allocate(getMaxRecordDataSize()))
                      .build();
        CommitLog.instance.add(rm);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceedRecordLimit() throws Exception
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation rm = new RowUpdateBuilder(cfs.metadata, 0, "k")
                      .clustering("bytes")
                      .add("val", ByteBuffer.allocate(1 + getMaxRecordDataSize()))
                      .build();
        CommitLog.instance.add(rm);
        throw new AssertionError("mutation larger than limit was accepted");
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

    /**
     * Create a temporary commit log file with an appropriate descriptor at the head.
     *
     * @return the commit log file reference and the first position after the descriptor in the file
     * (so that subsequent writes happen at the correct file location).
     */
    protected Pair<File, Integer> tmpFile() throws IOException
    {
        EncryptionContext encryptionContext = DatabaseDescriptor.getEncryptionContext();
        CommitLogDescriptor desc = new CommitLogDescriptor(CommitLogDescriptor.current_version,
                                                           CommitLogSegment.getNextId(),
                                                           DatabaseDescriptor.getCommitLogCompression(),
                                                           encryptionContext);

        // if we're testing encryption, we need to write out a cipher IV to the descriptor headers
        Map<String, String> additionalHeaders = new HashMap<>();
        if (encryptionContext.isEnabled())
        {
            byte[] buf = new byte[16];
            new Random().nextBytes(buf);
            additionalHeaders.put(EncryptionContext.ENCRYPTION_IV, Hex.bytesToHex(buf));
        }

        ByteBuffer buf = ByteBuffer.allocate(1024);
        CommitLogDescriptor.writeHeader(buf, desc, additionalHeaders);
        buf.flip();
        int positionAfterHeader = buf.limit() + 1;

        File logFile = new File(logDirectory, desc.fileName());
        logFile.deleteOnExit();

        try (OutputStream lout = new FileOutputStream(logFile))
        {
            lout.write(buf.array(), 0, buf.limit());
        }

        return Pair.create(logFile, positionAfterHeader);
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
        desc = new CommitLogDescriptor(desc.version, fromFile.id, desc.compression, desc.getEncryptionContext());
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
        CommitLogDescriptor desc = new CommitLogDescriptor(4, null, EncryptionContextGenerator.createDisabledContext());
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
        CommitLogDescriptor desc = new CommitLogDescriptor(4, new ParameterizedClass("UnknownCompressor", null), EncryptionContextGenerator.createDisabledContext());
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
        runExpecting(() -> testRecovery(new CommitLogDescriptor(4, null, EncryptionContextGenerator.createDisabledContext()), logData), expected);
    }

    protected void testRecovery(byte[] logData) throws Exception
    {
        Pair<File, Integer> pair = tmpFile();
        try (RandomAccessFile raf = new RandomAccessFile(pair.left, "rw"))
        {
            raf.seek(pair.right);
            raf.write(logData);
            raf.close();

            CommitLog.instance.recover(pair.left); //CASSANDRA-1119 / CASSANDRA-1179 throw on failure*/
        }
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
                    CommitLog.instance.discardCompletedSegments(syscfs.metadata.cfId, position);
            CommitLog.instance.discardCompletedSegments(cfs2.metadata.cfId, position);
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
        CommitLog.instance.resetUnsafe(true);
        boolean originalState = DatabaseDescriptor.getAutoSnapshot();
        try
        {
            DatabaseDescriptor.setAutoSnapshot(false);
            Keyspace notDurableKs = Keyspace.open(KEYSPACE2);
            Assert.assertFalse(notDurableKs.getMetadata().params.durableWrites);

            ColumnFamilyStore cfs = notDurableKs.getColumnFamilyStore("Standard1");
            new RowUpdateBuilder(cfs.metadata, 0, "key1")
            .clustering("bytes").add("val", bytes("abcd"))
            .build()
            .applyUnsafe();

            assertTrue(Util.getOnlyRow(Util.cmd(cfs).columns("val").build())
                           .cells().iterator().next().value().equals(bytes("abcd")));

            cfs.truncateBlocking();

            Util.assertEmpty(Util.cmd(cfs).columns("val").build());
        }
        finally
        {
            DatabaseDescriptor.setAutoSnapshot(originalState);
        }
    }

    @Test
    public void replay_StandardMmapped() throws IOException
    {
        ParameterizedClass originalCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext originalEncryptionContext = DatabaseDescriptor.getEncryptionContext();
        try
        {
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createDisabledContext());
            CommitLog.instance.resetUnsafe(true);
            replaySimple(CommitLog.instance);
            replayWithDiscard(CommitLog.instance);
        }
        finally
        {
            DatabaseDescriptor.setCommitLogCompression(originalCompression);
            DatabaseDescriptor.setEncryptionContext(originalEncryptionContext);
            CommitLog.instance.resetUnsafe(true);
        }
    }

    @Test
    public void replay_Compressed_LZ4() throws IOException
    {
        replay_Compressed(new ParameterizedClass(LZ4Compressor.class.getName(), Collections.<String, String>emptyMap()));
    }

    @Test
    public void replay_Compressed_Snappy() throws IOException
    {
        replay_Compressed(new ParameterizedClass(SnappyCompressor.class.getName(), Collections.<String, String>emptyMap()));
    }

    @Test
    public void replay_Compressed_Deflate() throws IOException
    {
        replay_Compressed(new ParameterizedClass(DeflateCompressor.class.getName(), Collections.<String, String>emptyMap()));
    }

    private void replay_Compressed(ParameterizedClass parameterizedClass) throws IOException
    {
        ParameterizedClass originalCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext originalEncryptionContext = DatabaseDescriptor.getEncryptionContext();
        try
        {
            DatabaseDescriptor.setCommitLogCompression(parameterizedClass);
            DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createDisabledContext());
            CommitLog.instance.resetUnsafe(true);

            replaySimple(CommitLog.instance);
            replayWithDiscard(CommitLog.instance);
        }
        finally
        {
            DatabaseDescriptor.setCommitLogCompression(originalCompression);
            DatabaseDescriptor.setEncryptionContext(originalEncryptionContext);
            CommitLog.instance.resetUnsafe(true);
        }
    }

    @Test
    public void replay_Encrypted() throws IOException
    {
        ParameterizedClass originalCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext originalEncryptionContext = DatabaseDescriptor.getEncryptionContext();
        try
        {
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(EncryptionContextGenerator.createContext(true));
            CommitLog.instance.resetUnsafe(true);

            replaySimple(CommitLog.instance);
            replayWithDiscard(CommitLog.instance);
        }
        finally
        {
            DatabaseDescriptor.setCommitLogCompression(originalCompression);
            DatabaseDescriptor.setEncryptionContext(originalEncryptionContext);
            CommitLog.instance.resetUnsafe(true);
        }
    }

    private void replaySimple(CommitLog commitLog) throws IOException
    {
        int cellCount = 0;
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        final Mutation rm1 = new RowUpdateBuilder(cfs.metadata, 0, "k1")
                             .clustering("bytes")
                             .add("val", bytes("this is a string"))
                             .build();
        cellCount += 1;
        commitLog.add(rm1);

        final Mutation rm2 = new RowUpdateBuilder(cfs.metadata, 0, "k2")
                             .clustering("bytes")
                             .add("val", bytes("this is a string"))
                             .build();
        cellCount += 1;
        commitLog.add(rm2);

        commitLog.sync(true);

        Replayer replayer = new Replayer(commitLog, ReplayPosition.NONE);
        List<String> activeSegments = commitLog.getActiveSegmentNames();
        Assert.assertFalse(activeSegments.isEmpty());

        File[] files = new File(commitLog.location).listFiles((file, name) -> activeSegments.contains(name));
        replayer.recover(files);

        assertEquals(cellCount, replayer.cells);
    }

    private void replayWithDiscard(CommitLog commitLog) throws IOException
    {
        int cellCount = 0;
        int max = 1024;
        int discardPosition = (int)(max * .8); // an arbitrary number of entries that we'll skip on the replay
        ReplayPosition replayPosition = null;
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        for (int i = 0; i < max; i++)
        {
            final Mutation rm1 = new RowUpdateBuilder(cfs.metadata, 0, "k" + 1)
                                 .clustering("bytes")
                                 .add("val", bytes("this is a string"))
                                 .build();
            ReplayPosition position = commitLog.add(rm1);

            if (i == discardPosition)
                replayPosition = position;
            if (i > discardPosition)
            {
                cellCount += 1;
            }
        }

        commitLog.sync(true);

        Replayer replayer = new Replayer(commitLog, replayPosition);
        List<String> activeSegments = commitLog.getActiveSegmentNames();
        Assert.assertFalse(activeSegments.isEmpty());

        File[] files = new File(commitLog.location).listFiles((file, name) -> activeSegments.contains(name));
        replayer.recover(files);

        assertEquals(cellCount, replayer.cells);
    }

    class Replayer extends CommitLogReplayer
    {
        private final ReplayPosition filterPosition;
        int cells;
        int skipped;

        Replayer(CommitLog commitLog, ReplayPosition filterPosition)
        {
            super(commitLog, filterPosition, null, ReplayFilter.create());
            this.filterPosition = filterPosition;
        }

        @SuppressWarnings("resource")
        void replayMutation(byte[] inputBuffer, int size, final long entryLocation, final CommitLogDescriptor desc) throws IOException
        {
            if (entryLocation <= filterPosition.position)
            {
                // Skip over this mutation.
                skipped++;
                return;
            }

            FastByteArrayInputStream bufIn = new FastByteArrayInputStream(inputBuffer, 0, size);
            Mutation mutation = Mutation.serializer.deserialize(new DataInputPlus.DataInputStreamPlus(bufIn), desc.getMessagingVersion(), SerializationHelper.Flag.LOCAL);
            for (PartitionUpdate partitionUpdate : mutation.getPartitionUpdates())
                for (Row row : partitionUpdate)
                    cells += Iterables.size(row.cells());
        }
    }
}

