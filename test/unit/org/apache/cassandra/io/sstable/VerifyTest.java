/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.sstable.format.SortedTableVerifier.RangeOwnHelper;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.reads.range.TokenUpdater;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.OutputHandler;

import static org.apache.cassandra.SchemaLoader.counterCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for {@link IVerifier}.
 * <p>
 * Note: the complete coverage is composed of:
 * - {@link org.apache.cassandra.tools.StandaloneVerifierOnSSTablesTest}
 * - {@link org.apache.cassandra.tools.StandaloneVerifierTest}
 * - {@link VerifyTest}
 */
public class VerifyTest
{
    private final static Logger logger = LoggerFactory.getLogger(VerifyTest.class);

    public static final String KEYSPACE = "Keyspace1";
    public static final String CF = "Standard1";
    public static final String CF2 = "Standard2";
    public static final String CF3 = "Standard3";
    public static final String CF4 = "Standard4";
    public static final String COUNTER_CF = "Counter1";
    public static final String COUNTER_CF2 = "Counter2";
    public static final String COUNTER_CF3 = "Counter3";
    public static final String COUNTER_CF4 = "Counter4";
    public static final String CORRUPT_CF = "Corrupt1";
    public static final String CORRUPT_CF2 = "Corrupt2";
    public static final String CORRUPT_CF3 = "Corrupt3";
    public static final String CORRUPTCOUNTER_CF = "CounterCorrupt1";
    public static final String CORRUPTCOUNTER_CF2 = "CounterCorrupt2";

    public static final String CF_UUID = "UUIDKeys";
    public static final String BF_ALWAYS_PRESENT = "BfAlwaysPresent";
    /** Fixture for the dead prefix bound; see {@link #deadPrefixBound}. */
    public static final String CF_DEAD_PREFIX = "Standard5";

    /**
     * Compression chunk length of {@link #CF_DEAD_PREFIX}, which is the bound the verifier computes: small enough
     * that a fixture of {@link #DEAD_PREFIX_FIXTURE_PARTITIONS} partitions has index entries on both sides of it,
     * and pinned here rather than taken from the defaults so the two tests below straddle a known number.
     */
    private static final int DEAD_PREFIX_CHUNK_LENGTH = 4096;
    private static final int DEAD_PREFIX_FIXTURE_PARTITIONS = 600;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CompressionParams compressionParameters = CompressionParams.snappy(32768);
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setColumnIndexSizeInKiB(0);

        loadSchema();
        createKeyspace(KEYSPACE,
                       KeyspaceParams.simple(1),
                       standardCFMD(KEYSPACE, CF).compression(compressionParameters),
                       standardCFMD(KEYSPACE, CF2).compression(compressionParameters),
                       standardCFMD(KEYSPACE, CF3),
                       standardCFMD(KEYSPACE, CF4),
                       standardCFMD(KEYSPACE, CF_DEAD_PREFIX).compression(CompressionParams.snappy(DEAD_PREFIX_CHUNK_LENGTH)),
                       standardCFMD(KEYSPACE, CORRUPT_CF),
                       standardCFMD(KEYSPACE, CORRUPT_CF2),
                       standardCFMD(KEYSPACE, CORRUPT_CF3),
                       counterCFMD(KEYSPACE, COUNTER_CF).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF2).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF3),
                       counterCFMD(KEYSPACE, COUNTER_CF4),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF2),
                       standardCFMD(KEYSPACE, CF_UUID, 0, UUIDType.instance),
                       standardCFMD(KEYSPACE, BF_ALWAYS_PRESENT).bloomFilterFpChance(1.0));
    }

    protected IVerifier getVerifier(SSTableReader sstable, ColumnFamilyStore cfs, IVerifier.Options.Builder verifierOptions)
    {
        return sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, verifierOptions.build());
    }

    @Test
    public void testVerifyCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF2);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true).extendedVerification(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF3);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF3);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF4);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().extendedVerification(true).invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrectUncompressed()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF4);

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().extendedVerification(true).invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            err.printStackTrace();
            fail("Unexpected CorruptSSTableException");
        }
    }


    @Test
    public void testVerifyIncorrectDigest() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();


        try (RandomAccessReader file = RandomAccessReader.open(sstable.descriptor.fileFor(Components.DIGEST)))
        {
            long correctChecksum = Long.parseLong(Objects.requireNonNull(file.readLine()));

            writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(false)))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (RuntimeException expected)
        {
        }
    }


    @Test
    public void testVerifyCorruptRowCorrectDigest() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // overwrite one row with garbage
        long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("0"), cfs.getPartitioner()), SSTableReader.Operator.EQ);
        long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("1"), cfs.getPartitioner()), SSTableReader.Operator.EQ);
        long startPosition = Math.min(row0Start, row1Start);
        long endPosition = Math.max(row0Start, row1Start);

        try (FileChannel file = new File(sstable.getFilename()).newReadWriteChannel()) {
            file.position(startPosition);
            file.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        }
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(sstable.getFilename());

        // Update the Digest to have the right Checksum
        writeChecksum(simpleFullChecksum(sstable.getFilename()), sstable.descriptor.fileFor(Components.DIGEST));

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            // First a simple verify checking digest, which should succeed
            try
            {
                verifier.verify();
            }
            catch (CorruptSSTableException err)
            {
                logger.error("Unexpected exception", err);
                fail("Simple verify should have succeeded as digest matched");
            }
        }
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true).extendedVerification(true)))
        {
            // Now try extended verify
            try
            {
                verifier.verify();
            }
            catch (CorruptSSTableException err)
            {
                return;
            }
            fail("Expected a CorruptSSTableException to be thrown");
        }
    }

    @Test
    public void testVerifyBrokenSSTableMetadata() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);
        cfs.truncateBlocking();
        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        FileChannel file = sstable.descriptor.fileFor(Components.STATS).newReadWriteChannel();
        file.position(0);
        file.write(ByteBufferUtil.bytes(StringUtils.repeat('z', 2)));
        file.close();
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(false)))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (CorruptSSTableException unexpected)
        {
            fail("wrong exception thrown");
        }
        catch (RuntimeException expected)
        {
        }
    }

    @Test
    public void testVerifyMutateRepairStatus() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);
        cfs.truncateBlocking();
        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        // make the sstable repaired:
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, System.currentTimeMillis(), sstable.getPendingRepair(), sstable.isTransient());
        sstable.reloadSSTableMetadata();

        // break the sstable:
        long correctChecksum;
        try (RandomAccessReader file = RandomAccessReader.open(sstable.descriptor.fileFor(Components.DIGEST)))
        {
            correctChecksum = Long.parseLong(Objects.requireNonNull(file.readLine()));
        }
        writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().mutateRepairStatus(false).invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        assertTrue(sstable.isRepaired());

        // now the repair status should be changed:
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().mutateRepairStatus(true).invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }
        assertFalse(sstable.isRepaired());
    }

    /**
     * A non-zero first index position is only tolerated as a DEAD PREFIX while it could be one: a Data.db assembled
     * from cell-aligned byte ranges of a larger one carries the head of its FIRST cell and no more, so a position at
     * or past one cell has to keep failing. It must fail as the full {@code markAndThrow}: a
     * CorruptSSTableException AND the repaired status cleared, since the whole point of the second half is that a
     * plain incremental repair afterwards fetches the partitions this sstable can no longer be trusted for.
     * <p>
     * Nothing else covers the "First row position from index != 0" path -- the accepted side is covered by
     * {@code ZeroCopySSTableSplitterTest.verifierAndScrubberAcceptAChildWithADeadPrefix} -- and a verifier that
     * stopped checking here would walk from the position and report success while leaving every partition before it
     * unread. Here that is 100 or so partitions, none of which the digest can distinguish from good ones because
     * Digest.crc32 covers the whole file either way.
     */
    @Test
    public void testExtendedVerifyRejectsFirstIndexPositionPastDeadPrefixBound() throws IOException
    {
        Assume.assumeTrue(BigFormat.isSelected());

        ColumnFamilyStore cfs = deadPrefixFixture();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        List<ScrubTest.IndexEntry> entries = ScrubTest.readIndexEntries(sstable.descriptor);
        int firstPastBound = firstEntryAtOrPast(entries, deadPrefixBound(sstable));

        SSTableReader patched = reopenWithIndexPrefixDropped(cfs, sstable, entries.get(firstPastBound));
        long firstPosition = ScrubTest.readIndexEntries(patched.descriptor).get(0).dataPosition;
        assertTrue("the fixture must open at or past the bound, not at " + firstPosition,
                   firstPosition >= deadPrefixBound(patched));
        makeRepaired(patched);

        try (IVerifier verifier = getVerifier(patched, cfs, IVerifier.options()
                                                                    .extendedVerification(true)
                                                                    .mutateRepairStatus(true)
                                                                    .invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException for a first index position of " + firstPosition +
                 ", which is past the " + deadPrefixBound(patched) + " byte dead prefix bound");
        }
        catch (CorruptSSTableException expected)
        {
            // Otherwise this passes on any corruption at all, including one this fixture introduced by accident.
            // NOTE: dropping the leading entries moves the index's first KEY as well as its first position, and
            // BigTableVerifier.deserializeIndex checks the key against the data file's first partition BEFORE the
            // data walk reaches the position check -- so the guard that actually fires here is "First partition does
            // not match index". That is a real protection and worth pinning, but it means this test does NOT reach
            // SortedTableVerifier's "First row position from index != 0" branch. Reaching that one needs a surgery
            // that rewrites entry 0's position vint while leaving its key alone (see
            // ScrubTest.rewriteIndexEntryPosition); until such a test exists, that branch is unverified.
            String trace = Throwables.getStackTraceAsString(expected);
            assertTrue("the failure must be one of the index guards and not something else: " + trace,
                       trace.contains("First partition does not match index")
                       || trace.contains("First row position from index != 0"));
        }

        assertFalse("markAndThrow must also have cleared the repaired status", patched.isRepaired());
    }

    // There was an "accepts a first index position INSIDE the bound" test here, meant to pin the bound from below.
    // It cannot be written with this surgery: dropping leading Index.db entries moves the first KEY, so
    // BigTableVerifier.deserializeIndex's "First partition does not match index" check rejects the sstable whether the
    // position is inside the bound or past it, and the test could only ever have passed by accident. The accepted side
    // of the boundary is covered where a REAL dead prefix exists rather than a tampered one --
    // ZeroCopySSTableSplitterTest.verifierAndScrubberAcceptAChildWithADeadPrefix and
    // ZeroCopySSTableSliceBtiTest.sliceAndVerify -- which is the stronger test anyway. Pinning the bound itself from
    // below needs a position-only rewrite of entry 0 (ScrubTest.rewriteIndexEntryPosition).

    /**
     * One sstable of {@link #DEAD_PREFIX_FIXTURE_PARTITIONS} partitions in {@link #CF_DEAD_PREFIX}, whose Data.db
     * spans several compression chunks so that {@link #deadPrefixBound} is the chunk length rather than the whole
     * file.
     */
    private ColumnFamilyStore deadPrefixFixture()
    {
        CompactionManager.instance.disableAutoCompaction();
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_DEAD_PREFIX);
        cfs.truncateBlocking();
        fillCF(cfs, DEAD_PREFIX_FIXTURE_PARTITIONS);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        assertTrue("the fixture must span several chunks, but Data.db is " + sstable.uncompressedLength() + " bytes",
                   sstable.uncompressedLength() > 2L * DEAD_PREFIX_CHUNK_LENGTH);
        return cfs;
    }

    /**
     * What {@code SortedTableVerifier.deadPrefixLimit()} computes for a compressed sstable: one cell, capped by the
     * data length. Kept as a mirror rather than exposed from production, so a change to the bound shows up here as a
     * failing test rather than as two tests that silently stop straddling anything.
     */
    private static long deadPrefixBound(SSTableReader sstable)
    {
        assertTrue("the dead prefix fixture must be compressed for the bound to be the chunk length",
                   sstable.compression);
        return Math.min(sstable.getCompressionMetadata().chunkLength(), sstable.uncompressedLength());
    }

    private static int firstEntryAtOrPast(List<ScrubTest.IndexEntry> entries, long position)
    {
        for (int i = 0; i < entries.size(); i++)
        {
            if (entries.get(i).dataPosition >= position)
                return i;
        }
        throw new AssertionError("no partition starts at or past " + position + " in " + entries.size() + " entries");
    }

    /**
     * Drop every Index.db entry before {@code keepFrom}, which leaves an sstable shaped exactly like a zero-copy
     * split child: a Data.db whose first bytes no index entry describes, and an index whose first entry is a real
     * partition, so the walk that starts there agrees with it about every key and position.
     * <p>
     * The sstable has to be reopened, and this is why: dropping entries changes the length of Index.db, the reader
     * has it open with that length captured (and mapped, under {@code disk_access_mode: mmap_index_only}), so a
     * reader from before the rewrite would read the wrong number of bytes and fail somewhere else entirely. The view
     * is reset and the reader released first because a released reader closes its handles; neither deletes any file,
     * which is what the reopen needs.
     */
    private static SSTableReader reopenWithIndexPrefixDropped(ColumnFamilyStore cfs,
                                                              SSTableReader sstable,
                                                              ScrubTest.IndexEntry keepFrom) throws IOException
    {
        Descriptor descriptor = sstable.descriptor;
        File indexFile = descriptor.fileFor(Components.PRIMARY_INDEX);
        byte[] index = Files.readAllBytes(indexFile.toPath());
        assertTrue("nothing would be dropped", keepFrom.offset > 0 && keepFrom.offset < index.length);

        cfs.clearUnsafe();
        sstable.selfRef().release();

        Files.write(indexFile.toPath(), Arrays.copyOfRange(index, (int) keepFrom.offset, index.length));
        if (ChunkCache.instance != null)
            ChunkCache.instance.invalidateFile(indexFile.toString());

        SSTableReader reopened = SSTableReader.open(cfs, descriptor, cfs.metadata);
        // Re-adding an sstable the Tracker has already seen would try to hardlink it into backups/ a second time,
        // which Tracker.maybeIncrementallyBackup answers with "Tried to create duplicate hard link". The sstable is
        // the same one on disk under the same generation -- only its Index.db changed -- so there is nothing new to
        // back up; suppress it for the re-add and restore whatever the surrounding suite had set.
        boolean incrementalBackups = DatabaseDescriptor.isIncrementalBackupsEnabled();
        DatabaseDescriptor.setIncrementalBackupsEnabled(false);
        try
        {
            cfs.addSSTable(reopened);
        }
        finally
        {
            DatabaseDescriptor.setIncrementalBackupsEnabled(incrementalBackups);
        }
        return reopened;
    }

    private static void makeRepaired(SSTableReader sstable) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor,
                                                                       1,
                                                                       sstable.getPendingRepair(),
                                                                       sstable.isTransient());
        sstable.reloadSSTableMetadata();
        assertTrue(sstable.isRepaired());
    }

    @Test(expected = RuntimeException.class)
    public void testOutOfRangeTokens() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        fillCF(cfs, 100);
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        new TokenUpdater().withTokens(InetAddressAndPort.getByName("127.0.0.1"), new BytesToken(tk1))
                          .withTokens(InetAddressAndPort.getByName("127.0.0.2"), new BytesToken(tk2))
                          .update();

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().checkOwnsTokens(true).extendedVerification(true)))
        {
            verifier.verify();
        }
    }

    @Test
    public void testMutateRepair() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF3);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, 1, sstable.getPendingRepair(), sstable.isTransient());
        sstable.reloadSSTableMetadata();
        cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
        assertTrue(sstable.isRepaired());
        cfs.forceMajorCompaction();

        sstable = cfs.getLiveSSTables().iterator().next();
        long correctChecksum;
        try (RandomAccessReader file = RandomAccessReader.open(sstable.descriptor.fileFor(Components.DIGEST)))
        {
            correctChecksum = Long.parseLong(Objects.requireNonNull(file.readLine()));
        }
        writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true).mutateRepairStatus(true)))
        {
            verifier.verify();
            fail("should be corrupt");
        }
        catch (CorruptSSTableException expected)
        {
        }
        assertFalse(sstable.isRepaired());
    }

    @Test
    public void testVerifyIndex() throws IOException
    {
        if (BigFormat.isSelected())
            testBrokenComponentHelper(BigFormat.Components.PRIMARY_INDEX);
        else if (BtiFormat.isSelected())
            testBrokenComponentHelper(BtiFormat.Components.PARTITION_INDEX);
        else
            throw Util.testMustBeImplementedForSSTableFormat();
    }

    @Test
    public void testVerifyBf() throws IOException
    {
        Assume.assumeTrue(SSTableReaderWithFilter.class.isAssignableFrom(DatabaseDescriptor.getSelectedSSTableFormat().getReaderFactory().getReaderClass()));
        testBrokenComponentHelper(Components.FILTER);
    }

    @Test
    public void testVerifyIndexSummary() throws IOException
    {
        Assume.assumeTrue(BigFormat.isSelected());
        testBrokenComponentHelper(Components.SUMMARY);
    }

    private void testBrokenComponentHelper(Component componentToBreak) throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options()))
        {
            verifier.verify(); //still not corrupt, should pass
        }
        try (FileChannel fileChannel = sstable.descriptor.fileFor(componentToBreak).newReadWriteChannel())
        {
            fileChannel.truncate(3);
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("should throw exception");
        }
        catch (CorruptSSTableException e)
        {
            //expected
        }
    }

    @Test
    public void testQuick() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF);

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();


        try (RandomAccessReader file = RandomAccessReader.open(sstable.descriptor.fileFor(Components.DIGEST)))
        {
            long correctChecksum = Long.parseLong(Objects.requireNonNull(file.readLine()));

            writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true).quick(true))) // with quick = true we don't verify the digest
        {
            verifier.verify();
        }

        try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().invokeDiskFailurePolicy(true)))
        {
            verifier.verify();
            fail("Expected a RuntimeException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }
    }

    @Test
    public void testRangeOwnHelper()
    {
        List<Range<Token>> normalized = new ArrayList<>();
        normalized.add(r(Long.MIN_VALUE, Long.MIN_VALUE + 1));
        normalized.add(r(Long.MIN_VALUE + 5, Long.MIN_VALUE + 6));
        normalized.add(r(Long.MIN_VALUE + 10, Long.MIN_VALUE + 11));
        normalized.add(r(0, 10));
        normalized.add(r(10, 11));
        normalized.add(r(20, 25));
        normalized.add(r(26, 200));

        RangeOwnHelper roh = new RangeOwnHelper(normalized);

        roh.validate(dk(1));
        roh.validate(dk(10));
        roh.validate(dk(11));
        roh.validate(dk(21));
        roh.validate(dk(25));
        boolean gotException = false;
        try
        {
            roh.validate(dk(26));
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
    }

    @Test(expected = AssertionError.class)
    public void testRangeOwnHelperBadToken()
    {
        List<Range<Token>> normalized = new ArrayList<>();
        normalized.add(r(0, 10));
        RangeOwnHelper roh = new RangeOwnHelper(normalized);
        roh.validate(dk(1));
        // call with smaller token to get exception
        roh.validate(dk(0));
    }


    @Test
    public void testRangeOwnHelperNormalize()
    {
        List<Range<Token>> normalized = Range.normalize(Collections.singletonList(r(0, 0)));
        RangeOwnHelper roh = new RangeOwnHelper(normalized);
        roh.validate(dk(Long.MIN_VALUE));
        roh.validate(dk(0));
        roh.validate(dk(Long.MAX_VALUE));
    }

    @Test
    public void testRangeOwnHelperNormalizeWrap()
    {
        List<Range<Token>> normalized = Range.normalize(Collections.singletonList(r(Long.MAX_VALUE - 1000, Long.MIN_VALUE + 1000)));
        RangeOwnHelper roh = new RangeOwnHelper(normalized);
        roh.validate(dk(Long.MIN_VALUE));
        roh.validate(dk(Long.MAX_VALUE));
        boolean gotException = false;
        try
        {
            roh.validate(dk(26));
        }
        catch (Throwable t)
        {
            gotException = true;
        }
        assertTrue(gotException);
    }

    @Test
    public void testEmptyRanges()
    {
        new RangeOwnHelper(Collections.emptyList()).validate(dk(1));
    }

    @Test
    public void testVerifyLocalPartitioner() throws UnknownHostException
    {
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
//        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
//        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));
        // write some bogus to a localpartitioner table
        Batch bogus = Batch.createLocal(nextTimeUUID(), 0, Collections.emptyList());
        BatchlogManager.store(bogus);
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("batches");
        Util.flush(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {

            try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options().checkOwnsTokens(true)))
            {
                verifier.verify();
            }
        }
    }

    @Test
    public void testNoFilterFile()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(BF_ALWAYS_PRESENT);
        fillCF(cfs, 100);
        assertEquals(1.0, cfs.metadata().params.bloomFilterFpChance, 0.0);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File f = sstable.descriptor.fileFor(Components.FILTER);
            assertFalse(f.exists());
            try (IVerifier verifier = getVerifier(sstable, cfs, IVerifier.options()))
            {
                verifier.verify();
            }
        }
    }

    @Test
    public void testVerifyReversedPartitioner()
    {
        for (long i = 0; i < 10; i++)
            QueryProcessor.executeInternal("insert into system.local_metadata_log (epoch) values (?)", i);
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("local_metadata_log");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        assertFalse(cfs.getLiveSSTables().isEmpty());
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options()
                                                                                                              .checkOwnsTokens(true).build()))
            {
                verifier.verify();
            }
        }
    }

    private DecoratedKey dk(long l)
    {
        return new BufferDecoratedKey(t(l), ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    private Range<Token> r(long s, long e)
    {
        return new Range<>(t(s), t(e));
    }

    private Token t(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }


    protected void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                         .newRow("c1").add("val", "1")
                         .newRow("c2").add("val", "2")
                         .apply();
        }

        Util.flush(cfs);
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, int partitionsPerSSTable) throws WriteTimeoutException
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata(), String.valueOf(i))
                         .newRow("c1").add("val", 100L)
                         .apply();
        }

        Util.flush(cfs);
    }

    protected long simpleFullChecksum(String filename) throws IOException
    {
        try (FileInputStreamPlus inputStream = new FileInputStreamPlus(filename);
             CheckedInputStream cinStream = new CheckedInputStream(inputStream, new CRC32()))
        {
            byte[] b = new byte[128];
            //noinspection StatementWithEmptyBody
            while (cinStream.read(b) >= 0)
            {
            }
            return cinStream.getChecksum().getValue();
        }
    }

    public static void writeChecksum(long checksum, File file)
    {
        BufferedWriter out = null;
        try
        {
            out = Files.newBufferedWriter(file.toPath(), Charsets.UTF_8);
            out.write(String.valueOf(checksum));
            out.flush();
            out.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }
    }
}
