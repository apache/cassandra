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
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import com.google.common.base.Charsets;
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
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
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
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
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
    public static final String CORRUPTCOUNTER_CF = "CounterCorrupt1";
    public static final String CORRUPTCOUNTER_CF2 = "CounterCorrupt2";

    public static final String CF_UUID = "UUIDKeys";
    public static final String BF_ALWAYS_PRESENT = "BfAlwaysPresent";

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
                       standardCFMD(KEYSPACE, CORRUPT_CF),
                       standardCFMD(KEYSPACE, CORRUPT_CF2),
                       counterCFMD(KEYSPACE, COUNTER_CF).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF2).compression(compressionParameters),
                       counterCFMD(KEYSPACE, COUNTER_CF3),
                       counterCFMD(KEYSPACE, COUNTER_CF4),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF),
                       counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF2),
                       standardCFMD(KEYSPACE, CF_UUID, 0, UUIDType.instance),
                       standardCFMD(KEYSPACE, BF_ALWAYS_PRESENT).bloomFilterFpChance(1.0));
    }


    @Test
    public void testVerifyCorrect()
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).extendedVerification(true).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().extendedVerification(true).invokeDiskFailurePolicy(true).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().extendedVerification(true).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
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
            long correctChecksum = file.readLong();

            writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(false).build()))
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

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).extendedVerification(true).build()))
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
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(false).build()))
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
            correctChecksum = file.readLong();
        }
        writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().mutateRepairStatus(false).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        assertTrue(sstable.isRepaired());

        // now the repair status should be changed:
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().mutateRepairStatus(true).invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }
        assertFalse(sstable.isRepaired());
    }

    @Test(expected = RuntimeException.class)
    public void testOutOfRangeTokens() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);
        fillCF(cfs, 100);
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkOwnsTokens(true).extendedVerification(true).build()))
        {
            verifier.verify();
        }
        finally
        {
            StorageService.instance.getTokenMetadata().clearUnsafe();
        }
    }

    @Test
    public void testMutateRepair() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

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
            correctChecksum = file.readLong();
        }
        writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).mutateRepairStatus(true).build()))
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
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().build()))
        {
            verifier.verify(); //still not corrupt, should pass
        }
        try (FileChannel fileChannel = sstable.descriptor.fileFor(componentToBreak).newReadWriteChannel())
        {
            fileChannel.truncate(3);
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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
            long correctChecksum = file.readLong();

            writeChecksum(++correctChecksum, sstable.descriptor.fileFor(Components.DIGEST));
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException expected)
        {
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).quick(true).build())) // with quick = true we don't verify the digest
        {
            verifier.verify();
        }

        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
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
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk1), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(new ByteOrderedPartitioner.BytesToken(tk2), InetAddressAndPort.getByName("127.0.0.2"));
        // write some bogus to a localpartitioner table
        Batch bogus = Batch.createLocal(nextTimeUUID(), 0, Collections.emptyList());
        BatchlogManager.store(bogus);
        ColumnFamilyStore cfs = Keyspace.open("system").getColumnFamilyStore("batches");
        Util.flush(cfs);
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {

            try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().checkOwnsTokens(true).build()))
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
            try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().build()))
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
