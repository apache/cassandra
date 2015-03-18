package org.apache.cassandra.db;
/*
 *
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
 *
 */

import com.google.common.base.Charsets;
import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.Util.column;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class VerifyTest
{
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

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CompressionParameters compressionParameters = new CompressionParameters(SnappyCompressor.instance, 32768, new HashMap<String, String>());

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF).compressionParameters(compressionParameters),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF2).compressionParameters(compressionParameters),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF3),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF4),
                                    SchemaLoader.standardCFMD(KEYSPACE, CORRUPT_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, CORRUPT_CF2),
                                    CFMetaData.denseCFMetaData(KEYSPACE, COUNTER_CF, BytesType.instance).defaultValidator(CounterColumnType.instance).compressionParameters(compressionParameters),
                                    CFMetaData.denseCFMetaData(KEYSPACE, COUNTER_CF2, BytesType.instance).defaultValidator(CounterColumnType.instance).compressionParameters(compressionParameters),
                                    CFMetaData.denseCFMetaData(KEYSPACE, COUNTER_CF3, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE, COUNTER_CF4, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE, CORRUPTCOUNTER_CF, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    CFMetaData.denseCFMetaData(KEYSPACE, CORRUPTCOUNTER_CF2, BytesType.instance).defaultValidator(CounterColumnType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_UUID).keyValidator(UUIDType.instance));
    }


    @Test
    public void testVerifyCorrect() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, KEYSPACE, CF, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(false);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrect() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);

        fillCounterCF(cfs, KEYSPACE, COUNTER_CF, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(false);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrect() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF2);

        fillCF(cfs, KEYSPACE, CF2, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(true);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrect() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF2);

        fillCounterCF(cfs, KEYSPACE, COUNTER_CF2, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(true);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCorrectUncompressed() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF3);

        fillCF(cfs, KEYSPACE, CF3, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(false);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testVerifyCounterCorrectUncompressed() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF3);

        fillCounterCF(cfs, KEYSPACE, COUNTER_CF3, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(false);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCorrectUncompressed() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF4);

        fillCF(cfs, KEYSPACE, CF4, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(true);
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException");
        }
    }

    @Test
    public void testExtendedVerifyCounterCorrectUncompressed() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF4);

        fillCounterCF(cfs, KEYSPACE, COUNTER_CF4, 2);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(true);
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

        fillCF(cfs, KEYSPACE, CORRUPT_CF, 2);

        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);

        SSTableReader sstable = cfs.getSSTables().iterator().next();


        RandomAccessFile file = new RandomAccessFile(sstable.descriptor.filenameFor(Component.DIGEST), "rw");
        Long correctChecksum = Long.parseLong(file.readLine());
        file.close();

        writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(Component.DIGEST));

        Verifier verifier = new Verifier(cfs, sstable, false);
        try
        {
            verifier.verify(false);
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err) {}
    }


    @Test
    public void testVerifyCorruptRowCorrectDigest() throws IOException, WriteTimeoutException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CORRUPT_CF2);

        fillCF(cfs, KEYSPACE, CORRUPT_CF2, 2);

        List<Row> rows = cfs.getRangeSlice(Util.range("", ""), null, new IdentityQueryFilter(), 1000);

        SSTableReader sstable = cfs.getSSTables().iterator().next();

        // overwrite one row with garbage
        long row0Start = sstable.getPosition(RowPosition.ForKey.get(ByteBufferUtil.bytes("0"), sstable.partitioner), SSTableReader.Operator.EQ).position;
        long row1Start = sstable.getPosition(RowPosition.ForKey.get(ByteBufferUtil.bytes("1"), sstable.partitioner), SSTableReader.Operator.EQ).position;
        long startPosition = row0Start < row1Start ? row0Start : row1Start;
        long endPosition = row0Start < row1Start ? row1Start : row0Start;

        RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw");
        file.seek(startPosition);
        file.writeBytes(StringUtils.repeat('z', (int) 2));
        file.close();

        // Update the Digest to have the right Checksum
        writeChecksum(simpleFullChecksum(sstable.getFilename()), sstable.descriptor.filenameFor(Component.DIGEST));

        Verifier verifier = new Verifier(cfs, sstable, false);

        // First a simple verify checking digest, which should succeed
        try
        {
            verifier.verify(false);
        }
        catch (CorruptSSTableException err)
        {
            fail("Simple verify should have succeeded as digest matched");
        }

        // Now try extended verify
        try
        {
            verifier.verify(true);

        }
        catch (CorruptSSTableException err)
        {
            return;
        }
        fail("Expected a CorruptSSTableException to be thrown");

    }

    protected void fillCF(ColumnFamilyStore cfs, String keyspace, String columnFamily, int rowsPerSSTable)
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(keyspace, columnFamily);
            cf.addColumn(column("c1", "1", 1L));
            cf.addColumn(column("c2", "2", 1L));
            Mutation rm = new Mutation(keyspace, ByteBufferUtil.bytes(key), cf);
            rm.apply();
        }

        cfs.forceBlockingFlush();
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, String keyspace, String columnFamily, int rowsPerSSTable) throws WriteTimeoutException
    {
        for (int i = 0; i < rowsPerSSTable; i++)
        {
            String key = String.valueOf(i);
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(keyspace, columnFamily);
            Mutation rm = new Mutation(keyspace, ByteBufferUtil.bytes(key), cf);
            rm.addCounter(columnFamily, cellname("Column1"), 100);
            CounterMutation cm = new CounterMutation(rm, ConsistencyLevel.ONE);
            cm.apply();
        }

        cfs.forceBlockingFlush();
    }

    protected long simpleFullChecksum(String filename) throws IOException
    {
        FileInputStream inputStream = new FileInputStream(filename);
        Adler32 adlerChecksum = new Adler32();
        CheckedInputStream cinStream = new CheckedInputStream(inputStream, adlerChecksum);
        byte[] b = new byte[128];
        while (cinStream.read(b) >= 0) {
        }
        return cinStream.getChecksum().getValue();
    }

    protected void writeChecksum(long checksum, String filePath)
    {
        File outFile = new File(filePath);
        BufferedWriter out = null;
        try
        {
            out = Files.newBufferedWriter(outFile.toPath(), Charsets.UTF_8);
            out.write(String.valueOf(checksum));
            out.flush();
            out.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, outFile);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }

    }

}
