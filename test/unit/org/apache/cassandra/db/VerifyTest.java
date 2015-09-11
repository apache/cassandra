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
package org.apache.cassandra.db;

import com.google.common.base.Charsets;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.*;
import java.nio.file.Files;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

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
        CompressionParams compressionParameters = CompressionParams.snappy(32768);

        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF).compression(compressionParameters),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF2).compression(compressionParameters),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF3),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF4),
                                    SchemaLoader.standardCFMD(KEYSPACE, CORRUPT_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, CORRUPT_CF2),
                                    SchemaLoader.counterCFMD(KEYSPACE, COUNTER_CF).compression(compressionParameters),
                                    SchemaLoader.counterCFMD(KEYSPACE, COUNTER_CF2).compression(compressionParameters),
                                    SchemaLoader.counterCFMD(KEYSPACE, COUNTER_CF3),
                                    SchemaLoader.counterCFMD(KEYSPACE, COUNTER_CF4),
                                    SchemaLoader.counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF),
                                    SchemaLoader.counterCFMD(KEYSPACE, CORRUPTCOUNTER_CF2),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_UUID, 0, UUIDType.instance));
    }


    @Test
    public void testVerifyCorrect() throws IOException
    {
        CompactionManager.instance.disableAutoCompaction();
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCounterCF(cfs, 2);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();


        RandomAccessFile file = new RandomAccessFile(sstable.descriptor.filenameFor(sstable.descriptor.digestComponent), "rw");
        Long correctChecksum = Long.parseLong(file.readLine());
        file.close();

        writeChecksum(++correctChecksum, sstable.descriptor.filenameFor(sstable.descriptor.digestComponent));

        try (Verifier verifier = new Verifier(cfs, sstable, false))
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

        fillCF(cfs, 2);

        Util.getAll(Util.cmd(cfs).build());

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // overwrite one row with garbage
        long row0Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("0"), cfs.getPartitioner()), SSTableReader.Operator.EQ).position;
        long row1Start = sstable.getPosition(PartitionPosition.ForKey.get(ByteBufferUtil.bytes("1"), cfs.getPartitioner()), SSTableReader.Operator.EQ).position;
        long startPosition = row0Start < row1Start ? row0Start : row1Start;
        long endPosition = row0Start < row1Start ? row1Start : row0Start;

        RandomAccessFile file = new RandomAccessFile(sstable.getFilename(), "rw");
        file.seek(startPosition);
        file.writeBytes(StringUtils.repeat('z', (int) 2));
        file.close();

        // Update the Digest to have the right Checksum
        writeChecksum(simpleFullChecksum(sstable.getFilename()), sstable.descriptor.filenameFor(sstable.descriptor.digestComponent));

        try (Verifier verifier = new Verifier(cfs, sstable, false))
        {
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

    }

    protected void fillCF(ColumnFamilyStore cfs, int partitionsPerSSTable)
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata, String.valueOf(i))
                         .newRow("c1").add("val", "1")
                         .newRow("c2").add("val", "2")
                         .apply();
        }

        cfs.forceBlockingFlush();
    }

    protected void fillCounterCF(ColumnFamilyStore cfs, int partitionsPerSSTable) throws WriteTimeoutException
    {
        for (int i = 0; i < partitionsPerSSTable; i++)
        {
            UpdateBuilder.create(cfs.metadata, String.valueOf(i))
                         .newRow("c1").add("val", 100L)
                         .apply();
        }

        cfs.forceBlockingFlush();
    }

    protected long simpleFullChecksum(String filename) throws IOException
    {
        FileInputStream inputStream = new FileInputStream(filename);
        CRC32 checksum = new CRC32();
        CheckedInputStream cinStream = new CheckedInputStream(inputStream, checksum);
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
