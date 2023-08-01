/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming.compression;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.streaming.CompressedInputStream;
import org.apache.cassandra.db.streaming.CompressionInfo;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.CompressionInfoComponent;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ChecksumType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 */
public class CompressedInputStreamTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCompressedRead() throws Exception
    {
        testCompressedReadWith(new long[]{0L}, false, false, 0);
        testCompressedReadWith(new long[]{1L}, false, false, 0);
        testCompressedReadWith(new long[]{100L}, false, false, 0);

        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, false, 0);
    }

    @Test(expected = EOFException.class)
    public void testTruncatedRead() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, true, false, 0);
    }

    /**
     * Test that CompressedInputStream does not block if there's an exception while reading stream
     */
    @Test(timeout = 30000)
    public void testException() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, true, 0);
    }

    @Test
    public void testCompressedReadUncompressedChunks() throws Exception
    {
        testCompressedReadWith(new long[]{0L}, false, false, 3);
        testCompressedReadWith(new long[]{1L}, false, false, 3);
        testCompressedReadWith(new long[]{100L}, false, false, 3);

        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, false, 3);
    }

    @Test(expected = EOFException.class)
    public void testTruncatedReadUncompressedChunks() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, true, false, 3);
    }

    @Test(timeout = 30000)
    public void testCorruptedReadUncompressedChunks() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, true, 3);
    }

    /**
     * @param valuesToCheck array of longs of range(0-999)
     * @throws Exception
     */
    private void testCompressedReadWith(long[] valuesToCheck, boolean testTruncate, boolean testException, double minCompressRatio) throws Exception
    {
        assert valuesToCheck != null && valuesToCheck.length > 0;

        // write compressed data file of longs
        File parentDir = new File(tempFolder.newFolder());
        Descriptor desc = new Descriptor(parentDir, "ks", "cf", new SequenceBasedSSTableId(1));
        File tmp = desc.fileFor(Components.DATA);
        MetadataCollector collector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        CompressionParams param = CompressionParams.snappy(32, minCompressRatio);
        Map<Long, Long> index = new HashMap<Long, Long>();
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(tmp,
                                                                                desc.fileFor(Components.COMPRESSION_INFO),
                                                                                null,
                                                                                SequentialWriterOption.DEFAULT,
                                                                                param, collector))
        {
            for (long l = 0L; l < 1000; l++)
            {
                index.put(l, writer.position());
                writer.writeLong(l);
            }
            writer.finish();
        }

        CompressionMetadata comp = CompressionInfoComponent.load(desc);
        List<SSTableReader.PartitionPositionBounds> sections = new ArrayList<>();
        for (long l : valuesToCheck)
        {
            long position = index.get(l);
            sections.add(new SSTableReader.PartitionPositionBounds(position, position + 8));
        }
        CompressionMetadata.Chunk[] chunks = comp.getChunksForSections(sections);
        long totalSize = comp.getTotalSizeForSections(sections);
        long expectedSize = 0;
        for (CompressionMetadata.Chunk c : chunks)
            expectedSize += c.length + 4;
        assertEquals(expectedSize, totalSize);

        // buffer up only relevant parts of file
        int size = 0;
        for (CompressionMetadata.Chunk c : chunks)
            size += (c.length + 4); // 4bytes CRC
        byte[] toRead = new byte[size];

        try (RandomAccessReader f = RandomAccessReader.open(tmp))
        {
            int pos = 0;
            for (CompressionMetadata.Chunk c : chunks)
            {
                f.seek(c.offset);
                pos += f.read(toRead, pos, c.length + 4);
            }
        }

        if (testTruncate)
        {
            byte [] actuallyRead = new byte[50];
            System.arraycopy(toRead, 0, actuallyRead, 0, 50);
            toRead = actuallyRead;
        }

        // read buffer using CompressedInputStream
        CompressionInfo info = CompressionInfo.newInstance(chunks, param);

        if (testException)
        {
            testException(sections, info);
            return;
        }
        CompressedInputStream input = new CompressedInputStream(new DataInputBuffer(toRead), info, ChecksumType.CRC32, () -> 1.0);

        try (DataInputStream in = new DataInputStream(input))
        {
            for (int i = 0; i < sections.size(); i++)
            {
                input.position(sections.get(i).lowerPosition);
                long readValue = in.readLong();
                assertEquals("expected " + valuesToCheck[i] + " but was " + readValue, valuesToCheck[i], readValue);
            }
        }
    }

    private static void testException(List<SSTableReader.PartitionPositionBounds> sections, CompressionInfo info) throws IOException
    {
        CompressedInputStream input = new CompressedInputStream(new DataInputBuffer(new byte[0]), info, ChecksumType.CRC32, () -> 1.0);

        try (DataInputStream in = new DataInputStream(input))
        {
            for (int i = 0; i < sections.size(); i++)
            {
                try {
                    input.position(sections.get(i).lowerPosition);
                    in.readLong();
                    fail("Should have thrown IOException");
                }
                catch (IOException e)
                {
                    continue;
                }
            }
        }
    }
}
