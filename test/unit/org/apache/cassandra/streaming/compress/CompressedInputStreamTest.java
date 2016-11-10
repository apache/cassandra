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
package org.apache.cassandra.streaming.compress;

import java.io.*;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 */
public class CompressedInputStreamTest
{
    @Test
    public void testCompressedRead() throws Exception
    {
        testCompressedReadWith(new long[]{0L}, false, false);
        testCompressedReadWith(new long[]{1L}, false, false);
        testCompressedReadWith(new long[]{100L}, false, false);

        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, false);
    }

    @Test(expected = EOFException.class)
    public void testTruncatedRead() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, true, false);
    }

    /**
     * Test that CompressedInputStream does not block if there's an exception while reading stream
     */
    @Test(timeout = 30000)
    public void testException() throws Exception
    {
        testCompressedReadWith(new long[]{1L, 122L, 123L, 124L, 456L}, false, true);
    }

    /**
     * @param valuesToCheck array of longs of range(0-999)
     * @throws Exception
     */
    private void testCompressedReadWith(long[] valuesToCheck, boolean testTruncate, boolean testException) throws Exception
    {
        assert valuesToCheck != null && valuesToCheck.length > 0;

        // write compressed data file of longs
        File tmp = new File(File.createTempFile("cassandra", "unittest").getParent(), "ks-cf-ib-1-Data.db");
        Descriptor desc = Descriptor.fromFilename(tmp.getAbsolutePath());
        MetadataCollector collector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
        CompressionParameters param = new CompressionParameters(SnappyCompressor.instance, 32, Collections.<String, String>emptyMap());
        Map<Long, Long> index = new HashMap<>();
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(tmp, desc.filenameFor(Component.COMPRESSION_INFO), param, collector))
        {
            for (long l = 0L; l < 1000; l++)
            {
                index.put(l, writer.getFilePointer());
                writer.stream.writeLong(l);
            }
            writer.finish();
        }

        CompressionMetadata comp = CompressionMetadata.create(tmp.getAbsolutePath());
        List<Pair<Long, Long>> sections = new ArrayList<>();
        for (long l : valuesToCheck)
        {
            long position = index.get(l);
            sections.add(Pair.create(position, position + 8));
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

        try (RandomAccessFile f = new RandomAccessFile(tmp, "r"))
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
        CompressionInfo info = new CompressionInfo(chunks, param);

        if (testException)
        {
            testException(sections, info);
            return;
        }
        CompressedInputStream input = new CompressedInputStream(new ByteArrayInputStream(toRead), info);

        try (DataInputStream in = new DataInputStream(input))
        {
            for (int i = 0; i < sections.size(); i++)
            {
                input.position(sections.get(i).left);
                long readValue = in.readLong();
                assertEquals("expected " + valuesToCheck[i] + " but was " + readValue, valuesToCheck[i], readValue);
            }
        }
    }

    private static void testException(List<Pair<Long, Long>> sections, CompressionInfo info) throws IOException
    {
        CompressedInputStream input = new CompressedInputStream(new ByteArrayInputStream(new byte[0]), info);

        try (DataInputStream in = new DataInputStream(input))
        {
            for (int i = 0; i < sections.size(); i++)
            {
                input.position(sections.get(i).left);
                try {
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
