/**
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
package org.apache.cassandra.io.compress;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.MmappedRegions;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.SyncUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompressedRandomAccessReaderTest
{
    @Test
    public void testResetAndTruncate() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(File.createTempFile("normal", "1"), false, false, 10);
        testResetAndTruncate(File.createTempFile("normal", "2"), false, false, CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    @Test
    public void testResetAndTruncateCompressed() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(File.createTempFile("compressed", "1"), true, false, 10);
        testResetAndTruncate(File.createTempFile("compressed", "2"), true, false, CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    @Test
    public void testResetAndTruncateCompressedMmap() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(File.createTempFile("compressed_mmap", "1"), true, true, 10);
        testResetAndTruncate(File.createTempFile("compressed_mmap", "2"), true, true, CompressionParams.DEFAULT_CHUNK_LENGTH);
    }

    @Test
    public void test6791() throws IOException, ConfigurationException
    {
        File f = File.createTempFile("compressed6791_", "3");
        String filename = f.getAbsolutePath();
        try (ChannelProxy channel = new ChannelProxy(f))
        {

            MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata", CompressionParams.snappy(32), sstableMetadataCollector))
            {

                for (int i = 0; i < 20; i++)
                    writer.write("x".getBytes());

                DataPosition mark = writer.mark();
                // write enough garbage to create new chunks:
                for (int i = 0; i < 40; ++i)
                    writer.write("y".getBytes());

                writer.resetAndTruncate(mark);

                for (int i = 0; i < 20; i++)
                    writer.write("x".getBytes());
                writer.finish();
            }

            try (RandomAccessReader reader = new CompressedRandomAccessReader.Builder(channel,
                                                                                      new CompressionMetadata(filename + ".metadata", f.length(), ChecksumType.CRC32))
                    .build())
            {
                String res = reader.readLine();
                assertEquals(res, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                assertEquals(40, res.length());
            }
        }
        finally
        {
            if (f.exists())
                assertTrue(f.delete());
            File metadata = new File(filename + ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }

    /**
     * JIRA: CASSANDRA-15595 verify large position with small chunk length won't overflow chunk index
     */
    @Test
    public void testChunkIndexOverflow() throws IOException
    {
        File file = File.createTempFile("chunk_idx_overflow", "1");
        String filename = file.getAbsolutePath();
        int chunkLength = 4096; // 4k

        try
        {
            writeSSTable(file, CompressionParams.snappy(chunkLength), 10);
            CompressionMetadata metadata = new CompressionMetadata(filename + ".metadata", file.length(), ChecksumType.CRC32);

            long chunks = 2761628520L;
            long midPosition = (chunks / 2L) * chunkLength;
            int idx = 8 * (int) (midPosition / chunkLength); // before patch
            assertTrue("Expect integer overflow", idx < 0);

            try
            {
                metadata.chunkFor(midPosition);
                fail("Expected to throw EOF exception with chunk idx larger than total number of chunks in the sstable");
            }
            catch (CorruptSSTableException e)
            {
                assertTrue("Expect EOF, but got " + e.getCause(), e.getCause() instanceof EOFException);
            }
        }
        finally
        {
            if (file.exists())
                assertTrue(file.delete());
            File metadata = new File(filename + ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }

    private static void testResetAndTruncate(File f, boolean compressed, boolean usemmap, int junkSize) throws IOException
    {
        final String filename = f.getAbsolutePath();
        try(ChannelProxy channel = new ChannelProxy(f))
        {
            writeSSTable(f, compressed ? CompressionParams.snappy() : null, junkSize);

            CompressionMetadata compressionMetadata = compressed ? new CompressionMetadata(filename + ".metadata", f.length(), ChecksumType.CRC32) : null;
            RandomAccessReader.Builder builder = compressed
                                                 ? new CompressedRandomAccessReader.Builder(channel, compressionMetadata)
                                                 : new RandomAccessReader.Builder(channel);

            if (usemmap)
            {
                if (compressed)
                    builder.regions(MmappedRegions.map(channel, compressionMetadata));
                else
                    builder.regions(MmappedRegions.map(channel, f.length()));
            }

            try(RandomAccessReader reader = builder.build())
            {
                String expected = "The quick brown fox jumps over the lazy dog";
                assertEquals(expected.length(), reader.length());
                byte[] b = new byte[expected.length()];
                reader.readFully(b);
                assert new String(b).equals(expected) : "Expecting '" + expected + "', got '" + new String(b) + '\'';
            }

            if (usemmap)
                builder.regions.close();
        }
        finally
        {
            if (f.exists())
                assertTrue(f.delete());
            File metadata = new File(filename + ".metadata");
            if (compressed && metadata.exists())
                metadata.delete();
        }
    }

    private static void writeSSTable(File f, CompressionParams params, int junkSize) throws IOException
    {
        final String filename = f.getAbsolutePath();
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try(SequentialWriter writer = params != null
                                      ? new CompressedSequentialWriter(f, filename + ".metadata", params, sstableMetadataCollector)
                                      : SequentialWriter.open(f))
        {
            writer.write("The quick ".getBytes());
            DataPosition mark = writer.mark();
            writer.write("blue fox jumps over the lazy dog".getBytes());

            // write enough to be sure to change chunk
            for (int i = 0; i < junkSize; ++i)
            {
                writer.write((byte) 1);
            }

            writer.resetAndTruncate(mark);
            writer.write("brown fox jumps over the lazy dog".getBytes());
            writer.finish();
        }
        assert f.exists();
    }

    /**
     * If the data read out doesn't match the checksum, an exception should be thrown
     */
    @Test
    public void testDataCorruptionDetection() throws IOException
    {
        String CONTENT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vitae.";

        File file = new File("testDataCorruptionDetection");
        file.deleteOnExit();

        File metadata = new File(file.getPath() + ".meta");
        metadata.deleteOnExit();

        assertTrue(file.createNewFile());
        assertTrue(metadata.createNewFile());

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (SequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(), CompressionParams.snappy(), sstableMetadataCollector))
        {
            writer.write(CONTENT.getBytes());
            writer.finish();
        }

        try(ChannelProxy channel = new ChannelProxy(file))
        {
            // open compression metadata and get chunk information
            CompressionMetadata meta = new CompressionMetadata(metadata.getPath(), file.length(), ChecksumType.CRC32);
            CompressionMetadata.Chunk chunk = meta.chunkFor(0);

            try(RandomAccessReader reader = new CompressedRandomAccessReader.Builder(channel, meta).build())
            {// read and verify compressed data
                assertEquals(CONTENT, reader.readLine());
            }

            Random random = new Random();
            try(RandomAccessFile checksumModifier = new RandomAccessFile(file, "rw"))
            {
                byte[] checksum = new byte[4];

                // seek to the end of the compressed chunk
                checksumModifier.seek(chunk.length);
                // read checksum bytes
                checksumModifier.read(checksum);

                byte[] corruptChecksum = new byte[4];
                do
                {
                    random.nextBytes(corruptChecksum);
                } while (Arrays.equals(corruptChecksum, checksum));

                updateChecksum(checksumModifier, chunk.length, corruptChecksum);

                try (final RandomAccessReader r = new CompressedRandomAccessReader.Builder(channel, meta).build())
                {
                    Throwable exception = null;
                    try
                    {
                        r.readLine();
                    }
                    catch (Throwable t)
                    {
                        exception = t;
                    }
                    assertNotNull(exception);
                    assertSame(exception.getClass(), CorruptSSTableException.class);
                    assertSame(exception.getCause().getClass(), CorruptBlockException.class);
                }

                // lets write original checksum and check if we can read data
                updateChecksum(checksumModifier, chunk.length, checksum);

                // read and verify compressed data
                try (RandomAccessReader cr = new CompressedRandomAccessReader.Builder(channel, meta).build())
                {
                    assertEquals(CONTENT, cr.readLine());
                }
            }
        }
    }

    private static void updateChecksum(RandomAccessFile file, long checksumOffset, byte[] checksum) throws IOException
    {
        file.seek(checksumOffset);
        file.write(checksum);
        SyncUtil.sync(file);
    }
}
