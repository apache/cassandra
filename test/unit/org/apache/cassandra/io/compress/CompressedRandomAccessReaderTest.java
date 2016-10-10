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
package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.SyncUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompressedRandomAccessReaderTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

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
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try(CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata",
                                                                               null, SequentialWriterOption.DEFAULT,
                                                                               CompressionParams.snappy(32),
                                                                               sstableMetadataCollector))
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

        try (FileHandle.Builder builder = new FileHandle.Builder(filename)
                                                              .withCompressionMetadata(new CompressionMetadata(filename + ".metadata",
                                                                                                               f.length(),
                                                                                                               ChecksumType.CRC32));
             FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            String res = reader.readLine();
            assertEquals(res, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            assertEquals(40, res.length());
        }
        finally
        {
            if (f.exists())
                assertTrue(f.delete());
            File metadata = new File(filename+ ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }

    private static void testResetAndTruncate(File f, boolean compressed, boolean usemmap, int junkSize) throws IOException
    {
        final String filename = f.getAbsolutePath();
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try(SequentialWriter writer = compressed
                ? new CompressedSequentialWriter(f, filename + ".metadata",
                null, SequentialWriterOption.DEFAULT,
                CompressionParams.snappy(), sstableMetadataCollector)
                : new SequentialWriter(f))
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

        CompressionMetadata compressionMetadata = compressed ? new CompressionMetadata(filename + ".metadata", f.length(), ChecksumType.CRC32) : null;
        try (FileHandle.Builder builder = new FileHandle.Builder(filename).mmapped(usemmap).withCompressionMetadata(compressionMetadata);
             FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            String expected = "The quick brown fox jumps over the lazy dog";
            assertEquals(expected.length(), reader.length());
            byte[] b = new byte[expected.length()];
            reader.readFully(b);
            assert new String(b).equals(expected) : "Expecting '" + expected + "', got '" + new String(b) + '\'';
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
        try (SequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(),
                                                                      null, SequentialWriterOption.DEFAULT,
                                                                      CompressionParams.snappy(), sstableMetadataCollector))
        {
            writer.write(CONTENT.getBytes());
            writer.finish();
        }

        // open compression metadata and get chunk information
        CompressionMetadata meta = new CompressionMetadata(metadata.getPath(), file.length(), ChecksumType.CRC32);
        CompressionMetadata.Chunk chunk = meta.chunkFor(0);

        try (FileHandle.Builder builder = new FileHandle.Builder(file.getPath()).withCompressionMetadata(meta);
             FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {// read and verify compressed data
            assertEquals(CONTENT, reader.readLine());
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

                try (final RandomAccessReader r = fh.createReader())
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
                try (RandomAccessReader cr = fh.createReader())
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
