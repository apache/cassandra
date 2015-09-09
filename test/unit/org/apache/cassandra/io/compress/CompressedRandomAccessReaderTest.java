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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Random;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;

import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.CompressedPoolingSegmentedFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.SyncUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompressedRandomAccessReaderTest
{
    @Test
    public void testResetAndTruncate() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(File.createTempFile("normal", "1"), false, 10);
        testResetAndTruncate(File.createTempFile("normal", "2"), false, CompressionParameters.DEFAULT_CHUNK_LENGTH);
    }

    @Test
    public void testResetAndTruncateCompressed() throws IOException
    {
        // test reset in current buffer or previous one
        testResetAndTruncate(File.createTempFile("compressed", "1"), true, 10);
        testResetAndTruncate(File.createTempFile("compressed", "2"), true, CompressionParameters.DEFAULT_CHUNK_LENGTH);
    }

    @Test
    public void test6791() throws IOException, ConfigurationException
    {
        File f = File.createTempFile("compressed6791_", "3");
        String filename = f.getAbsolutePath();
        ChannelProxy channel = new ChannelProxy(f);
        try
        {

            MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
            CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata", new CompressionParameters(SnappyCompressor.instance, 32, Collections.<String, String>emptyMap()), sstableMetadataCollector);

            for (int i = 0; i < 20; i++)
                writer.write("x".getBytes());

            FileMark mark = writer.mark();
            // write enough garbage to create new chunks:
            for (int i = 0; i < 40; ++i)
                writer.write("y".getBytes());

            writer.resetAndTruncate(mark);

            for (int i = 0; i < 20; i++)
                writer.write("x".getBytes());
            writer.finish();

            CompressedRandomAccessReader reader = CompressedRandomAccessReader.open(channel, new CompressionMetadata(filename + ".metadata", f.length()));
            String res = reader.readLine();
            assertEquals(res, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
            assertEquals(40, res.length());
        }
        finally
        {
            // cleanup
            channel.close();

            if (f.exists())
                f.delete();
            File metadata = new File(filename+ ".metadata");
                if (metadata.exists())
                    metadata.delete();
        }
    }

    private void testResetAndTruncate(File f, boolean compressed, int junkSize) throws IOException
    {
        final String filename = f.getAbsolutePath();
        ChannelProxy channel = new ChannelProxy(f);
        try
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
            SequentialWriter writer = compressed
                ? new CompressedSequentialWriter(f, filename + ".metadata", new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector)
                : SequentialWriter.open(f);

            writer.write("The quick ".getBytes());
            FileMark mark = writer.mark();
            writer.write("blue fox jumps over the lazy dog".getBytes());

            // write enough to be sure to change chunk
            for (int i = 0; i < junkSize; ++i)
            {
                writer.write((byte)1);
            }

            writer.resetAndTruncate(mark);
            writer.write("brown fox jumps over the lazy dog".getBytes());
            writer.finish();

            assert f.exists();
            RandomAccessReader reader = compressed
                                      ? CompressedRandomAccessReader.open(channel, new CompressionMetadata(filename + ".metadata", f.length()))
                                      : RandomAccessReader.open(f);
            String expected = "The quick brown fox jumps over the lazy dog";
            assertEquals(expected.length(), reader.length());
            byte[] b = new byte[expected.length()];
            reader.readFully(b);
            assert new String(b).equals(expected) : "Expecting '" + expected + "', got '" + new String(b) + "'";
        }
        finally
        {
            // cleanup
            channel.close();

            if (f.exists())
                f.delete();
            File metadata = new File(filename + ".metadata");
            if (compressed && metadata.exists())
                metadata.delete();
        }
    }

    @Test
    public void testDataCorruptionDetection() throws IOException
    {
        String CONTENT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vitae.";

        File file = new File("testDataCorruptionDetection");
        file.deleteOnExit();

        File metadata = new File(file.getPath() + ".meta");
        metadata.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
        try (SequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(), new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector))
        {
            writer.write(CONTENT.getBytes());
            writer.finish();
        }

        ChannelProxy channel = new ChannelProxy(file);

        // open compression metadata and get chunk information
        CompressionMetadata meta = new CompressionMetadata(metadata.getPath(), file.length());
        CompressionMetadata.Chunk chunk = meta.chunkFor(0);

        RandomAccessReader reader = CompressedRandomAccessReader.open(channel, meta);
        // read and verify compressed data
        assertEquals(CONTENT, reader.readLine());

        Random random = new Random();
        RandomAccessFile checksumModifier = null;

        try
        {
            checksumModifier = new RandomAccessFile(file, "rw");
            byte[] checksum = new byte[4];

            // seek to the end of the compressed chunk
            checksumModifier.seek(chunk.length);
            // read checksum bytes
            checksumModifier.read(checksum);
            // seek back to the chunk end
            checksumModifier.seek(chunk.length);

            // lets modify one byte of the checksum on each iteration
            for (int i = 0; i < checksum.length; i++)
            {
                checksumModifier.write(random.nextInt());
                SyncUtil.sync(checksumModifier); // making sure that change was synced with disk

                final RandomAccessReader r = CompressedRandomAccessReader.open(channel, meta);

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
                assertEquals(exception.getClass(), CorruptSSTableException.class);
                assertEquals(exception.getCause().getClass(), CorruptBlockException.class);

                r.close();
            }

            // lets write original checksum and check if we can read data
            updateChecksum(checksumModifier, chunk.length, checksum);

            reader = CompressedRandomAccessReader.open(channel, meta);
            // read and verify compressed data
            assertEquals(CONTENT, reader.readLine());
            // close reader
            reader.close();
        }
        finally
        {
            channel.close();

            if (checksumModifier != null)
                checksumModifier.close();
        }
    }

    @Test
    public void testThrottledReadersAreNotCached() throws IOException
    {
        String CONTENT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam vitae.";

        File file = new File("testThrottledReadersAreNotCached");
        file.deleteOnExit();

        File metadata = new File(file.getPath() + ".meta");
        metadata.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
        try (SequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(), new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector))
        {
            writer.write(CONTENT.getBytes());
            writer.finish();
        }

        CompressionMetadata meta = new CompressionMetadata(metadata.getPath(), file.length());

        try(ChannelProxy channel = new ChannelProxy(file);
            CompressedPoolingSegmentedFile segmentedFile = new CompressedPoolingSegmentedFile(channel, meta))
        {
            //The cache bucket is only initialized by a call to FileCacheService.instance.get() so first
            // we must create a reader using the interface for accessing segments
            FileDataInput reader = segmentedFile.getSegment(0);
            assertNotNull(reader);
            reader.close();

            //Now we create a throttled reader, this should not be added to the cache
            RateLimiter limiter = RateLimiter.create(1024);
            reader = segmentedFile.createThrottledReader(limiter);
            assertNotNull(reader);
            assertTrue(reader instanceof CompressedThrottledReader);
            reader.close();

            //We retrieve 2 readers, neither should be a throttled reader
            FileDataInput[] readers =
            {
                segmentedFile.getSegment(0),
                segmentedFile.getSegment(0)
            };

            for (FileDataInput r : readers)
            {
                assertNotNull(r);
                assertFalse(r instanceof CompressedThrottledReader);
            }

            for (FileDataInput r : readers)
                r.close();
        }
    }

    private void updateChecksum(RandomAccessFile file, long checksumOffset, byte[] checksum) throws IOException
    {
        file.seek(checksumOffset);
        file.write(checksum);
        SyncUtil.sync(file);
    }
}
