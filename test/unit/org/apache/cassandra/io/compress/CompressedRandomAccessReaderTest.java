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

import java.io.*;
import java.util.Random;
import java.util.concurrent.Callable;

import org.junit.Test;

import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.util.*;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.Util.expectException;

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

    private void testResetAndTruncate(File f, boolean compressed, int junkSize) throws IOException
    {
        final String filename = f.getAbsolutePath();

        try
        {
            SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(null);
            SequentialWriter writer = compressed
                ? new CompressedSequentialWriter(f, filename + ".metadata", false, new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector)
                : new SequentialWriter(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);

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
            writer.close();

            assert f.exists();
            RandomAccessReader reader = compressed
                ? new CompressedRandomAccessReader(filename, new CompressionMetadata(filename + ".metadata", f.length()), false)
                : new RandomAccessReader(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);
            String expected = "The quick brown fox jumps over the lazy dog";
            assertEquals(expected.length(), reader.length());
            byte[] b = new byte[expected.length()];
            reader.readFully(b);
            assert new String(b).equals(expected) : "Expecting '" + expected + "', got '" + new String(b) + "'";
        }
        finally
        {
            // cleanup
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

        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(null);
        SequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(), false, new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector);

        writer.write(CONTENT.getBytes());
        writer.close();

        // open compression metadata and get chunk information
        CompressionMetadata meta = new CompressionMetadata(metadata.getPath(), file.length());
        CompressionMetadata.Chunk chunk = meta.chunkFor(0);

        RandomAccessReader reader = CompressedRandomAccessReader.open(file.getPath(), meta, false);
        // read and verify compressed data
        assertEquals(CONTENT, reader.readLine());
        // close reader
        reader.close();

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
                checksumModifier.getFD().sync(); // making sure that change was synced with disk

                final RandomAccessReader r = CompressedRandomAccessReader.open(file.getPath(), meta, false);

                expectException(new Callable<String>()
                {
                    public String call() throws Exception
                    {
                        return r.readLine();
                    }
                }, CorruptedBlockException.class);

                r.close();
            }

            // lets write original checksum and check if we can read data
            updateChecksum(checksumModifier, chunk.length, checksum);

            reader = CompressedRandomAccessReader.open(file.getPath(), meta, false);
            // read and verify compressed data
            assertEquals(CONTENT, reader.readLine());
            // close reader
            reader.close();
        }
        finally
        {
            if (checksumModifier != null)
                checksumModifier.close();
        }
    }

    private void updateChecksum(RandomAccessFile file, long checksumOffset, byte[] checksum) throws IOException
    {
        file.seek(checksumOffset);
        file.write(checksum);
        file.getFD().sync();
    }
}
