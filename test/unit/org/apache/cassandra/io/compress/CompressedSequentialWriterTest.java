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
package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.FileMark;

public class CompressedSequentialWriterTest
{
    private ICompressor compressor;

    private void runTests(String testName) throws IOException, ConfigurationException
    {
        // Test small < 1 chunk data set
        testWrite(File.createTempFile(testName + "_small", "1"), 25);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(File.createTempFile(testName + "_chunkAligned", "1"), CompressionParameters.DEFAULT_CHUNK_LENGTH);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(File.createTempFile(testName + "_large", "1"), CompressionParameters.DEFAULT_CHUNK_LENGTH * 3 + 100);
    }

    @Test
    public void testLZ4Writer() throws IOException, ConfigurationException
    {
        compressor = LZ4Compressor.instance;
        runTests("LZ4");
    }

    @Test
    public void testDeflateWriter() throws IOException, ConfigurationException
    {
        compressor = DeflateCompressor.instance;
        runTests("Deflate");
    }

    @Test
    public void testSnappyWriter() throws IOException, ConfigurationException
    {
        compressor = SnappyCompressor.instance;
        runTests("Snappy");
    }

    private void testWrite(File f, int bytesToTest) throws IOException, ConfigurationException
    {
        try
        {
            final String filename = f.getAbsolutePath();

            MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance)).replayPosition(null);
            CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata", new CompressionParameters(compressor), sstableMetadataCollector);

            byte[] dataPre = new byte[bytesToTest];
            byte[] rawPost = new byte[bytesToTest];
            Random r = new Random();

            // Test both write with byte[] and ByteBuffer
            r.nextBytes(dataPre);
            r.nextBytes(rawPost);

            writer.write(dataPre);
            FileMark mark = writer.mark();

            // Write enough garbage to transition chunk
            for (int i = 0; i < CompressionParameters.DEFAULT_CHUNK_LENGTH; i++)
            {
                writer.write((byte)i);
            }
            writer.resetAndTruncate(mark);
            writer.write(rawPost);
            writer.close();

            assert f.exists();
            CompressedRandomAccessReader reader = CompressedRandomAccessReader.open(filename, new CompressionMetadata(filename + ".metadata", f.length(), true));
            assertEquals(dataPre.length + rawPost.length, reader.length());
            byte[] result = new byte[(int)reader.length()];

            reader.readFully(result);

            assert(reader.isEOF());
            reader.close();

            byte[] fullInput = new byte[bytesToTest * 2];
            System.arraycopy(dataPre, 0, fullInput, 0, dataPre.length);
            System.arraycopy(rawPost, 0, fullInput, bytesToTest, rawPost.length);
            assert Arrays.equals(result, fullInput);
        }
        finally
        {
            // cleanup
            if (f.exists())
                f.delete();
            File metadata = new File(f + ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }
}
