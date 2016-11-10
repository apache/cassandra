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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.commons.io.FileUtils.readFileToByteArray;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterTest;

public class CompressedSequentialWriterTest extends SequentialWriterTest
{
    private ICompressor compressor;

    private void runTests(String testName) throws IOException
    {
        // Test small < 1 chunk data set
        testWrite(File.createTempFile(testName + "_small", "1"), 25);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(File.createTempFile(testName + "_chunkAligned", "1"), CompressionParameters.DEFAULT_CHUNK_LENGTH);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(File.createTempFile(testName + "_large", "1"), CompressionParameters.DEFAULT_CHUNK_LENGTH * 3 + 100);
    }

    @Test
    public void testLZ4Writer() throws IOException
    {
        compressor = LZ4Compressor.instance;
        runTests("LZ4");
    }

    @Test
    public void testDeflateWriter() throws IOException
    {
        compressor = DeflateCompressor.instance;
        runTests("Deflate");
    }

    @Test
    public void testSnappyWriter() throws IOException
    {
        compressor = SnappyCompressor.instance;
        runTests("Snappy");
    }

    private void testWrite(File f, int bytesToTest) throws IOException
    {
        final String filename = f.getAbsolutePath();
        final ChannelProxy channel = new ChannelProxy(f);

        try
        {
            MetadataCollector sstableMetadataCollector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance));
            byte[] dataPre = new byte[bytesToTest];
            byte[] rawPost = new byte[bytesToTest];
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, filename + ".metadata", new CompressionParameters(compressor), sstableMetadataCollector);)
            {
                Random r = new Random();

                // Test both write with byte[] and ByteBuffer
                r.nextBytes(dataPre);
                r.nextBytes(rawPost);
                ByteBuffer dataPost = makeBB(bytesToTest);
                dataPost.put(rawPost);
                dataPost.flip();

                writer.write(dataPre);
                FileMark mark = writer.mark();

                // Write enough garbage to transition chunk
                for (int i = 0; i < CompressionParameters.DEFAULT_CHUNK_LENGTH; i++)
                {
                    writer.write((byte)i);
                }
                writer.resetAndTruncate(mark);
                writer.write(dataPost);
                writer.finish();
            }

            assert f.exists();
            RandomAccessReader reader = CompressedRandomAccessReader.open(channel, new CompressionMetadata(filename + ".metadata", f.length()));
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
            channel.close();

            if (f.exists())
                f.delete();
            File metadata = new File(f + ".metadata");
            if (metadata.exists())
                metadata.delete();
        }
    }

    private ByteBuffer makeBB(int size)
    {
        return compressor.preferredBufferType().allocate(size);
    }

    private final List<TestableCSW> writers = new ArrayList<>();

    @After
    public void cleanup()
    {
        for (TestableCSW sw : writers)
            sw.cleanup();
        writers.clear();
    }

    protected TestableTransaction newTest() throws IOException
    {
        TestableCSW sw = new TestableCSW();
        writers.add(sw);
        return sw;
    }

    private static class TestableCSW extends TestableSW
    {
        final File offsetsFile;

        private TestableCSW() throws IOException
        {
            this(tempFile("compressedsequentialwriter"),
                 tempFile("compressedsequentialwriter.offsets"));
        }

        private TestableCSW(File file, File offsetsFile) throws IOException
        {
            this(file, offsetsFile, new CompressedSequentialWriter(file, offsetsFile.getPath(), new CompressionParameters(LZ4Compressor.instance, BUFFER_SIZE, new HashMap<String, String>()), new MetadataCollector(CellNames.fromAbstractType(UTF8Type.instance, false))));
        }

        private TestableCSW(File file, File offsetsFile, CompressedSequentialWriter sw) throws IOException
        {
            super(file, sw);
            this.offsetsFile = offsetsFile;
        }

        protected void assertInProgress() throws Exception
        {
            Assert.assertTrue(file.exists());
            Assert.assertFalse(offsetsFile.exists());
            byte[] compressed = readFileToByteArray(file);
            byte[] uncompressed = new byte[partialContents.length];
            LZ4Compressor.instance.uncompress(compressed, 0, compressed.length - 4, uncompressed, 0);
            Assert.assertTrue(Arrays.equals(partialContents, uncompressed));
        }

        protected void assertPrepared() throws Exception
        {
            Assert.assertTrue(file.exists());
            Assert.assertTrue(offsetsFile.exists());
            DataInputStream offsets = new DataInputStream(new ByteArrayInputStream(readFileToByteArray(offsetsFile)));
            Assert.assertTrue(offsets.readUTF().endsWith("LZ4Compressor"));
            Assert.assertEquals(0, offsets.readInt());
            Assert.assertEquals(BUFFER_SIZE, offsets.readInt());
            Assert.assertEquals(fullContents.length, offsets.readLong());
            Assert.assertEquals(2, offsets.readInt());
            Assert.assertEquals(0, offsets.readLong());
            int offset = (int) offsets.readLong();
            byte[] compressed = readFileToByteArray(file);
            byte[] uncompressed = new byte[fullContents.length];
            LZ4Compressor.instance.uncompress(compressed, 0, offset - 4, uncompressed, 0);
            LZ4Compressor.instance.uncompress(compressed, offset, compressed.length - (4 + offset), uncompressed, partialContents.length);
            Assert.assertTrue(Arrays.equals(fullContents, uncompressed));
        }

        protected void assertAborted() throws Exception
        {
            super.assertAborted();
            Assert.assertFalse(offsetsFile.exists());
        }

        void cleanup()
        {
            file.delete();
            offsetsFile.delete();
        }
    }

}
