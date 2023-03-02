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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.io.util.SequentialWriterTest;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.apache.commons.io.FileUtils.readFileToByteArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompressedSequentialWriterTest extends SequentialWriterTest
{
    private CompressionParams compressionParameters;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private void runTests(String testName) throws IOException
    {
        // Test small < 1 chunk data set
        testWrite(FileUtils.createTempFile(testName + "_small", "1"), 25, false);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(FileUtils.createTempFile(testName + "_chunkAligned", "1"), DEFAULT_CHUNK_LENGTH, false);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(FileUtils.createTempFile(testName + "_large", "1"), DEFAULT_CHUNK_LENGTH * 3 + 100, false);

        // Test small < 1 chunk data set
        testWrite(FileUtils.createTempFile(testName + "_small", "2"), 25, true);

        // Test to confirm pipeline w/chunk-aligned data writes works
        testWrite(FileUtils.createTempFile(testName + "_chunkAligned", "2"), DEFAULT_CHUNK_LENGTH, true);

        // Test to confirm pipeline on non-chunk boundaries works
        testWrite(FileUtils.createTempFile(testName + "_large", "2"), DEFAULT_CHUNK_LENGTH * 3 + 100, true);
    }

    @Test
    public void testLZ4Writer() throws IOException
    {
        compressionParameters = CompressionParams.lz4();
        runTests("LZ4");
    }

    @Test
    public void testDeflateWriter() throws IOException
    {
        compressionParameters = CompressionParams.deflate();
        runTests("Deflate");
    }

    @Test
    public void testSnappyWriter() throws IOException
    {
        compressionParameters = CompressionParams.snappy();
        runTests("Snappy");
    }

    @Test
    public void testZSTDWriter() throws IOException
    {
        compressionParameters = CompressionParams.zstd();
        runTests("ZSTD");
    }

    @Test
    public void testNoopWriter() throws IOException
    {
        compressionParameters = CompressionParams.noop();
        runTests("Noop");
    }

    private void testWrite(File f, int bytesToTest, boolean useMemmap) throws IOException
    {
        final String filename = f.absolutePath();
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));

        byte[] dataPre = new byte[bytesToTest];
        byte[] rawPost = new byte[bytesToTest];
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, new File(filename + ".metadata"),
                null, SequentialWriterOption.DEFAULT,
                compressionParameters,
                sstableMetadataCollector))
        {
            Random r = new Random(42);

            // Test both write with byte[] and ByteBuffer
            r.nextBytes(dataPre);
            r.nextBytes(rawPost);
            ByteBuffer dataPost = makeBB(bytesToTest);
            dataPost.put(rawPost);
            dataPost.flip();

            writer.write(dataPre);
            DataPosition mark = writer.mark();

            // Write enough garbage to transition chunk
            for (int i = 0; i < DEFAULT_CHUNK_LENGTH; i++)
            {
                writer.write((byte)i);
            }
            if (bytesToTest <= DEFAULT_CHUNK_LENGTH)
                assertEquals(writer.getLastFlushOffset(), DEFAULT_CHUNK_LENGTH);
            else
                assertTrue(writer.getLastFlushOffset() % DEFAULT_CHUNK_LENGTH == 0);

            writer.resetAndTruncate(mark);
            writer.write(dataPost);
            writer.finish();
        }

        assert f.exists();
        try (CompressionMetadata compressionMetadata = CompressionMetadata.open(new File(filename + ".metadata"), f.length(), true);
             FileHandle fh = new FileHandle.Builder(f).withCompressionMetadata(compressionMetadata).complete();
             RandomAccessReader reader = fh.createReader())
        {
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
            if (f.exists())
                f.tryDelete();
            File metadata = new File(f + ".metadata");
            if (metadata.exists())
                metadata.tryDelete();
        }
    }

    @Test
    public void testShortUncompressedChunk() throws IOException
    {
        // Test uncompressed chunk below threshold (CASSANDRA-14892)
        compressionParameters = CompressionParams.lz4(DEFAULT_CHUNK_LENGTH, DEFAULT_CHUNK_LENGTH);
        testWrite(FileUtils.createTempFile("14892", "1"), compressionParameters.maxCompressedLength() - 1, false);
    }

    @Test
    public void testUncompressedChunks() throws IOException
    {
        for (double ratio = 1.25; ratio >= 1; ratio -= 1.0/16)
            testUncompressedChunks(ratio);
    }

    private void testUncompressedChunks(double ratio) throws IOException
    {
        for (int compressedSizeExtra : new int[] {-3, 0, 1, 3, 15, 1051})
            testUncompressedChunks(ratio, compressedSizeExtra);
    }

    private void testUncompressedChunks(double ratio, int compressedSizeExtra) throws IOException
    {
        for (int size = (int) (DEFAULT_CHUNK_LENGTH / ratio - 5); size <= DEFAULT_CHUNK_LENGTH / ratio + 5; ++size)
            testUncompressedChunks(size, ratio, compressedSizeExtra);
    }

    private void testUncompressedChunks(int size, double ratio, int extra) throws IOException
    {
        // System.out.format("size %d ratio %f extra %d\n", size, ratio, extra);
        ByteBuffer b = ByteBuffer.allocate(size);
        ByteBufferUtil.writeZeroes(b, size);
        b.flip();

        File f = FileUtils.createTempFile("testUncompressedChunks", "1");
        String filename = f.path();
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
        compressionParameters = new CompressionParams(MockCompressor.class.getTypeName(),
                                                      MockCompressor.paramsFor(ratio, extra),
                                                      DEFAULT_CHUNK_LENGTH, ratio);
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, new File(f.path() + ".metadata"),
                                                                                null, SequentialWriterOption.DEFAULT,
                                                                                compressionParameters,
                                                                                sstableMetadataCollector))
        {
            writer.write(b);
            writer.finish();
            b.flip();
        }

        assert f.exists();
        try (CompressionMetadata compressionMetadata = CompressionMetadata.open(new File(filename + ".metadata"), f.length(), true);
             FileHandle fh = new FileHandle.Builder(f).withCompressionMetadata(compressionMetadata).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(size, reader.length());
            byte[] result = new byte[(int)reader.length()];

            reader.readFully(result);
            assert(reader.isEOF());

            assert Arrays.equals(b.array(), result);
        }
        finally
        {
            if (f.exists())
                f.tryDelete();
            File metadata = new File(f + ".metadata");
            if (metadata.exists())
                metadata.tryDelete();
        }

    }


    private ByteBuffer makeBB(int size)
    {
        return compressionParameters.getSstableCompressor().preferredBufferType().allocate(size);
    }

    private final List<TestableCSW> writers = new ArrayList<>();

    @After
    public void cleanup()
    {
        for (TestableCSW sw : writers)
            sw.cleanup();
        writers.clear();
    }

    @Test
    @Override
    public void resetAndTruncateTest()
    {
        File tempFile = new File(Files.createTempDir().toPath(), "reset.txt");
        File offsetsFile = FileUtils.createDeletableTempFile("compressedsequentialwriter.offset", "test");
        final int bufferSize = 48;
        final int writeSize = 64;
        byte[] toWrite = new byte[writeSize];
        try (SequentialWriter writer = new CompressedSequentialWriter(tempFile, offsetsFile,
                                                                      null, SequentialWriterOption.DEFAULT,
                                                                      CompressionParams.lz4(bufferSize),
                                                                      new MetadataCollector(new ClusteringComparator(UTF8Type.instance))))
        {
            // write bytes greather than buffer
            writer.write(toWrite);
            long flushedOffset = writer.getLastFlushOffset();
            assertEquals(writeSize, writer.position());
            // mark thi position
            DataPosition pos = writer.mark();
            // write another
            writer.write(toWrite);
            // another buffer should be flushed
            assertEquals(flushedOffset * 2, writer.getLastFlushOffset());
            assertEquals(writeSize * 2, writer.position());
            // reset writer
            writer.resetAndTruncate(pos);
            // current position and flushed size should be changed
            assertEquals(writeSize, writer.position());
            assertEquals(flushedOffset, writer.getLastFlushOffset());
            // write another byte less than buffer
            writer.write(new byte[]{0});
            assertEquals(writeSize + 1, writer.position());
            // flush off set should not be increase
            assertEquals(flushedOffset, writer.getLastFlushOffset());
            writer.finish();
        }
        catch (IOException e)
        {
            Assert.fail();
        }
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
        static final int MAX_COMPRESSED = BUFFER_SIZE * 10;     // Always compress for this test.

        private TestableCSW() throws IOException
        {
            this(tempFile("compressedsequentialwriter"),
                 tempFile("compressedsequentialwriter.offsets"));
        }

        private TestableCSW(File file, File offsetsFile) throws IOException
        {
            this(file, offsetsFile, new CompressedSequentialWriter(file, offsetsFile,
                                                                   null, SequentialWriterOption.DEFAULT,
                                                                   CompressionParams.lz4(BUFFER_SIZE, MAX_COMPRESSED),
                                                                   new MetadataCollector(new ClusteringComparator(UTF8Type.instance))));

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
            byte[] compressed = readFileToByteArray(file.toJavaIOFile());
            byte[] uncompressed = new byte[partialContents.length];
            LZ4Compressor.create(Collections.<String, String>emptyMap()).uncompress(compressed, 0, compressed.length - 4, uncompressed, 0);
            Assert.assertTrue(Arrays.equals(partialContents, uncompressed));
        }

        protected void assertPrepared() throws Exception
        {
            Assert.assertTrue(file.exists());
            Assert.assertTrue(offsetsFile.exists());
            DataInputStream offsets = new DataInputStream(new ByteArrayInputStream(readFileToByteArray(offsetsFile.toJavaIOFile())));
            Assert.assertTrue(offsets.readUTF().endsWith("LZ4Compressor"));
            Assert.assertEquals(0, offsets.readInt());
            Assert.assertEquals(BUFFER_SIZE, offsets.readInt());
            Assert.assertEquals(MAX_COMPRESSED, offsets.readInt());
            Assert.assertEquals(fullContents.length, offsets.readLong());
            Assert.assertEquals(2, offsets.readInt());
            Assert.assertEquals(0, offsets.readLong());
            int offset = (int) offsets.readLong();
            byte[] compressed = readFileToByteArray(file.toJavaIOFile());
            byte[] uncompressed = new byte[fullContents.length];
            LZ4Compressor.create(Collections.<String, String>emptyMap()).uncompress(compressed, 0, offset - 4, uncompressed, 0);
            LZ4Compressor.create(Collections.<String, String>emptyMap()).uncompress(compressed, offset, compressed.length - (4 + offset), uncompressed, partialContents.length);
            Assert.assertTrue(Arrays.equals(fullContents, uncompressed));
        }

        protected void assertAborted() throws Exception
        {
            super.assertAborted();
        }

        void cleanup()
        {
            file.tryDelete();
            offsetsFile.tryDelete();
        }
    }

}