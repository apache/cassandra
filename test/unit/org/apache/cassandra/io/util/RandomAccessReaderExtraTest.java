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

package org.apache.cassandra.io.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.quicktheories.generators.SourceDSL;

import static java.nio.file.StandardOpenOption.WRITE;
import static org.quicktheories.QuickTheory.qt;

@RunWith(Parameterized.class)
public class RandomAccessReaderExtraTest
{
    private static final int GIG = 1024 * 1024 * 1024;

    // 4096 is default buffer size, so use 20 regions so the size is 5,120; just past buffer size
    private static FileRef TWENTY_REGIONS;
    private static FileRef LARGE_TWO_RECORDS;
    private static long LARGE_TWO_RECORDS_GAP;

    private final boolean mmap;
    private final BufferType bufferType;
    private final boolean compressed;

    public RandomAccessReaderExtraTest(boolean mmap, BufferType bufferType, boolean compressed)
    {
        this.mmap = mmap;
        this.bufferType = bufferType;
        this.compressed = compressed;
    }

    @Test
    public void seekTest()
    {
        try (FileHandle fh = createFileHandle(TWENTY_REGIONS);
             RandomAccessReader reader = fh.createReader())
        {
            qt().forAll(SourceDSL.longs().between(0, TWENTY_REGIONS.length() - 1)).checkAssert(jLong -> {
                try
                {
                    long offset = jLong;
                    reader.seek(offset);
                    Assert.assertEquals(positionToByteValue(offset), reader.readByte());
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            });
        }
    }

    @Test
    public void skipBytes()
    {
        try (FileHandle fh = createFileHandle(TWENTY_REGIONS);
             RandomAccessReader reader = fh.createReader())
        {
            qt().forAll(SourceDSL.longs().between(0, TWENTY_REGIONS.length() - 1)).checkAssert(jLong -> {
                try
                {
                    long offset = jLong;
                    reader.seek(0);
                    Assert.assertEquals(offset, reader.skipBytes(Math.toIntExact(offset)));
                    Assert.assertEquals(positionToByteValue(offset), reader.readByte());
                }
                catch (IOException e)
                {
                    throw new AssertionError(e);
                }
            });
        }
    }

    /**
     * This test makes sure the skip method match that of InputStream.
     */
    @Test
    public void skipMatchJavaAPIs() throws IOException
    {
        Assume.assumeTrue("Skip compressed files", !compressed);

        // FileInputStream will always return the value of n even when its past the EOF; this is the documented behavior of that class
        // that is problematic for higher level since apis like skipBytesFully assume that return != n means EOF.
        try (FileHandle fh = createFileHandle(TWENTY_REGIONS);
             RandomAccessReader reader = fh.createReader();
             ByteArrayInputStream stream = new ByteArrayInputStream(Files.readAllBytes(new File(fh.channel.filePath()).toPath())))
        {
            // skip till EOF is reached
            long length = reader.length();
            while (true)
            {
                int bytesToSkip = 1024;

                long position = reader.getPosition();

                long fileSkipped = stream.skip(bytesToSkip);
                long readerSkipped = reader.skip(bytesToSkip);
                Assert.assertEquals("reader.skip does not match InputStream.skip at position " + position + " (" + length + ")", fileSkipped, readerSkipped);

                if (fileSkipped <= 0)
                    break;
            }
        }
    }

    /**
     * This test makes sure the skipByte method match that of DataInput.
     */
    @Test
    public void skipBytesMatchJavaAPIs() throws IOException
    {
        Assume.assumeTrue("Skip compressed files", !compressed);

        try (FileHandle fh = createFileHandle(TWENTY_REGIONS);
             RandomAccessReader reader = fh.createReader();
             RandomAccessFile dataInput = new RandomAccessFile(fh.channel.filePath(), "r"))
        {
            // skip till EOF is reached
            long length = reader.length();
            while (true)
            {
                int bytesToSkip = 1024;

                long position = reader.getPosition();

                long fileSkippedBytes = dataInput.skipBytes(bytesToSkip);
                long readerSkippedBytes = reader.skipBytes(bytesToSkip);
                Assert.assertEquals("reader.skipBytes does not match InputStream.skipBytes at position " + position + " (" + length + ")", fileSkippedBytes, readerSkippedBytes);

                if (fileSkippedBytes <= 0)
                    break;
            }
        }
    }

    @Test
    public void skipBytesLargeFile() throws IOException
    {
        // This test acts more like a bad performance test than it does a correctness test; since the above tests
        // cover this path.
        // This test should always be a few milliseconds (< 60ms), if larger than a second, something is wrong.
        // To more noticablly check this, set the file to be 10g+
        try (FileHandle fh = createFileHandle(LARGE_TWO_RECORDS);
             RandomAccessReader reader = fh.createReader())
        {
            Assert.assertEquals(42, reader.readLong());
            reader.skipFully(LARGE_TWO_RECORDS_GAP);
            Assert.assertEquals(78, reader.readLong());
        }
    }

    private FileHandle createFileHandle(FileRef f)
    {
        if (compressed)
            return new FileHandle.Builder(f.compressed.getAbsolutePath()).mmapped(mmap).bufferType(bufferType).compressed(true).complete();
        else
            return new FileHandle.Builder(f.uncompressed.getAbsolutePath()).mmapped(mmap).bufferType(bufferType).complete();
    }

    @BeforeClass
    public static void setupFiles() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        TWENTY_REGIONS = createByteOffsetFile(20);

        long bytesToSkip = 1L * GIG;
        LARGE_TWO_RECORDS = createLargeFileWithGap(bytesToSkip);
        LARGE_TWO_RECORDS_GAP = bytesToSkip;
    }

    private static FileRef createLargeFileWithGap(long bytesToSkip) throws IOException
    {
        File dir = Files.createTempDirectory("RandomAccessReaderExtraTest").toFile();
        String prefix = "na-42-big";
        File uncompressed = new File(dir, "uncompressed-Data.db");
        uncompressed.createNewFile();
        // is there not a cleaner way to compress a stream? Pretending to be a SSTable feels so wrong...
        File compressed = new File(dir, prefix + "-Data.db");
        try (FileChannel cn = FileChannel.open(uncompressed.toPath(), EnumSet.of(WRITE)))
        {
            ByteBuffer buffer = ByteBuffer.allocate(8);
            buffer.putLong(42);
            buffer.flip();
            Assert.assertEquals(8, cn.write(buffer));

            cn.position(bytesToSkip + Long.BYTES);

            buffer.clear();
            buffer.putLong(78);
            buffer.flip();
            Assert.assertEquals(8, cn.write(buffer));
        }
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
        compressed,
        new File(dir, prefix + "-CompressionInfo.db").getAbsolutePath(),
        new File(dir, prefix + "-Digest.crc32"),
        SequentialWriterOption.newBuilder().build(),
        CompressionParams.snappy(), // compression
        new MetadataCollector(new ClusteringComparator())))
        {
            writer.writeLong(42);

            // add zeros...
            byte[] zeros = new byte[(int) Math.min(1 << 12, bytesToSkip)];
            long remainingZeros = bytesToSkip;
            while (remainingZeros >= zeros.length)
            {
                writer.write(zeros, 0, zeros.length);
                remainingZeros -= zeros.length;
            }
            for (int i = 0; i < remainingZeros; i++)
                writer.writeByte(0);

            writer.writeLong(78);

            writer.finish();
        }
        return new FileRef(uncompressed, compressed);
    }

    @AfterClass
    public static void cleanupFiles()
    {
        TWENTY_REGIONS.delete();
        LARGE_TWO_RECORDS.delete();
    }

    @Parameters(name = "{0}/{1}/{2}")
    public static Collection<Object[]> testParams()
    {
        List<Object[]> tests = new ArrayList<>();
        for (Boolean mmap : Arrays.asList(Boolean.TRUE, Boolean.FALSE))
        {
            for (BufferType bufferType : BufferType.values())
            {
                for (Boolean compressed : Arrays.asList(Boolean.TRUE, Boolean.FALSE))
                {
                    tests.add(new Object[]{ mmap, bufferType, compressed });
                }
            }
        }
        return tests;
    }

    private static FileRef createByteOffsetFile(int numRegions) throws IOException
    {
        byte[] bytes = createBytes();
        File dir = Files.createTempDirectory("RandomAccessReaderExtraTest").toFile();
        String prefix = "na-42-big";
        File uncompressed = new File(dir, "uncompressed-Data.db");
        uncompressed.createNewFile();
        File compressed = new File(dir, prefix + "-Data.db");
        try (FileChannel channel = FileChannel.open(uncompressed.toPath(), EnumSet.of(StandardOpenOption.WRITE));
             CompressedSequentialWriter writer = new CompressedSequentialWriter(compressed,
                                                                                new File(dir, prefix + "-CompressionInfo.db").getAbsolutePath(),
                                                                                new File(dir, prefix + "-Digest.crc32"),
                                                                                SequentialWriterOption.newBuilder().build(),
                                                                                CompressionParams.snappy(), // compression
                                                                                new MetadataCollector(new ClusteringComparator())))
        {
            ByteBuffer buff = ByteBuffer.wrap(bytes);
            for (int i = 0; i < numRegions; i++)
            {
                channel.write(buff);
                buff.position(0);
                writer.write(bytes);
            }

            // do a partial region which will not be a power of 2
            buff.position(0);
            buff.limit(121);
            channel.write(buff);
            writer.write(bytes, 0, 121);

            writer.finish();
        }
        return new FileRef(uncompressed, compressed);
    }

    private static byte positionToByteValue(long position)
    {
        return (byte) (position % 256);
    }

    private static byte[] createBytes()
    {
        byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) i;
        return bytes;
    }

    private static final class FileRef
    {
        public final File uncompressed;
        public final File compressed;

        private FileRef(File uncompressed, File compressed)
        {
            this.uncompressed = uncompressed;
            this.compressed = compressed;
        }

        private long length()
        {
            return uncompressed.length();
        }

        private void delete()
        {
            FileUtils.deleteRecursive(uncompressed.getParentFile());
        }
    }
}
