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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import com.google.common.io.Files;
import static org.junit.Assert.*;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CompressorTest
{
    ICompressor compressor;

    ICompressor[] compressors = new ICompressor[] {
            LZ4Compressor.create(Collections.<String, String>emptyMap()),
            DeflateCompressor.create(Collections.<String, String>emptyMap()),
            SnappyCompressor.create(Collections.<String, String>emptyMap())
    };

    @Test
    public void testAllCompressors() throws IOException
    {
        for (ICompressor compressor : compressors)
        {
            this.compressor = compressor;

            testEmptyArray();
            testLongArray();
            testShortArray();
            testMappedFile();
        }
    }

    public void testArrayUncompress(byte[] data, int off, int len) throws IOException
    {
        final int inOffset = 2;
        ByteBuffer src = makeBB(len + inOffset);
        src.position(inOffset);
        src.put(data, off, len);
        src.flip().position(inOffset);

        final int outOffset = 3;
        final ByteBuffer compressed = makeBB(outOffset + compressor.initialCompressedBufferLength(len));
        fillBBWithRandom(compressed);
        compressed.position(outOffset);

        compressor.compress(src, compressed);
        compressed.flip().position(outOffset);

        final int restoreOffset = 5;
        final byte[] restored = new byte[restoreOffset + len];
        new Random().nextBytes(restored);

        // need byte[] representation which direct buffers don't have
        byte[] compressedBytes = new byte[compressed.capacity()];
        ByteBufferUtil.arrayCopy(compressed, outOffset, compressedBytes, outOffset, compressed.limit() - outOffset);

        final int decompressedLength = compressor.uncompress(compressedBytes, outOffset, compressed.remaining(), restored, restoreOffset);

        assertEquals(decompressedLength, len);
        assertArrayEquals(Arrays.copyOfRange(data, off, off + len),
                          Arrays.copyOfRange(restored, restoreOffset, restoreOffset + decompressedLength));
    }

    public void testArrayUncompress(byte[] data) throws IOException
    {
        testArrayUncompress(data, 0, data.length);
    }

    public void testEmptyArray() throws IOException
    {
        testArrayUncompress(new byte[0]);
    }

    public void testShortArray() throws UnsupportedEncodingException, IOException
    {
        testArrayUncompress("Cassandra".getBytes("UTF-8"), 1, 7);
    }

    public void testLongArray() throws UnsupportedEncodingException, IOException
    {
        byte[] data = new byte[1 << 20];
        testArrayUncompress(data, 13, 1 << 19);
        new Random(0).nextBytes(data);
        testArrayUncompress(data, 13, 1 << 19);
    }

    public void testMappedFile() throws IOException
    {
        byte[] data = new byte[1 << 20];
        new Random().nextBytes(data);
        ByteBuffer src = makeBB(data.length);
        src.put(data);
        src.flip();

        // create a temp file
        File temp = File.createTempFile("tempfile", ".tmp");
        temp.deleteOnExit();

        // Prepend some random bytes to the output and compress
        final int outOffset = 3;
        byte[] garbage = new byte[outOffset + compressor.initialCompressedBufferLength(data.length)];
        new Random().nextBytes(garbage);
        ByteBuffer dest = makeBB(outOffset + compressor.initialCompressedBufferLength(data.length));
        dest.put(garbage);
        dest.clear();
        dest.position(outOffset);

        compressor.compress(src, dest);
        int compressedLength = dest.position() - outOffset;

        FileChannel channel = FileChannel.open(temp.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
        dest.clear();
        channel.write(dest);

        MappedByteBuffer mappedData = Files.map(temp);
        ByteBuffer result = makeBB(data.length + 100);
        mappedData.position(outOffset).limit(outOffset + compressedLength);

        compressor.uncompress(mappedData, result);
        channel.close();
        result.flip();

        Assert.assertEquals(data.length, result.limit());
        for (int i = 0; i < result.limit(); i++)
        {
            Assert.assertEquals("Decompression mismatch at byte "+i, data[i], result.get());
        }
    }

    @Test
    public void testLZ4ByteBuffers() throws IOException
    {
        compressor = LZ4Compressor.create(Collections.<String, String>emptyMap());
        testByteBuffers();
    }

    @Test
    public void testDeflateByteBuffers() throws IOException
    {
        compressor = DeflateCompressor.create(Collections.<String, String>emptyMap());
        testByteBuffers();
    }

    @Test
    public void testSnappyByteBuffers() throws IOException
    {
        compressor = SnappyCompressor.create(Collections.<String, String>emptyMap());
        testByteBuffers();
    }

    private void testByteBuffers() throws IOException
    {
        assert compressor.supports(BufferType.OFF_HEAP);
        assert compressor.supports(compressor.preferredBufferType());

        for (BufferType in: BufferType.values())
            if (compressor.supports(in))
                for (BufferType comp: BufferType.values())
                    if (compressor.supports(comp))
                        for (BufferType out: BufferType.values())
                            if (compressor.supports(out))
                                testByteBuffers(in, comp, out);
    }

    private void testByteBuffers(BufferType typeIn, BufferType typeComp, BufferType typeOut) throws IOException
    {
        try
        {
            int n = RandomAccessReader.DEFAULT_BUFFER_SIZE;
            byte[] srcData = new byte[n];
            new Random().nextBytes(srcData);

            final int inOffset = 2;
            ByteBuffer src = typeIn.allocate(inOffset + n + inOffset);
            src.position(inOffset);
            src.put(srcData, 0, n);
            src.flip().position(inOffset);

            int outOffset = 5;
            ByteBuffer compressed = typeComp.allocate(outOffset + compressor.initialCompressedBufferLength(srcData.length) + outOffset);
            byte[] garbage = new byte[compressed.capacity()];
            new Random().nextBytes(garbage);
            compressed.put(garbage);
            compressed.position(outOffset).limit(compressed.capacity() - outOffset);

            compressor.compress(src, compressed);
            assertEquals(inOffset + n, src.position());
            assertEquals(inOffset + n, src.limit());
            assertEquals(compressed.capacity() - outOffset, compressed.limit());
            compressed.flip().position(outOffset);
            int len = compressed.remaining();

            ByteBuffer result = typeOut.allocate(inOffset + n + inOffset);
            result.position(inOffset).limit(result.capacity() - inOffset);
            compressor.uncompress(compressed, result);
            assertEquals(outOffset + len, compressed.position());
            assertEquals(outOffset + len, compressed.limit());
            assertEquals(result.capacity() - inOffset, result.limit());

            int decompressed = result.position() - inOffset;
            assert decompressed == n : "Failed uncompressed size";
            for (int i = 0; i < n; ++i)
                assert srcData[i] == result.get(inOffset + i) : "Failed comparison on index: " + i;
        }
        catch (Throwable e)
        {
            throw new AssertionError("Failed testing compressor " + compressor.getClass().getSimpleName() + " with buffer types in:" + typeIn + " compressed:" + typeComp + " out:" + typeOut, e);
        }
    }

    private ByteBuffer makeBB(int size)
    {
        return compressor.preferredBufferType().allocate(size);
    }

    private void fillBBWithRandom(ByteBuffer dest)
    {
        byte[] random = new byte[dest.capacity()];
        new Random().nextBytes(random);
        dest.clear();
        dest.put(random);
    }

}
