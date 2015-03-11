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
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.*;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;

import org.apache.cassandra.io.compress.ICompressor.WrappedByteBuffer;
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
        ByteBuffer src = makeBB(len);
        src.put(data, off, len);
        src.rewind();

        final int outOffset = 3;
        final WrappedByteBuffer compressed = makeWrappedBB(outOffset + compressor.initialCompressedBufferLength(len));
        fillBBWithRandom(compressed.buffer);
        compressed.buffer.clear();
        compressed.buffer.position(outOffset);

        final int compressedLength = compressor.compress(src, compressed);

        final int restoreOffset = 5;
        final byte[] restored = new byte[restoreOffset + len];
        new Random().nextBytes(restored);

        // need byte[] representation which direct buffers don't have
        byte[] compressedBytes = new byte[compressed.buffer.capacity()];
        ByteBufferUtil.arrayCopy(compressed.buffer, outOffset, compressedBytes, 0, compressed.buffer.capacity() - outOffset);

        final int decompressedLength = compressor.uncompress(compressedBytes, 0, compressedLength, restored, restoreOffset);

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
        WrappedByteBuffer dest = makeWrappedBB(outOffset + compressor.initialCompressedBufferLength(data.length));
        dest.buffer.put(garbage);
        dest.buffer.clear();
        dest.buffer.position(outOffset);

        final int compressedLength = compressor.compress(src, dest);

        FileChannel channel = new FileOutputStream(temp, false).getChannel();
        dest.buffer.clear();
        channel.write(dest.buffer);

        MappedByteBuffer mappedData = Files.map(temp);
        mappedData.position(outOffset);
        mappedData.limit(compressedLength + outOffset);

        ByteBuffer result = makeBB(data.length + 100);

        int length = compressor.uncompress(mappedData, result);

        Assert.assertEquals(data.length, length);
        for (int i = 0; i < length; i++)
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
        int n = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        byte[] srcData = new byte[n];
        new Random().nextBytes(srcData);

        ByteBuffer src = makeBB(n);
        src.put(srcData, 0, n);
        src.flip();

        int outOffset = 5;
        ICompressor.WrappedByteBuffer compressed = makeWrappedBB(outOffset + compressor.initialCompressedBufferLength(srcData.length));
        byte[] garbage = new byte[compressed.buffer.capacity()];
        new Random().nextBytes(garbage);
        compressed.buffer.put(garbage);
        compressed.buffer.clear();
        compressed.buffer.position(outOffset);

        compressor.compress(src, compressed);
        compressed.buffer.flip();
        compressed.buffer.position(outOffset);

        ByteBuffer result = makeBB(outOffset + n);
        int decompressed = compressor.uncompress(compressed.buffer, result);

        assert decompressed == n;
        for (int i = 0; i < n; ++i)
            assert srcData[i] == result.get(i) : "Failed comparison on index: " + i + " with compressor: " + compressor.getClass().toString();
    }

    private ByteBuffer makeBB(int size)
    {
        return compressor.useDirectOutputByteBuffers()
                ? ByteBuffer.allocateDirect(size)
                : ByteBuffer.allocate(size);
    }

    private WrappedByteBuffer makeWrappedBB(int size)
    {
        return compressor.useDirectOutputByteBuffers()
                ? new WrappedByteBuffer(ByteBuffer.allocateDirect(size))
                : new WrappedByteBuffer(ByteBuffer.allocate(size));
    }

    private void fillBBWithRandom(ByteBuffer dest)
    {
        ByteBuffer dupe = dest.duplicate();
        byte[] random = new byte[dest.capacity()];
        new Random().nextBytes(random);
        dest.clear();
        dest.put(random);
    }

}
