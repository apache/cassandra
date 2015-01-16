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
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import com.google.common.io.Files;
import org.apache.cassandra.io.compress.ICompressor.WrappedArray;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

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


    public void test(byte[] data, int off, int len) throws IOException
    {
        final int outOffset = 3;
        final WrappedArray out = new WrappedArray(new byte[outOffset + compressor.initialCompressedBufferLength(len)]);
        new Random().nextBytes(out.buffer);
        final int compressedLength = compressor.compress(data, off, len, out, outOffset);
        final int restoredOffset = 5;
        final byte[] restored = new byte[restoredOffset + len];
        new Random().nextBytes(restored);
        final int decompressedLength = compressor.uncompress(out.buffer, outOffset, compressedLength, restored, restoredOffset);
        assertEquals(decompressedLength, len);
        assertArrayEquals(Arrays.copyOfRange(data, off, off + len),
                          Arrays.copyOfRange(restored, restoredOffset, restoredOffset + decompressedLength));
    }

    public void test(byte[] data) throws IOException
    {
        test(data, 0, data.length);
    }

    public void testEmptyArray() throws IOException
    {
        test(new byte[0]);
    }

    public void testShortArray() throws UnsupportedEncodingException, IOException
    {
        test("Cassandra".getBytes("UTF-8"), 1, 7);
    }

    public void testLongArray() throws UnsupportedEncodingException, IOException
    {
        byte[] data = new byte[1 << 20];
        test(data, 13, 1 << 19);
        new Random(0).nextBytes(data);
        test(data, 13, 1 << 19);
    }

    public void testMappedFile() throws IOException
    {
        byte[] data = new byte[1 << 20];
        new Random().nextBytes(data);

        //create a temp file
        File temp = File.createTempFile("tempfile", ".tmp");
        temp.deleteOnExit();

        //Prepend some random bytes to the output and compress
        final int outOffset = 3;
        final WrappedArray out = new WrappedArray(new byte[outOffset + compressor.initialCompressedBufferLength(data.length)]);
        new Random().nextBytes(out.buffer);
        final int compressedLength = compressor.compress(data, 0, data.length, out, outOffset);
        Files.write(out.buffer, temp);

        MappedByteBuffer mappedData = Files.map(temp);
        mappedData.position(outOffset);
        mappedData.limit(compressedLength+outOffset);


        ByteBuffer result = compressor.useDirectOutputByteBuffers()
                ? ByteBuffer.allocateDirect(data.length + 100)
                : ByteBuffer.allocate(data.length + 100);

        int length = compressor.uncompress(mappedData, result);

        Assert.assertEquals(data.length, length);
        for (int i = 0; i < length; i++)
        {
            Assert.assertEquals("Decompression mismatch at byte "+i, data[i], result.get());
        }
    }
}
